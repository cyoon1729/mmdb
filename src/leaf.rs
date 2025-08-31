use std::fmt;

use crate::buf::{as_u16_slice, ByteBuf};
use crate::constants::*;
use crate::page::Page;

pub struct LeafPage<'a> {
    pgno: Pgno,
    flags: PageFlag,
    lower: u16,
    upper: u16,
    offsets: &'a [u16],
    data: &'a [u8],
}

pub struct LeafNode<'a> {
    flags: NodeFlag,
    key_size: usize,
    data_size: usize,
    key: &'a [u8],
    data: &'a [u8],
}

impl fmt::Debug for LeafPage<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LeafPage")
            .field("pgno", &self.pgno)
            .field("flags", &self.flags)
            .field("lower", &self.lower)
            .field("upper", &self.upper)
            .field("offsets", &self.offsets)
            .field("data", &self.data)
            .finish()
    }
}

impl fmt::Debug for LeafNode<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LeafNode")
            .field("flags", &self.flags)
            .field("key_size", &self.key_size)
            .field("key", &String::from_utf8(self.key.to_vec()).unwrap())
            .field("data_size", &self.data_size)
            .field("data", &String::from_utf8(self.data.to_vec()).unwrap())
            .finish()
    }
}

impl<'a> LeafNode<'a> {
    fn from(key: &'a [u8], data: &'a [u8]) -> Self {
        LeafNode {
            flags: NodeFlag::ALIVE,
            key_size: key.len(),
            data_size: data.len(),
            key,
            data,
        }
    }

    pub fn pack(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(
            2 +                    // flags (u16)
            std::mem::size_of::<usize>() * 2 + // key_size + data_size
            self.key.len() +
            self.data.len(),
        );

        buf.extend_from_slice(&self.flags.bits().to_le_bytes());
        buf.extend_from_slice(&(self.key_size as usize).to_le_bytes());
        buf.extend_from_slice(&(self.data_size as usize).to_le_bytes());
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.data);
        buf
    }

    fn get_size(&self) -> usize {
        self.key_size + self.data_size + 2 * USIZE_N + 2
    }

    fn get_key(&self) -> &[u8] {
        self.key
    }

    fn get_data(&self) -> &[u8] {
        self.data
    }
}

impl<'a> LeafPage<'a> {
    pub fn from(page: &'a Page) -> Result<Self, DBError> {
        let leaf_page = LeafPage {
            pgno: page.get_pgno(),
            flags: page.get_flag(),
            lower: page.get_lower(),
            upper: page.get_upper(),
            offsets: Self::get_node_offset(page.get_data(), page.get_lower()),
            data: page.get_data(),
        };

        Ok(leaf_page)
    }

    fn get_node_offset(data: &[u8], lower: u16) -> &[u16] {
        let offsets_end = lower as usize;
        as_u16_slice(&data[..offsets_end])
    }

    pub fn read_node_from_offset(&self, offset: usize) -> LeafNode {
        let flags = NodeFlag::from_bits(self.data.read_u16_le(offset).unwrap())
            .expect("Unrecognized node flags");
        let key_size = self.data.read_usize_le(offset + U16_N).unwrap();
        let data_size = self.data.read_usize_le(offset + U16_N + USIZE_N).unwrap();
        let key_start = offset + U16_N + USIZE_N * 2;
        let key = self.data.read_n_bytes(key_start, key_size).unwrap();
        let data = self
            .data
            .read_n_bytes(key_start + key_size, data_size)
            .unwrap();

        LeafNode {
            flags,
            key_size,
            data_size,
            key,
            data,
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<LeafNode, DBError> {
        let offset_idx_or_error = match self.offsets.binary_search_by(|offset| {
            let node = self.read_node_from_offset(*offset as usize);
            node.get_key().cmp(key)
        }) {
            Ok(idx) => Ok(idx),
            Err(_idx) => Err(DBError::KeyNotFound),
        };

        offset_idx_or_error.map(|idx| {
            let offset = self.offsets[idx] as usize;
            self.read_node_from_offset(offset)
        })
    }

    pub fn can_insert(&self, new_node: LeafNode) -> bool {
        let remaining_space = (self.upper - self.lower) as usize;
        remaining_space > new_node.get_size()
    }

    pub fn insert(&self, key: &[u8], data: &[u8]) -> Result<Page, DBError> {
        let mut nodes: Vec<LeafNode> = self
            .offsets
            .iter()
            .map(|&offset| self.read_node_from_offset(offset as usize))
            .collect();
        match nodes.binary_search_by(|n| n.get_key().cmp(key)) {
            Ok(idx) => {
                // upsert
                nodes[idx] = LeafNode::from(key, data);
            }
            Err(idx) => {
                // insert
                nodes.insert(idx, LeafNode::from(key, data));
            }
        }

        Ok(Self::write_new_page(self.pgno, &nodes))
    }

    fn write_new_page(pgno: Pgno, nodes: &[LeafNode]) -> Page {
        let mut page_data_buf = [0u8; PAGE_BUF_SIZE];
        let mut lower = 0;
        let mut upper = PAGE_BUF_SIZE;
        for node in nodes.iter() {
            let node_bytes = node.pack();
            page_data_buf[upper - node_bytes.len()..upper].copy_from_slice(&node_bytes);
            upper -= node_bytes.len();

            let offset = (upper).to_le_bytes();
            page_data_buf[lower..lower + U16_N].copy_from_slice(&offset);
            lower += 2;
        }

        Page::from(
            pgno,
            0x0,
            PageFlag::ALIVE,
            lower as u16,
            upper as u16,
            page_data_buf,
        )
    }
}
