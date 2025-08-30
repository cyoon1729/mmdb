use std::fmt;

use crate::buf::ByteBuf;
use crate::constants::*;
use crate::page::Page;

pub struct LeafPage<'a> {
    pgno: Pgno,
    flags: PageFlag,
    lower: u16,
    upper: u16,
    nodes: Vec<LeafNode<'a>>,
    offsets: &'a [u16], 
    data: &'a [u8]
}

pub struct LeafNode<'a> {
    flags: NodeFlag,
    key_size: usize,
    data_size: usize,
    key: &'a [u8],
    data: &'a [u8],
}

pub struct WLeafNode {
    flags: NodeFlag,
    key_size: usize,
    data_size: usize,
    key: Vec<u8>,
    data: Vec<u8>,
}

impl WLeafNode {
    pub fn pack(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(
            2 +                    // flags (u16)
            std::mem::size_of::<usize>() * 2 + // key_size + data_size
            self.key.len() +
            self.data.len()
        );

        buf.extend_from_slice(&self.flags.bits().to_le_bytes());
        buf.extend_from_slice(&(self.key_size as usize).to_le_bytes());
        buf.extend_from_slice(&(self.data_size as usize).to_le_bytes());
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.data);
        buf
    }
}


impl fmt::Debug for LeafPage<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LeafPage")
            .field("pgno", &self.pgno)
            .field("flags", &self.flags)
            .field("lower", &self.lower)
            .field("upper", &self.upper)
            .field("nodes", &self.nodes)
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
            nodes: Self::get_nodes(page),
        };

        Ok(leaf_page)
    }

    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        match self.nodes.binary_search_by(|n| n.get_key().cmp(key)) {
            Ok(idx) => Some(self.nodes.get(idx).unwrap().get_data()),
            Err(_) => None 
        }
    }

    pub fn can_insert(&self, new_node: LeafNode) -> bool {
        let remaining_space = (self.upper - self.lower) as usize;
        remaining_space > new_node.get_size()
    }

    pub fn insert(&self, key: &[u8], data: &[u8]) -> Result<(), DBError>{
        let _nodes_after_insert = match self.nodes.binary_search_by(|n| n.get_key().cmp(key)) {
            Ok(idx) => todo!(),
            Err(idx) =>  todo!()
        };

        Ok(())
    }

    pub fn split(&self, pgno_generator: &mut PgnoGenerator) -> Result<(Page, Page), DBError>{
        let (left_nodes, right_nodes) = self.nodes.split_at(self.nodes.len() / 2);
        let left_page = Self::write_new_page(left_nodes, pgno_generator.get_free());
        let right_page = Self::write_new_page(right_nodes, pgno_generator.get_free());

        Ok((left_page, right_page))
    }

    fn write_new_page(nodes: &[LeafNode], pgno: Pgno) -> Page {
        let writeable_nodes: Vec<WLeafNode> = nodes.iter()
            .map(|node| WLeafNode { 
                    flags: node.flags,
                    key_size: node.key_size,
                    data_size: node.data_size,
                    key: node.key.to_vec(),
                    data: node.data.to_vec(),
                })
            .rev()
            .collect();

        let mut page_data_buf = vec![0u8; PAGE_SIZE - PAGE_HEADER_SIZE];
        let mut upper = PAGE_SIZE;
        let mut lower = 0;
        for node in writeable_nodes.iter() {
            let node_bytes = node.pack();
            page_data_buf[upper - node_bytes.len() .. upper].copy_from_slice(&node_bytes);
            upper = upper - node_bytes.len();

            let offset = (upper as u16).to_le_bytes(); 
            page_data_buf[lower .. lower + U16_N].copy_from_slice(&offset);
            lower += 2;
        }

        return Page {
            pgno: pgno,
            pad: 0x0,
            flags: PageFlag::ALIVE,
            lower: top,
            upper: bottom,
            data: page_data_buf,
        }
    }

    fn get_nodes(page: &Page) -> Vec<LeafNode> {
        let offsets = Self::get_node_offsets(page);
        offsets
            .iter()
            .map(|offset| Self::get_node(&page.get_data(), *offset))
            .collect::<Vec<LeafNode>>()
    }

    fn get_node_offsets(page: &Page) -> Vec<usize> {
        let data = page.get_data();
        let offsets_end = (page.get_lower() as usize) - PAGE_HEADER_SIZE;
        data[..offsets_end]
            .chunks_exact(2)
            .map(|chunk| (u16::from_le_bytes([chunk[0], chunk[1]]) as usize))
            .map(|offset| offset - PAGE_HEADER_SIZE)
            .collect::<Vec<_>>()
    }

    fn get_node(page_data: &[u8], offset: usize) -> LeafNode {
        let flags = NodeFlag::from_bits(page_data.read_u16_le(offset).unwrap()).unwrap();
        let key_size = page_data.read_usize_le(offset + 2).unwrap();
        let data_size = page_data.read_usize_le(offset + 4).unwrap();
            
        let key_start = offset + 8;
        let data_start = key_start + key_size;
        LeafNode {
            flags,
            key_size,
            data_size,
            key: &page_data[key_start..data_start],
            data: &page_data[data_start..data_start + data_size],
        }
    }
}
