use memmap2::Mmap;
use std::fmt;
use std::io::{self, ErrorKind, Result};

use crate::buf::ByteBuf;
use crate::constants::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Key {
    Normal(Vec<u8>),
    Sentinel,
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (Key::Sentinel, Key::Sentinel) => std::cmp::Ordering::Equal,
            (Key::Sentinel, _) => std::cmp::Ordering::Greater,
            (_, Key::Sentinel) => std::cmp::Ordering::Less,
            (Key::Normal(a), Key::Normal(b)) => a.cmp(b),
        }
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[repr(C)]
pub struct Page {
    pgno: Pgno,
    pad: u16,
    flags: PageFlag,
    lower: u16,
    upper: u16,
    data: [u8; PAGE_SIZE - PAGE_HEADER_SIZE],
}

pub struct MetaPage {
    pgno: Pgno,
    pad: u16,
    flags: u16,
    lower: u16,
    upper: u16,
    data: [u8; PAGE_SIZE - PAGE_HEADER_SIZE],
}

pub struct MetaPageData {
    data_root:  Pgno,
    free_root:  Pgno,
    curr_txn: TxnId,
    max_pgno: Pgno,
}

pub struct BranchPage {
    pgno: Pgno,
    pad: u16,
    flags: u16,
    lower: u16,
    upper: u16,
    nodes: Vec<BranchNode>,
}

pub struct BranchNode {
    flags: u16,
    pgno: Pgno,
    key_size: usize,
    max_key: Vec<u8>,
}

pub struct LeafPage<'a> {
    pgno: Pgno,
    pad: u16,
    flags: u16,
    lower: u16,
    upper: u16,
    nodes: Vec<LeafNode<'a>>,
}

enum LeafNode<'a> {
    Read(RLeafNode<'a>),
    Write(WLeafNode)
}

pub struct RLeafNode<'a> {
    flags: u16,
    key_size: usize,
    data_size: usize,
    key: &'a [u8],
    data: &'a [u8],
}

pub struct WLeafNode {
    flags: u16,
    key_size: usize,
    data_size: usize,
    key: Vec<u8>,
    value: Vec<u8>,
}

pub struct PgnoGenerator {
    pgno: Pgno,
}

impl PgnoGenerator {
    fn create(pgno: Pgno) -> Self {
        PgnoGenerator { pgno }
    }

    fn get_free(&mut self) -> Pgno {
        self.pgno += 1;
        self.pgno
    }
}


impl fmt::Debug for LeafNode<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LeafNode::Read(leaf_node) => leaf_node.fmt(f),
            LeafNode::Write(_) => todo!(),
        }
    }
}

impl fmt::Debug for RLeafNode<'_> {
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

impl fmt::Debug for LeafPage<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LeafPage")
            .field("pgno", &self.pgno)
            .field("pad", &self.pad)
            .field("flags", &self.flags)
            .field("lower", &self.lower)
            .field("upper", &self.upper)
            .field("nodes", &self.nodes)
            .finish()
    }
}

impl fmt::Debug for BranchNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BranchNode")
            .field("key_size", &self.key_size)
            .field("max_key", &String::from_utf8(self.max_key.clone()).unwrap())
            .field("pgno", &self.pgno)
            .finish()
    }
}

impl fmt::Debug for BranchPage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BranchPage")
            .field("pgno", &self.pgno)
            .field("pad", &self.pad)
            .field("flags", &self.flags)
            .field("lower", &self.lower)
            .field("upper", &self.upper)
            .field("nodes", &self.nodes)
            .finish()
    }
}

impl Page {
    pub fn get_pgno(&self) -> Pgno {
        self.pgno
    }

    pub fn get_pad(&self) -> u16 {
        self.pad
    }

    pub fn get_flag(&self) -> PageFlag {
        self.flags
    }

    pub fn get_lower(&self) -> u16 {
        self.lower
    }
    
    pub fn get_upper(&self) -> u16 {
        self.upper
    }

    pub fn get_data(&self) -> &[u8] {
        self.data;
    }

    pub fn read_from_mmap(mmap: &Mmap, pgno: usize) -> Result<Self> {
        let start = pgno * PAGE_SIZE;
        let end = start + PAGE_SIZE;
        let page_bytes = mmap
            .get(start..end)
            .ok_or_else(|| io::Error::new(ErrorKind::UnexpectedEof, "Page out of bounds"))?;
        let page = { unsafe { std::ptr::read_unaligned(page_bytes.as_ptr().cast::<Page>()) } };

        Ok(page)
    }
}

impl BranchPage {
    pub fn from(page: &Page) -> Result<Self> {
        let branch_page = BranchPage {
            pgno: page.pgno,
            pad: page.pad,
            flags: page.flags,
            lower: page.lower,
            upper: page.upper,
            nodes: Self::get_nodes(page),
        };

        Ok(branch_page)
    }

    fn get_nodes(page: &Page) -> Vec<BranchNode> {
        let offsets = Self::get_node_offsets(page);
        offsets
            .iter()
            .map(|offset| Self::get_node(&page.data, *offset))
            .collect::<Vec<BranchNode>>()
    }

    fn get_node_offsets(page: &Page) -> Vec<usize> {
        let offsets_end = (page.lower as usize) - PAGE_HEADER_SIZE;
        page.data[..offsets_end]
            .chunks_exact(2)
            .map(|chunk| (u16::from_le_bytes([chunk[0], chunk[1]]) as usize))
            .map(|offset| offset - PAGE_HEADER_SIZE)
            .collect::<Vec<_>>()
    }

    fn get_node(page_data: &[u8], offset: usize) -> BranchNode {
        let flags = page_data.read_u16_le(offset).unwrap(); 
        let pgno = page_data.read_u64_le(offset + 2).unwrap();
        let key_size = page_data.read_usize_le(offset + 10).unwrap();

        let key_start = offset + 12;
        BranchNode {
            flags,
            pgno,
            key_size,
            max_key: page_data.read_n_bytes(key_start + 12, key_size).unwrap().to_vec(),
        }
    }
}

impl<'a> LeafNode<'a> {
    fn get_key(&self) -> &[u8] {
        match self {
            LeafNode::Read(node) => node.get_key(),
            LeafNode::Write(_) => panic!(),
        }
    }

    fn get_data(&self) -> &[u8] {
        match self {
            LeafNode::Read(node) => node.get_data(),
            LeafNode::Write(_) => panic!(),
        }
    }

    fn get_size(&self) -> usize {
        match self {
            LeafNode::Read(node) => node.get_size(),
            LeafNode::Write(_) => todo!(),
        }
    }
}

impl<'a> RLeafNode<'a> {
    fn from(key: &'a [u8], data: &'a [u8]) -> Self {
        RLeafNode {
            flags: 0xBEEF,
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
    pub fn from(page: &'a Page) -> Result<Self> {
        let leaf_page = LeafPage {
            pgno: page.pgno,
            pad: page.pad,
            flags: page.flags,
            lower: page.lower,
            upper: page.upper,
            nodes: Self::get_nodes(page),
        };

        Ok(leaf_page)
    }

    fn get(&self, key: &[u8]) -> Option<&[u8]> {
        match self.nodes.binary_search_by(|n| n.get_key().cmp(key)) {
            Ok(idx) => Some(self.nodes.get(idx).unwrap().get_data()),
            Err(_) => None 
        }
    }

    fn can_insert(&self, new_node: LeafNode) -> bool {
        let remaining_space = (self.upper - self.lower) as usize;
        remaining_space > new_node.get_size()
    }

    fn insert(&self, key: &[u8], data: &[u8]) -> Result<()>{
        let _nodes_after_insert = match self.nodes.binary_search_by(|n| n.get_key().cmp(key)) {
            Ok(idx) => todo!() 
            Err(idx) =>  todo!()
        };

        Ok(())
    }

    fn split(&self, pgno_generator: &mut PgnoGenerator) -> Result<(LeafPage, LeafPage)>{
        let (left_nodes, right_nodes) = self.nodes.split_at(self.nodes.len() / 2);
        let left_page = Self::to_writeable_leaf_page(left_nodes, pgno_generator.get_free());
        let right_page = Self::to_writeable_leaf_page(right_nodes, pgno_generator.get_free());

        Ok((left_page, right_page))
    }

    fn to_writeable_leaf_page(nodes: &[LeafNode], pgno: Pgno) -> Self {
        let writeable_nodes: Vec<LeafNode> = nodes.iter()
            .map(|node| match node {
                LeafNode::Read(rn) => LeafNode::Write(WLeafNode { 
                    flags: rn.flags,
                    key_size: rn.key_size,
                    data_size: rn.data_size,
                    key: rn.key.to_vec(),
                    value: rn.data.to_vec(),
                }),
                LeafNode::Write(_) => unreachable!(),
            })
        .collect::<Vec<LeafNode>>();
        let total_size: usize = nodes.iter().map(|n| n.get_size()).sum();

        LeafPage {
            pgno,
            pad: 0xBEEF,
            flags: 0xBEEF,
            lower: (U16_N * writeable_nodes.len()) as u16,
            upper: (PAGE_SIZE - total_size) as u16,
            nodes: writeable_nodes,
        }
    }

    fn get_nodes(page: &Page) -> Vec<LeafNode> {
        let offsets = Self::get_node_offsets(page);
        offsets
            .iter()
            .map(|offset| Self::get_node(&page.data, *offset))
            .collect::<Vec<LeafNode>>()
    }

    fn get_node_offsets(page: &Page) -> Vec<usize> {
        let offsets_end = (page.lower as usize) - PAGE_HEADER_SIZE;
        page.data[..offsets_end]
            .chunks_exact(2)
            .map(|chunk| (u16::from_le_bytes([chunk[0], chunk[1]]) as usize))
            .map(|offset| offset - PAGE_HEADER_SIZE)
            .collect::<Vec<_>>()
    }

    fn get_node(page_data: &[u8], offset: usize) -> LeafNode {
        let flags = page_data.read_u16_le(offset).unwrap();
        let key_size = page_data.read_usize_le(offset + 2).unwrap();
        let data_size = page_data.read_usize_le(offset + 4).unwrap();
            
        let key_start = offset + 8;
        let data_start = key_start + key_size;
        LeafNode::Read(RLeafNode {
            flags,
            key_size,
            data_size,
            key: &page_data[key_start..data_start],
            data: &page_data[data_start..data_start + data_size],
        })
    }
}
