use std::fmt;

use crate::buf::{as_u16_slice, ByteBuf};
use crate::constants::*;
use crate::page::Page;

pub struct DataPage<'a> {
    pgno: Pgno,
    flags: PageFlag,
    lower: u16,
    upper: u16,
    offsets: &'a [u16],
    data: &'a [u8],
}

pub struct DataNode<'a> {
    flags: NodeFlag,
    key_size: usize,
    data_size: usize,
    key: &'a [u8],
    data: &'a [u8],
}

impl fmt::Debug for DataPage<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataPage")
            .field("pgno", &self.pgno)
            .field("flags", &self.flags)
            .field("lower", &self.lower)
            .field("upper", &self.upper)
            .field("offsets", &self.offsets)
            .field("data", &self.data)
            .finish()
    }
}

impl fmt::Debug for DataNode<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataNode")
            .field("flags", &self.flags)
            .field("key_size", &self.key_size)
            .field("key", &String::from_utf8(self.key.to_vec()).unwrap())
            .field("data_size", &self.data_size)
            .field("data", &String::from_utf8(self.data.to_vec()).unwrap())
            .finish()
    }
}

impl<'a> DataNode<'a> {
    fn from(key: &'a [u8], data: &'a [u8]) -> Self {
        DataNode {
            flags: NodeFlag::ALIVE,
            key_size: key.len(),
            data_size: data.len(),
            key,
            data,
        }
    }

    pub fn pack(&self) -> Vec<u8> {
        // flags (u16) + key_size (usize) + data_size (usize) + ...
        let mut buf = Vec::with_capacity(U16_N + USIZE_N * 2 + self.key.len() + self.data.len());

        buf.extend_from_slice(&self.flags.bits().to_le_bytes());
        buf.extend_from_slice(&self.key_size.to_le_bytes());
        buf.extend_from_slice(&self.data_size.to_le_bytes());
        buf.extend_from_slice(self.key);
        buf.extend_from_slice(self.data);
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

impl<'a> DataPage<'a> {
    pub fn from(page: &'a Page) -> Result<Self, DBError> {
        let leaf_page = DataPage {
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

    pub fn read_node_from_offset(&self, offset: usize) -> DataNode<'_> {
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

        DataNode {
            flags,
            key_size,
            data_size,
            key,
            data,
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<DataNode<'_>, DBError> {
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

    pub fn can_insert(&self, new_node: DataNode) -> bool {
        let remaining_space = (self.upper - self.lower) as usize;
        remaining_space > new_node.get_size()
    }

    pub fn insert(&self, key: &[u8], data: &[u8]) -> Result<Page, DBError> {
        let mut nodes: Vec<DataNode> = self
            .offsets
            .iter()
            .map(|&offset| self.read_node_from_offset(offset as usize))
            .collect();
        match nodes.binary_search_by(|n| n.get_key().cmp(key)) {
            Ok(idx) => {
                // upsert
                nodes[idx] = DataNode::from(key, data);
            }
            Err(idx) => {
                // insert
                nodes.insert(idx, DataNode::from(key, data));
            }
        }

        Ok(Self::write_new_page(self.pgno, &nodes))
    }

    fn write_new_page(pgno: Pgno, nodes: &[DataNode]) -> Page {
        let mut page_data_buf = [0u8; PAGE_BUF_SIZE];
        let mut lower = 0;
        let mut upper = PAGE_BUF_SIZE;
        for node in nodes.iter() {
            let node_bytes = node.pack();
            page_data_buf[upper - node_bytes.len()..upper].copy_from_slice(&node_bytes);
            upper -= node_bytes.len();

            let offset = (upper as u16).to_le_bytes();
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

#[cfg(test)]
mod tests {
    use super::*;
    use rand::distr::Alphanumeric;
    use rand::Rng;
    use std::collections::HashMap;

    #[test]
    fn test_inserts() {
        let mut page = DataPage::write_new_page(0, &[]);
        let mut leaf_page = DataPage::from(&page).unwrap();

        let test_key_values = generate_key_values(100);
        for (key, value) in &test_key_values {
            page = leaf_page.insert(key.as_bytes(), value.as_bytes()).unwrap();
            leaf_page = DataPage::from(&page).unwrap();
        }

        for (key, value) in &test_key_values {
            let found_value = leaf_page.get(key.as_bytes()).unwrap();
            assert_eq!(found_value.get_data(), value.as_bytes());
        }
    }

    #[test]
    fn test_nodes_are_ordered() {
        let mut page = DataPage::write_new_page(0, &[]);
        let mut leaf_page = DataPage::from(&page).unwrap();

        let test_key_values = generate_key_values(100);
        for (key, value) in &test_key_values {
            page = leaf_page.insert(key.as_bytes(), value.as_bytes()).unwrap();
            leaf_page = DataPage::from(&page).unwrap();
        }

        let nodes: Vec<DataNode> = leaf_page
            .offsets
            .iter()
            .map(|offset| leaf_page.read_node_from_offset(*offset as usize))
            .collect();

        assert!(is_sorted_by_key(&nodes));
    }

    fn generate_key_values(n: i32) -> HashMap<String, String> {
        let mut rng = rand::rng();
        (0..n)
            .map(|_| {
                let key: String = (0..5).map(|_| rng.sample(Alphanumeric) as char).collect();
                let value: String = (0..5).map(|_| rng.sample(Alphanumeric) as char).collect();
                (key, value)
            })
            .collect()
    }

    fn is_sorted_by_key(nodes: &[DataNode]) -> bool {
        nodes.windows(2).all(|w| w[0].key <= w[1].key)
    }
}
