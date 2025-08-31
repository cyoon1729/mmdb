use memmap2::Mmap;
use std::io::{self, ErrorKind, Result};

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

impl Page {
    pub fn from(
        pgno: Pgno,
        pad: u16,
        flags: PageFlag,
        lower: u16,
        upper: u16,
        data: [u8; 4080],
    ) -> Self {
        Page {
            pgno,
            pad,
            flags,
            lower,
            upper,
            data,
        }
    }
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
        &self.data
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
