use memmap2::Mmap;
use std::io::{self, ErrorKind, Result};

use crate::constants::*;

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
    pub const fn get_pgno(&self) -> Pgno {
        self.pgno
    }

    pub const fn get_pad(&self) -> u16 {
        self.pad
    }

    pub const fn get_flag(&self) -> PageFlag {
        self.flags
    }

    pub const fn get_lower(&self) -> u16 {
        self.lower
    }

    pub const fn get_upper(&self) -> u16 {
        self.upper
    }

    pub const fn get_data(&self) -> &[u8] {
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
