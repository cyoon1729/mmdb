use bitflags::bitflags;
use std::error::Error;
use std::fmt;

// type aliases
pub type Pgno = u64;
pub type TxnId = u64;

// sizes
pub const PAGE_HEADER_SIZE: usize = 16;
pub const PAGE_SIZE: usize = 4096;
pub const PAGE_BUF_SIZE: usize = PAGE_SIZE - PAGE_HEADER_SIZE;

pub const USIZE_N: usize = std::mem::size_of::<usize>();
pub const U16_N: usize = 2;
pub const KEY_SIZE: usize = USIZE_N;
pub const DATA_SIZE: usize = USIZE_N;

pub const MAX_PGNO: usize = usize::MAX;
pub const MAGIC_NUMBER: u16 = 0xBEEF;

// flags
bitflags! {
    #[repr(transparent)]
    #[derive(Copy, Clone, Debug, Eq, PartialEq)]
    pub struct PageFlag: u16 {
        const ALIVE = 1;
        const DIRTY = 2;
    }

    #[repr(transparent)]
    #[derive(Copy, Clone, Debug, Eq, PartialEq)]
    pub struct NodeFlag: u16 {
        const ALIVE = 1;
        const DIRTY = 2;
    }
}

// Errors
pub enum DBError {
    WriteLeafPageFailed,
    KeyNotFound,
}

impl Error for DBError {}

impl fmt::Debug for DBError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DBError::WriteLeafPageFailed => write!(f, "WriteLeafPageFailed"),
            DBError::KeyNotFound => write!(f, "KeyNotFound"),
        }
    }
}

impl fmt::Display for DBError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DBError::WriteLeafPageFailed => write!(f, "WriteLeafPageFailed"),
            DBError::KeyNotFound => write!(f, "KeyNotFound"),
        }
    }
}
