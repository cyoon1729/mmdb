// type aliases
pub type Pgno = u64;
pub type TxnId = u64;

// sizes
pub const PAGE_HEADER_SIZE: usize = 16;
pub const PAGE_SIZE: usize = 4096;
pub const USIZE_N: usize = std::mem::size_of::<usize>();
pub const KEY_SIZE: usize = USIZE_N;
pub const DATA_SIZE: usize = USIZE_N;

pub const MAX_PGNO: usize = usize::MAX;
pub const MAGIC_NUMBER: u16 = 0xBEEF;

