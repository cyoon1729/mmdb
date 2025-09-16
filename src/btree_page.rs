use crate::constants::*;
use crate::data_page::DataPage;
use crate::page::Page;

pub struct BranchPage<'a> {
    inner: DataPage<'a>
}

pub struct LeafPage<'a> {
    inner: DataPage<'a>
}

impl<'a> BranchPage<'a> {
    pub fn split(&self, pgno_left: Pgno, pgno_right: Pgno) -> Result<(Page, Page), DBError> {
        todo!();
    }

    pub fn get(&self, key: &[u8]) -> Result<Pgno, DBError> {
        todo!();
    }

    pub fn put(&self, key: &[u8], pgno: Pgno) -> Result<Page, DBError> {
        todo!();
    }
}

impl<'a> LeafPage<'a> {
    pub fn split(&self, pgno_left: Pgno, pgno_right: Pgno) -> Result<(Page, Page), DBError> {
        todo!();
    }

    pub fn get(&self, key: &[u8]) -> Result<Pgno, DBError> {
        todo!();
    }

    pub fn put(&self, new_pgno: Pgno, key: &[u8], data: &[u8]) -> Result<Page, DBError> {
        self.inner.put(new_pgno, key, data)
    }
}
