use crate::constants::USIZE_N;

use std::convert::TryInto;

pub trait ByteBuf {
    fn read_n_bytes(&self, offset: usize, n: usize) -> Option<&[u8]>;

    fn read_u16_le(&self, offset: usize) -> Option<u16> {
        let data = self.read_n_bytes(offset, 2)?;
        Some(u16::from_le_bytes(data.try_into().ok()?))
    }

    fn read_u32_le(&self, offset: usize) -> Option<u32> {
        let data = self.read_n_bytes(offset, 4)?;
        Some(u32::from_le_bytes(data.try_into().ok()?))
    }
    
    fn read_u64_le(&self, offset: usize) -> Option<u64> {
        let data = self.read_n_bytes(offset, 8)?;
        Some(u64::from_le_bytes(data.try_into().ok()?))
    }

    fn read_usize_le(&self, offset: usize) -> Option<usize> {
        let bytes = self.read_n_bytes(offset, USIZE_N)?;
        Some(match USIZE_N {
            4 => {
                let arr: [u8; 4] = bytes.try_into().ok()?;
                u32::from_le_bytes(arr) as usize
            }
            8 => {
                let arr: [u8; 8] = bytes.try_into().ok()?;
                u64::from_le_bytes(arr) as usize
            }
            _ => unreachable!("unsupported usize size"),
        })
    }
}

impl ByteBuf for [u8] {
    #[inline]
    fn read_n_bytes(&self, offset: usize, n: usize) -> Option<&[u8]> {
        self.get(offset..offset + n)
    }
}
