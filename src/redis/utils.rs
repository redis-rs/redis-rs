use libc::c_uint;
use libc;
use std::ptr;

#[link(name = "crypto")]
extern {
    fn EVP_MD_CTX_create() -> EVP_MD_CTX;
    fn EVP_MD_CTX_destroy(ctx: EVP_MD_CTX);

    fn EVP_sha1() -> EVP_MD;
    
    fn EVP_DigestInit(ctx: EVP_MD_CTX, typ: EVP_MD);
    fn EVP_DigestUpdate(ctx: EVP_MD_CTX, data: *u8, n: c_uint);
    fn EVP_DigestFinal(ctx: EVP_MD_CTX, res: *mut u8, n: *u32);
}

#[allow(non_camel_case_types)]
pub type EVP_MD_CTX = *libc::c_void;

#[allow(non_camel_case_types)]
pub type EVP_MD = *libc::c_void;

pub struct Sha1 {
    priv ctx: EVP_MD_CTX,
}

impl Sha1 {

    pub fn new() -> Sha1 {
        unsafe {
            let ctx = EVP_MD_CTX_create();
            EVP_DigestInit(ctx, EVP_sha1());
            Sha1 { ctx: ctx }
        }
    }

    pub fn update(&mut self, data: &[u8]) {
        unsafe {
            EVP_DigestUpdate(self.ctx, data.as_ptr(), data.len() as c_uint)
        }
    }

    pub fn digest(&mut self) -> ~[u8] {
        unsafe {
            let mut res = ~[0u8, ..20];
            EVP_DigestFinal(self.ctx, res.as_mut_ptr(), ptr::null());
            res
        }
    }
}

impl Drop for Sha1 {

    fn drop(&mut self) {
        unsafe {
            EVP_MD_CTX_destroy(self.ctx);
        }
    }
}

pub fn sha1(data: &[u8]) -> ~[u8] {
    let mut hasher = Sha1::new();
    hasher.update(data);
    hasher.digest()
}
