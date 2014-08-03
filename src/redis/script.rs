use utils::sha1;
use serialize::hex::ToHex;

pub struct Script {
    pub code: ~[u8],
    pub sha: ~str,
}

impl Script {

    pub fn new(code: &str) -> Script {
        let encoded_code = code.as_bytes().to_owned();
        let hash = sha1(encoded_code);
        Script { code: encoded_code, sha: hash.to_hex() }
    }
}
