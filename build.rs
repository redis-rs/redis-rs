use std::env;
use std::ffi::OsString;
use std::process::Command;


fn get_rustc_version() -> (i32, i32) {
    let rustc = env::var_os("RUSTC").unwrap_or_else(|| OsString::from("rustc"));
    let out = String::from_utf8(Command::new(&rustc)
        .arg("--version")
        .output()
        .unwrap().stdout).unwrap();
    let mut pieces = out.split(' ');
    let _ = pieces.next();

    let mut ver_iter = pieces.next().unwrap().split('.').map(|x| x.parse().unwrap());
    (ver_iter.next().unwrap(), ver_iter.next().unwrap())
}

fn rustc_has_unix_socket() -> bool {
    if !cfg!(unix) {
        false
    } else {
        let (major, minor) = get_rustc_version();
        major > 1 || (major == 1 && minor >= 10)
    }
}

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    if rustc_has_unix_socket() {
        println!("cargo:rustc-cfg=feature=\"with-system-unix-sockets\"");
    }
}
