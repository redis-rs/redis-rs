use std::env;
use std::ffi::OsString;
use std::process::Command;
use std::error::Error;


fn get_rustc_version() -> Result<(i32, i32), Box<Error>> {
    fn err() -> Box<Error> {
        "Can't determine the rustc version.".into()
    }

    let rustc = env::var_os("RUSTC").unwrap_or_else(|| OsString::from("rustc"));
    let out = String::from_utf8(Command::new(&rustc)
        .arg("--version")
        .output()
        .unwrap().stdout).unwrap();
    let mut pieces = out.split(' ');
    let _ = pieces.next().ok_or(err())?;

    let mut ver_iter = pieces
        .next()
        .ok_or(err())?
        .split('.')
        .map(|x| x.parse());
    let major = ver_iter.next().ok_or(err())??;
    let minor = ver_iter.next().ok_or(err())??;
    Ok((major, minor))
}

fn rustc_has_unix_socket() -> bool {
    if !cfg!(unix) {
        false
    } else {
        if let Ok((major, minor)) = get_rustc_version() {
            major > 1 || (major == 1 && minor >= 10)
        } else {
            false
        }
    }
}

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    if rustc_has_unix_socket() {
        println!("cargo:rustc-cfg=feature=\"with-system-unix-sockets\"");
    }
}
