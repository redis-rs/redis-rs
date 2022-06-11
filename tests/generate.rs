use std::{collections::HashMap, ffi::OsStr, fs};

use walkdir::WalkDir;

#[test]
fn generated_code_is_fresh() {
    let tmp_dir = tempfile::tempdir().unwrap();
    fs::create_dir_all(&tmp_dir).unwrap();
    redis_codegen::generate_commands(&"docs/commands.json", Some(&tmp_dir)).unwrap();

    // let mut root = String::new();
    // root.push_str("pub mod commands;");
    // fs::write(tmp_dir.path().join("mod.rs"), root).unwrap();

    let versions = [SOURCE_DIR, tmp_dir.path().to_str().unwrap()]
        .iter()
        .map(|path| {
            let mut files = HashMap::new();
            for entry in WalkDir::new(path) {
                let entry = match entry {
                    Ok(e) => e,
                    Err(_) => continue,
                };

                let is_file = entry.file_type().is_file();
                let rs = entry.path().extension() == Some(OsStr::new("rs"));
                if !is_file || !rs {
                    continue;
                }

                let file = entry.path();
                let name = file.strip_prefix(path).unwrap();
                files.insert(name.to_owned(), fs::read_to_string(file).unwrap());
            }

            files
        })
        .collect::<Vec<_>>();

    // Compare the old version and new version and fail the test if they're different.

    if versions[0] != versions[1] {
        let _ = fs::remove_dir_all(SOURCE_DIR);
        fs::rename(tmp_dir, SOURCE_DIR).unwrap();
        panic!("generated code in the repository is outdated, updating...");
    }
}

const BASE_URI: &str = "https://raw.githubusercontent.com/redis/redis-doc/master/commands.json";
const SOURCE_DIR: &str = "src/generated";
