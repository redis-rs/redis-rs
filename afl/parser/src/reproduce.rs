//! Replays a crash found by a fuzz target. `--codec` replays one found by
//! `fuzz-codec`; otherwise the input is from `fuzz-target`.

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (codec, path) = match args.as_slice() {
        [_, path] => (false, path),
        [_, flag, path] if flag == "--codec" => (true, path),
        _ => {
            println!("Usage: {} [--codec] <path-to-crash>", args[0]);
            std::process::exit(1);
        }
    };

    let data = std::fs::read(path).unwrap_or_else(|_| panic!("Could not open file {path}"));
    if codec {
        let (values, failed) = fuzz_target_parser::check_codec(&data);
        println!("Values: {values:?}, failed: {failed}");
    } else {
        let v = fuzz_target_parser::check_parse(&data);
        println!("Result: {v:?}");
    }
}
