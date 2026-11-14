use afl::fuzz;

fn main() {
    fuzz!(|data: &[u8]| {
        let _ = fuzz_target_parser::check_codec(data);
    });
}
