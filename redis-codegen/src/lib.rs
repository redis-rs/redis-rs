use code_generator::CodeGenerator;
use commands::CommandSet;
use std::{
    fs::{self, File},
    io::{self, BufReader},
    path::{Path, PathBuf},
};

mod code_generator;
mod commands;
mod feature_gates;
mod ident;

pub fn generate_commands(
    spec: impl AsRef<Path>,
    out_dir: Option<impl AsRef<Path>>,
) -> io::Result<()> {
    let out_dir = if let Some(out_dir) = out_dir.as_ref() {
        out_dir.as_ref().to_owned()
    } else {
        PathBuf::from(std::env::var("OUT_DIR").unwrap())
    };

    compile(spec, out_dir)?;
    Ok(())
}

fn compile(spec: impl AsRef<Path>, out_dir: PathBuf) -> io::Result<()> {
    let f = File::open(spec)?;
    let reader = BufReader::new(f);

    let deserializer = &mut serde_json::Deserializer::from_reader(reader);

    let command_set: CommandSet = serde_path_to_error::deserialize(deserializer)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;

    let mut buf = String::new();

    CodeGenerator::generate(command_set, &mut buf);
    let file_name = "commands.rs";
    let output_path = out_dir.join(file_name);

    let previous_content = fs::read(&output_path);

    if previous_content
        .map(|previous_content| previous_content == buf.as_bytes())
        .unwrap_or(false)
    {
        log::trace!("unchanged: {:?}", file_name);
    } else {
        log::trace!("writing: {:?}", file_name);
        fs::write(output_path, buf)?;
    }

    Ok(())
}
