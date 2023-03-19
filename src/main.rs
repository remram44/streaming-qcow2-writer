mod qcow2;

use std::ops::Range;
use std::path::Path;

use qcow2::StreamingQcow2Writer;

const USAGE: &'static str = "Usage: streaming-qcow2-writer input.img [layout.json] > output.qcow2";

fn main() {
    // Read command-line arguments
    let mut args = std::env::args_os();
    if let None = args.next() {
        eprintln!("Not enough arguments");
        eprintln!("{}", USAGE);
        std::process::exit(2);
    }
    let Some(input) = args.next() else {
        eprintln!("Not enough arguments");
        eprintln!("{}", USAGE);
        std::process::exit(2);
    };
    let layout = args.next();
    if let Some(_) = args.next() {
        eprintln!("Too many arguments");
        std::process::exit(2);
    }

    // Open input
    let (input, input_size) = match std::fs::File::open(input)
        .and_then(|f| f.metadata().map(|m| (f, m.len())))
    {
        Ok(o) => o,
        Err(e) => {
            eprintln!("Error opening input file: {}", e);
            std::process::exit(1);
        }
    };
    eprintln!("Input is {} bytes", input_size);

    // Read layout
    let layout = match layout {
        Some(arg) => match load_layout_file(Path::new(&arg)) {
            Ok(l) => l,
            Err(e) => {
                eprintln!("Error reading layout file: {}", e);
                std::process::exit(1);
            }
        }
        None => vec![0..input_size],
    };

    // Initialize writer
    let qcow2_writer = StreamingQcow2Writer::new(input_size, layout.iter().cloned());

    // Write
    let output = std::io::stdout().lock();
    let mut output = std::io::BufWriter::new(output);
    if let Err(e) = qcow2_writer.write_header(&mut output)
        .and_then(|()| qcow2_writer.copy_data(input, &mut output))
    {
        eprintln!("Error writing data: {}", e);
        std::process::exit(1);
    }
}

fn load_layout_file(path: &Path) -> std::io::Result<Vec<Range<u64>>> {
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct LayoutEntry {
        offset: u64,
        length: u64,
    }

    let file = std::fs::File::open(path)?;
    let file = std::io::BufReader::new(file);
    let entries: Vec<LayoutEntry> = serde_json::from_reader(file)?;
    let entries = entries.iter().map(|e| e.offset..(e.offset + e.length)).collect();
    Ok(entries)
}
