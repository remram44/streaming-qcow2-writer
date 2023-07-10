mod qcow2;
mod sparsify;
mod utils;

use std::ops::Range;
use std::path::Path;

use qcow2::StreamingQcow2Writer;
use sparsify::sparsify_layout;

const USAGE: &'static str = "Usage: streaming-qcow2-writer [--sparsify] input.img [layout.json] > output.qcow2";

#[cfg(unix)]
const BLKGETSIZE64_CODE: u8 = 0x12; // Defined in linux/fs.h
#[cfg(unix)]
const BLKGETSIZE64_SEQ: u8 = 114;
#[cfg(unix)]
nix::ioctl_read!(ioctl_blkgetsize64, BLKGETSIZE64_CODE, BLKGETSIZE64_SEQ, u64);

fn get_file_size(file: &std::fs::File) -> std::io::Result<u64> {
    let metadata = file.metadata()?;

    let file_type = metadata.file_type();

    #[cfg(unix)]
    {
        use std::os::unix::fs::FileTypeExt;
        use std::os::unix::io::AsRawFd;

        if file_type.is_block_device() {
            let fd = file.as_raw_fd();
            let mut cap = 0u64;
            let cap_ptr = &mut cap as *mut u64;
            unsafe {
                ioctl_blkgetsize64(fd, cap_ptr).unwrap();
            }

            return Ok(cap);
        }
    }

    if !metadata.file_type().is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "input is not a file",
        ));
    }

    Ok(metadata.len())
}

fn main() {
    // Read command-line arguments
    let mut args = std::env::args_os().peekable();
    let mut sparsify = false;
    if let None = args.next() {
        eprintln!("Not enough arguments");
        eprintln!("{}", USAGE);
        std::process::exit(2);
    }
    if let Some(arg) = args.peek() {
        if arg == "--sparsify" {
            sparsify = true;
            args.next().unwrap();
        }
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
    let (mut input, input_size) = match std::fs::File::open(input)
        .and_then(|f| get_file_size(&f).map(|s| (f, s)))
    {
        Ok(o) => o,
        Err(e) => {
            eprintln!("Error opening input file: {}", e);
            std::process::exit(1);
        }
    };
    eprintln!("Input is {} bytes", input_size);

    // Read layout
    let mut layout = match layout {
        Some(arg) => match load_layout_file(Path::new(&arg)) {
            Ok(l) => l,
            Err(e) => {
                eprintln!("Error reading layout file: {}", e);
                std::process::exit(1);
            }
        }
        None => vec![0..input_size],
    };

    // Optional first pass: find holes
    if sparsify {
        layout = match sparsify_layout(&mut input, &layout) {
            Ok(l) => l,
            Err(e) => {
                eprintln!("Error reading file: {}", e);
                std::process::exit(1);
            }
        };
    }

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
