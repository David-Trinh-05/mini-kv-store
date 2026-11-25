use std::{
    error::Error,
    fs::File,
    io::{self, Read, Write},
    path::PathBuf,
    process::ExitCode,
};

use clap::{Parser, Subcommand};
use mini_kv_store::{Error as StoreError, Mutation, Store};

const INPUT_LIMIT: u64 = 2 * 1024 * 1024;

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[arg(long, value_name = "DIR")]
    db: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Print one value and a final newline.
    Get { key: String },
    /// Replace a value.
    Set { key: String, value: String },
    /// Delete a key if present.
    Delete { key: String },
    /// Commit a JSON array of mutations as one batch. Use "-" for stdin.
    Apply { file: PathBuf },
    /// Print all keys and values as a JSON object.
    List,
    /// Print WAL and recovery metrics.
    Stats,
    /// Rewrite the WAL from live keys and print the size change.
    Compact,
}

fn run(cli: Cli) -> Result<ExitCode, Box<dyn Error>> {
    // Read external input before locking, including when stdin is a pipe.
    let mutations = match &cli.command {
        Command::Apply { file } if file.as_os_str() == "-" => read_batch(io::stdin().lock())?,
        Command::Apply { file } => read_batch(File::open(file)?)?,
        _ => Vec::new(),
    };
    let mut store = Store::open(cli.db)?;
    let stats = store.stats()?;
    if stats.recovered_bytes > 0 {
        eprintln!(
            "recovered {} incomplete trailing bytes at WAL offset {}",
            stats.recovered_bytes, stats.wal_bytes,
        );
    }
    let mut output = io::stdout().lock();
    match cli.command {
        Command::Get { key } => match store.get(&key)? {
            Some(value) => writeln!(output, "{value}")?,
            None => return Ok(ExitCode::from(1)),
        },
        Command::Set { key, value } => store.apply(vec![Mutation::Set { key, value }])?,
        Command::Delete { key } => store.apply(vec![Mutation::Delete { key }])?,
        Command::Apply { .. } => store.apply(mutations)?,
        Command::List => print_json(&mut output, store.entries()?)?,
        Command::Stats => print_json(&mut output, &stats)?,
        Command::Compact => print_json(&mut output, &store.compact()?)?,
    }
    output.flush()?;
    Ok(ExitCode::SUCCESS)
}

fn read_batch(input: impl Read) -> Result<Vec<Mutation>, Box<dyn Error>> {
    let mut bytes = Vec::new();
    input.take(INPUT_LIMIT + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > INPUT_LIMIT {
        return Err(StoreError::InvalidInput("JSON input exceeds 2 MiB").into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}

fn print_json(output: &mut impl Write, value: &impl serde::Serialize) -> io::Result<()> {
    serde_json::to_writer(&mut *output, value)?;
    writeln!(output)
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(status) => status,
        Err(error) => {
            eprintln!("mini-kv-store: {error}");
            if matches!(error.downcast_ref(), Some(StoreError::Busy)) {
                ExitCode::from(3)
            } else {
                ExitCode::from(4)
            }
        }
    }
}
