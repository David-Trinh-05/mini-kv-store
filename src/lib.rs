//! A local key-value store for a trusted directory on a Linux filesystem.

mod wal;

use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions, TryLockError},
    io::{self, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

const LOG: &str = "wal";
const STAGING: &str = ".wal.next";
const LOCK: &str = ".lock";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("database is locked by another handle")]
    Busy,
    #[error("corrupt WAL at byte {offset}: {reason}")]
    Corrupt { offset: u64, reason: &'static str },
    #[error("{0}")]
    InvalidInput(&'static str),
    #[error("write outcome is uncertain after an I/O error; reopen the database")]
    NeedsReopen,
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Changes in a batch are applied in order.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Mutation {
    Set { key: String, value: String },
    Delete { key: String },
}

#[derive(Debug, Serialize)]
pub struct Stats {
    pub keys: usize,
    pub frames: u64,
    pub wal_bytes: u64,
    pub recovered_bytes: u64,
}

#[derive(Debug, Serialize)]
pub struct Compaction {
    pub before_bytes: u64,
    pub after_bytes: u64,
}

/// Holds the exclusive database lock until dropped, including while compacting.
pub struct Store {
    path: PathBuf,
    directory: File,
    wal: File,
    index: BTreeMap<String, String>,
    frames: u64,
    recovered_bytes: u64,
    // An I/O failure can leave either an incomplete tail or a renamed WAL
    // without a confirmed directory sync. Replay before serving further calls.
    needs_reopen: bool,
    _lock: File,
}

impl Store {
    /// Open and replay the WAL, discarding only an incomplete final frame.
    /// Creates the store directory if its parent already exists.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref();
        match fs::create_dir(path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        let path = path.canonicalize()?;
        let directory = File::open(&path)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.join(LOCK))?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => return Err(Error::Busy),
            Err(TryLockError::Error(error)) => return Err(error.into()),
        }
        // Sync even an existing directory entry. A previous process might
        // have died immediately after creating the store directory.
        if let Some(parent) = path.parent() {
            File::open(parent)?.sync_all()?;
        }

        let log = match OpenOptions::new()
            .read(true)
            .append(true)
            .open(path.join(LOG))
        {
            Ok(file) => Some(file),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        let replay = log
            .as_ref()
            .map(wal::replay)
            .transpose()?
            .unwrap_or_default();
        match fs::remove_file(path.join(STAGING)) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let wal = if let Some(file) = log {
            if replay.recovered_bytes > 0 {
                file.set_len(replay.valid_bytes)?;
                file.sync_all()?;
            }
            // A compactor might have been killed just after rename.
            directory.sync_all()?;
            file
        } else {
            let mut staged = create_staging(&path)?;
            staged.write_all(wal::MAGIC)?;
            install(&path, &directory, &staged)?;
            staged
        };

        Ok(Self {
            path,
            directory,
            wal,
            index: replay.index,
            frames: replay.frames,
            recovered_bytes: replay.recovered_bytes,
            needs_reopen: false,
            _lock: lock,
        })
    }

    pub fn get(&self, key: &str) -> Result<Option<&str>, Error> {
        self.ensure_usable()?;
        wal::validate_key(key).map_err(Error::InvalidInput)?;
        Ok(self.index.get(key).map(String::as_str))
    }

    pub fn entries(&self) -> Result<&BTreeMap<String, String>, Error> {
        self.ensure_usable()?;
        Ok(&self.index)
    }

    pub fn stats(&self) -> Result<Stats, Error> {
        self.ensure_usable()?;
        Ok(Stats {
            keys: self.index.len(),
            frames: self.frames,
            wal_bytes: self.wal.metadata()?.len(),
            recovered_bytes: self.recovered_bytes,
        })
    }

    /// Appends one checksummed frame and syncs it before updating the index.
    pub fn apply(&mut self, mutations: Vec<Mutation>) -> Result<(), Error> {
        self.ensure_usable()?;
        let payload = wal::encode(&mutations)?;
        self.needs_reopen = true;
        wal::write_frame(&mut self.wal, &payload)?;
        self.wal.sync_all()?;
        wal::apply_to_index(&mut self.index, mutations);
        self.frames += 1;
        self.needs_reopen = false;
        Ok(())
    }

    /// Rewrite live values and replace the WAL atomically if it shrinks.
    pub fn compact(&mut self) -> Result<Compaction, Error> {
        self.ensure_usable()?;
        let before_bytes = self.wal.metadata()?.len();
        self.needs_reopen = true;
        let mut staged = create_staging(&self.path)?;
        let frames = wal::write_snapshot(&mut staged, &self.index)?;
        let snapshot_bytes = staged.metadata()?.len();
        let after_bytes = if snapshot_bytes < before_bytes {
            install(&self.path, &self.directory, &staged)?;
            self.wal = staged;
            self.frames = frames;
            snapshot_bytes
        } else {
            fs::remove_file(self.path.join(STAGING))?;
            before_bytes
        };
        self.needs_reopen = false;
        Ok(Compaction {
            before_bytes,
            after_bytes,
        })
    }

    fn ensure_usable(&self) -> Result<(), Error> {
        if self.needs_reopen {
            return Err(Error::NeedsReopen);
        }
        Ok(())
    }
}

fn create_staging(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .append(true)
        .create_new(true)
        .open(path.join(STAGING))
}

fn install(path: &Path, directory: &File, staged: &File) -> io::Result<()> {
    staged.sync_all()?;
    fs::rename(path.join(STAGING), path.join(LOG))?;
    directory.sync_all()
}
