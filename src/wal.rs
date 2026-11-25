use std::{
    collections::BTreeMap,
    fs::File,
    io::{self, BufReader, Read, Write},
};

use crate::{Error, Mutation};

pub(crate) const MAGIC: &[u8; 8] = b"MINIKV\x01\n";
pub(crate) const PAYLOAD_LIMIT: usize = 1024 * 1024;
const BATCH_LIMIT: usize = 1024;
const FRAME_HEADER_BYTES: usize = 8;
const CHECKSUM_BYTES: usize = 4;

#[derive(Default)]
pub(crate) struct Replay {
    pub index: BTreeMap<String, String>,
    pub frames: u64,
    pub valid_bytes: u64,
    pub recovered_bytes: u64,
}

pub(crate) fn replay(file: &File) -> Result<Replay, Error> {
    let file_bytes = file.metadata()?.len();
    let mut reader = BufReader::new(file);
    let mut magic = [0; MAGIC.len()];
    let mut payload = Vec::new();
    if file_bytes < MAGIC.len() as u64 {
        return Err(Error::Corrupt {
            offset: 0,
            reason: "incomplete file header",
        });
    }
    reader.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(Error::Corrupt {
            offset: 0,
            reason: "unrecognized file header",
        });
    }
    let mut replay = Replay {
        valid_bytes: MAGIC.len() as u64,
        ..Replay::default()
    };
    while replay.valid_bytes < file_bytes {
        let offset = replay.valid_bytes;
        let remaining = file_bytes - offset;
        if remaining < FRAME_HEADER_BYTES as u64 {
            break;
        }
        let len = read_u32(&mut reader)?;
        let length_crc = read_u32(&mut reader)?;
        if crc32fast::hash(&len.to_le_bytes()) != length_crc {
            return Err(Error::Corrupt {
                offset,
                reason: "length CRC mismatch",
            });
        }
        let len = len as usize;
        if !(1..=PAYLOAD_LIMIT).contains(&len) {
            return Err(Error::Corrupt {
                offset,
                reason: "frame length out of range",
            });
        }
        let frame_bytes = (FRAME_HEADER_BYTES + len + CHECKSUM_BYTES) as u64;
        if remaining < frame_bytes {
            break;
        }
        payload.resize(len, 0);
        reader.read_exact(&mut payload)?;
        let payload_crc = read_u32(&mut reader)?;
        if crc32fast::hash(&payload) != payload_crc {
            return Err(Error::Corrupt {
                offset,
                reason: "payload CRC mismatch",
            });
        }
        let mutations =
            serde_json::from_slice::<Vec<Mutation>>(&payload).map_err(|_| Error::Corrupt {
                offset,
                reason: "invalid batch JSON",
            })?;
        validate(&mutations).map_err(|reason| Error::Corrupt { offset, reason })?;
        apply_to_index(&mut replay.index, mutations);
        replay.valid_bytes += frame_bytes;
        replay.frames += 1;
    }
    replay.recovered_bytes = file_bytes - replay.valid_bytes;
    Ok(replay)
}

fn read_u32(reader: &mut impl Read) -> io::Result<u32> {
    let mut bytes = [0; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

pub(crate) fn validate_key(key: &str) -> Result<(), &'static str> {
    if !(1..=1024).contains(&key.len()) {
        return Err("keys must be 1 to 1024 UTF-8 bytes");
    }
    Ok(())
}

fn validate(mutations: &[Mutation]) -> Result<(), &'static str> {
    if !(1..=BATCH_LIMIT).contains(&mutations.len()) {
        return Err("a batch must contain 1 to 1024 mutations");
    }
    let mut total_raw_bytes = 0;
    for mutation in mutations {
        match mutation {
            Mutation::Set { key, value } => {
                validate_key(key)?;
                if value.len() > 64 * 1024 {
                    return Err("values must be at most 64 KiB of UTF-8");
                }
                total_raw_bytes += key.len() + value.len();
            }
            Mutation::Delete { key } => {
                validate_key(key)?;
                total_raw_bytes += key.len();
            }
        }
        if total_raw_bytes > PAYLOAD_LIMIT {
            return Err("encoded batch must be at most 1 MiB");
        }
    }
    Ok(())
}

pub(crate) fn encode(mutations: &[Mutation]) -> Result<Vec<u8>, Error> {
    validate(mutations).map_err(Error::InvalidInput)?;
    let payload = serde_json::to_vec(mutations).map_err(io::Error::other)?;
    if payload.len() > PAYLOAD_LIMIT {
        return Err(Error::InvalidInput("encoded batch must be at most 1 MiB"));
    }
    Ok(payload)
}

pub(crate) fn write_frame(writer: &mut impl Write, payload: &[u8]) -> io::Result<()> {
    let len = (payload.len() as u32).to_le_bytes();
    writer.write_all(&len)?;
    writer.write_all(&crc32fast::hash(&len).to_le_bytes())?;
    writer.write_all(payload)?;
    writer.write_all(&crc32fast::hash(payload).to_le_bytes())
}

pub(crate) fn apply_to_index(index: &mut BTreeMap<String, String>, mutations: Vec<Mutation>) {
    for mutation in mutations {
        match mutation {
            Mutation::Set { key, value } => {
                index.insert(key, value);
            }
            Mutation::Delete { key } => {
                index.remove(&key);
            }
        }
    }
}

pub(crate) fn write_snapshot(file: &mut File, index: &BTreeMap<String, String>) -> io::Result<u64> {
    let mut writer = io::BufWriter::new(file);
    writer.write_all(MAGIC)?;
    let mut frames = 0;
    let mut count = 0;
    let mut payload = vec![b'['];
    let mut entry = Vec::new();
    for (key, value) in index {
        entry.clear();
        serde_json::to_writer(
            &mut entry,
            &Mutation::Set {
                key: key.clone(),
                value: value.clone(),
            },
        )
        .map_err(io::Error::other)?;

        let bytes = payload.len() + usize::from(count > 0) + entry.len() + 1;
        if count == BATCH_LIMIT || bytes > PAYLOAD_LIMIT {
            payload.push(b']');
            write_frame(&mut writer, &payload)?;
            frames += 1;
            payload.truncate(1);
            count = 0;
        }
        if count > 0 {
            payload.push(b',');
        }
        payload.extend_from_slice(&entry);
        count += 1;
    }
    if count > 0 {
        payload.push(b']');
        write_frame(&mut writer, &payload)?;
        frames += 1;
    }
    writer.flush()?;
    Ok(frames)
}
