use std::{fs, path::Path};

use mini_kv_store::{Error, Mutation, Store};
use tempfile::tempdir;

fn set(key: &str, value: &str) -> Mutation {
    Mutation::Set {
        key: key.into(),
        value: value.into(),
    }
}

fn delete(key: &str) -> Mutation {
    Mutation::Delete { key: key.into() }
}

fn journal(path: &Path) -> Vec<u8> {
    fs::read(path.join("wal")).unwrap()
}

#[test]
fn batches_replay_in_order_and_reject_invalid_changes_before_writing() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("data");
    let mut store = Store::open(&path).unwrap();
    store
        .apply(vec![
            set("nhiệt độ", "23.4"),
            set("old", "value"),
            set("nhiệt độ", "25.0"),
            delete("old"),
            delete("never-existed"),
            set("empty", ""),
        ])
        .unwrap();
    let baseline = journal(&path);
    let oversize = "\0".repeat(64 * 1024);
    for changes in [
        vec![],
        vec![set("new", "valid"), delete("")],
        (0..1025).map(|_| delete("a")).collect(),
        vec![set("x", &"a".repeat(64 * 1024 + 1))],
        (0..3).map(|_| set("oversize", &oversize)).collect(),
    ] {
        assert!(matches!(store.apply(changes), Err(Error::InvalidInput(_))));
        assert_eq!(journal(&path), baseline);
    }
    drop(store);

    let store = Store::open(&path).unwrap();
    assert_eq!(store.get("nhiệt độ").unwrap(), Some("25.0"));
    assert_eq!(store.get("old").unwrap(), None);
    assert_eq!(store.get("new").unwrap(), None);
    assert_eq!(store.get("empty").unwrap(), Some(""));
    assert_eq!(store.stats().unwrap().frames, 1);
}

#[test]
fn every_incomplete_prefix_discards_the_entire_last_batch() {
    let temp = tempdir().unwrap();
    let path = temp.path();
    let mut store = Store::open(path).unwrap();
    store.apply(vec![set("keep", "old")]).unwrap();
    let start = journal(path).len();
    store
        .apply(vec![delete("keep"), set("new", "new")])
        .unwrap();
    let complete = journal(path);
    drop(store);

    for cut in 1..complete.len() - start {
        fs::write(path.join("wal"), &complete[..start + cut]).unwrap();
        let mut store = Store::open(path).unwrap();
        assert_eq!(store.stats().unwrap().recovered_bytes, cut as u64);
        assert_eq!(store.stats().unwrap().wal_bytes, start as u64);
        assert_eq!(store.get("keep").unwrap(), Some("old"));
        assert_eq!(store.get("new").unwrap(), None);
        store.apply(vec![set("after_recovery", "ok")]).unwrap();
        drop(store);

        let store = Store::open(path).unwrap();
        assert_eq!(store.stats().unwrap().recovered_bytes, 0);
        assert_eq!(store.get("after_recovery").unwrap(), Some("ok"));
    }
    fs::write(path.join("wal"), complete).unwrap();
    let store = Store::open(path).unwrap();
    assert_eq!(store.get("keep").unwrap(), None);
    assert_eq!(store.get("new").unwrap(), Some("new"));
}

#[test]
fn corrupted_complete_frames_are_never_truncated() {
    let temp = tempdir().unwrap();
    let path = temp.path();
    let mut store = Store::open(path).unwrap();
    store.apply(vec![set("keep", "old")]).unwrap();
    let offset = journal(path).len();
    store.apply(vec![set("new", "fresh")]).unwrap();
    let complete = journal(path);
    drop(store);

    for (index, reason) in [
        (offset, "length CRC mismatch"),
        (offset + 8, "payload CRC mismatch"),
        (complete.len() - 1, "payload CRC mismatch"),
    ] {
        let mut damaged = complete.clone();
        damaged[index] ^= 1;
        fs::write(path.join("wal"), &damaged).unwrap();
        assert!(matches!(
            Store::open(path),
            Err(Error::Corrupt { offset: found, reason: message })
                if found == offset as u64 && message == reason
        ));
        assert_eq!(journal(path), damaged);
    }
    let mut damaged = complete;
    let outside_limit = 1_048_577u32.to_le_bytes();
    damaged[offset..offset + 4].copy_from_slice(&outside_limit);
    damaged[offset + 4..offset + 8].copy_from_slice(&crc32fast::hash(&outside_limit).to_le_bytes());
    fs::write(path.join("wal"), &damaged).unwrap();
    assert!(matches!(
        Store::open(path),
        Err(Error::Corrupt {
            reason: "frame length out of range",
            ..
        })
    ));
    assert_eq!(journal(path), damaged);
}

#[test]
fn lock_survives_wal_replacement_and_reopen_ignores_staged_snapshot() {
    let temp = tempdir().unwrap();
    let path = temp.path();
    let mut store = Store::open(path).unwrap();
    store.apply(vec![set("mode", "left")]).unwrap();
    store.apply(vec![set("mode", "right")]).unwrap();
    assert!(matches!(Store::open(path), Err(Error::Busy)));
    let report = store.compact().unwrap();
    assert!(report.after_bytes < report.before_bytes);
    assert!(matches!(Store::open(path), Err(Error::Busy)));

    store.apply(vec![set("mode", "forward")]).unwrap();
    drop(store);
    fs::write(path.join(".wal.next"), b"interrupted snapshot").unwrap();
    let store = Store::open(path).unwrap();
    assert_eq!(store.get("mode").unwrap(), Some("forward"));
    assert!(!path.join(".wal.next").exists());
}

#[test]
fn compaction_splits_at_both_frame_limits_and_preserves_all_values() {
    let temp = tempdir().unwrap();
    let path = temp.path();
    let mut store = Store::open(path).unwrap();
    let large = "x".repeat(64 * 1024);
    for _ in 0..2 {
        for range in [0..1024, 1024..1100] {
            store
                .apply(range.map(|i| set(&format!("a{i:04}"), "small")).collect())
                .unwrap();
        }
        for range in [0..10, 10..20] {
            store
                .apply(range.map(|i| set(&format!("z{i:04}"), &large)).collect())
                .unwrap();
        }
    }
    let expected = store.entries().unwrap().clone();
    let result = store.compact().unwrap();
    assert!(result.after_bytes < result.before_bytes);
    assert_eq!(store.stats().unwrap().frames, 3);
    assert_eq!(store.entries().unwrap(), &expected);
    drop(store);

    let mut store = Store::open(path).unwrap();
    assert_eq!(store.entries().unwrap(), &expected);
    let no_growth = store.compact().unwrap();
    assert_eq!(no_growth.before_bytes, no_growth.after_bytes);
    assert!(!path.join(".wal.next").exists());
    store
        .apply(expected.keys().take(1024).map(|key| delete(key)).collect())
        .unwrap();
    store
        .apply(expected.keys().skip(1024).map(|key| delete(key)).collect())
        .unwrap();
    assert_eq!(store.compact().unwrap().after_bytes, 8);
    drop(store);
    assert!(Store::open(path).unwrap().entries().unwrap().is_empty());
}

#[test]
fn io_failure_prevents_using_the_handle_until_reopen() {
    let temp = tempdir().unwrap();
    let path = temp.path();
    let mut store = Store::open(path).unwrap();
    store.apply(vec![set("mode", "safe")]).unwrap();
    fs::write(path.join(".wal.next"), b"staging conflict").unwrap();
    assert!(matches!(store.compact(), Err(Error::Io(_))));
    assert!(matches!(store.get("mode"), Err(Error::NeedsReopen)));
    assert!(matches!(
        store.apply(vec![delete("mode")]),
        Err(Error::NeedsReopen)
    ));
    drop(store);

    let store = Store::open(path).unwrap();
    assert_eq!(store.get("mode").unwrap(), Some("safe"));
}
