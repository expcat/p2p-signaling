use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use uuid::Uuid;

pub const DEFAULT_CHUNK_SIZE: u64 = 256 * 1024;
pub const MAX_CHUNK_SIZE: u64 = 4 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkRange {
    pub start: u64,
    pub end: u64,
}

impl ChunkRange {
    pub fn new(start: u64, end: u64) -> Self {
        Self { start, end }
    }

    pub fn is_empty(&self) -> bool {
        self.start >= self.end
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeSet {
    ranges: Vec<ChunkRange>,
}

impl RangeSet {
    pub fn from_ranges(ranges: Vec<ChunkRange>) -> Self {
        let mut set = Self::default();
        for range in ranges {
            set.insert_range(range);
        }
        set
    }

    pub fn ranges(&self) -> &[ChunkRange] {
        &self.ranges
    }

    pub fn into_ranges(self) -> Vec<ChunkRange> {
        self.ranges
    }

    pub fn insert(&mut self, index: u64) {
        self.insert_range(ChunkRange::new(index, index + 1));
    }

    pub fn insert_range(&mut self, mut new_range: ChunkRange) {
        if new_range.is_empty() {
            return;
        }

        let mut merged = Vec::with_capacity(self.ranges.len() + 1);
        let mut inserted = false;

        for range in self.ranges.drain(..) {
            if range.end < new_range.start {
                merged.push(range);
            } else if new_range.end < range.start {
                if !inserted {
                    merged.push(new_range.clone());
                    inserted = true;
                }
                merged.push(range);
            } else {
                new_range.start = new_range.start.min(range.start);
                new_range.end = new_range.end.max(range.end);
            }
        }

        if !inserted {
            merged.push(new_range);
        }

        self.ranges = merged;
    }

    pub fn missing_ranges(&self, total_chunks: u64) -> Vec<ChunkRange> {
        let mut missing = Vec::new();
        let mut cursor = 0;

        for range in &self.ranges {
            if cursor < range.start {
                missing.push(ChunkRange::new(cursor, range.start.min(total_chunks)));
            }
            cursor = cursor.max(range.end).min(total_chunks);
        }

        if cursor < total_chunks {
            missing.push(ChunkRange::new(cursor, total_chunks));
        }

        missing
            .into_iter()
            .filter(|range| !range.is_empty())
            .collect()
    }

    pub fn completed_chunks(&self) -> u64 {
        self.ranges
            .iter()
            .map(|range| range.end.saturating_sub(range.start))
            .sum()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct FileMetadata {
    pub transfer_id: String,
    pub file_name: String,
    pub file_size: u64,
    pub chunk_size: u64,
    pub total_chunks: u64,
    pub modified_millis: Option<u64>,
    pub sample_hash: String,
    pub file_hash: String,
}

impl FileMetadata {
    pub fn bytes_for_chunks(&self, chunks: u64) -> u64 {
        chunks.saturating_mul(self.chunk_size).min(self.file_size)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum TransferDirection {
    Send,
    Receive,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum TransferStatus {
    Offered,
    Accepted,
    Paused,
    Complete,
    Cancelled,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransferManifest {
    pub version: u32,
    pub direction: TransferDirection,
    pub status: TransferStatus,
    pub metadata: FileMetadata,
    pub source_path: Option<PathBuf>,
    pub output_path: Option<PathBuf>,
    pub temp_path: Option<PathBuf>,
    pub completed_chunks: Vec<ChunkRange>,
    pub failure: Option<String>,
}

impl TransferManifest {
    pub fn new_sender(metadata: FileMetadata, source_path: PathBuf) -> Self {
        Self {
            version: 1,
            direction: TransferDirection::Send,
            status: TransferStatus::Offered,
            metadata,
            source_path: Some(source_path),
            output_path: None,
            temp_path: None,
            completed_chunks: Vec::new(),
            failure: None,
        }
    }

    pub fn new_receiver(metadata: FileMetadata, output_path: PathBuf) -> Self {
        let temp_path = part_path(&output_path);
        Self {
            version: 1,
            direction: TransferDirection::Receive,
            status: TransferStatus::Accepted,
            metadata,
            source_path: None,
            output_path: Some(output_path),
            temp_path: Some(temp_path),
            completed_chunks: Vec::new(),
            failure: None,
        }
    }

    pub fn completed_set(&self) -> RangeSet {
        RangeSet::from_ranges(self.completed_chunks.clone())
    }

    pub fn completed_bytes(&self) -> u64 {
        self.metadata
            .bytes_for_chunks(self.completed_set().completed_chunks())
    }

    pub fn is_complete(&self) -> bool {
        self.status == TransferStatus::Complete
            || self.completed_set().completed_chunks() >= self.metadata.total_chunks
    }
}

#[derive(Debug, Clone)]
pub struct RawChunk {
    pub index: u64,
    pub offset: u64,
    pub bytes: Vec<u8>,
}

#[derive(Clone)]
pub struct TransferStore {
    root: PathBuf,
}

impl TransferStore {
    pub fn platform_default() -> Result<Self> {
        let root = crate::platform_dirs::data_dir()
            .ok_or_else(|| anyhow::anyhow!("找不到本地数据目录"))?
            .join("p2p-signaling")
            .join("transfers");
        Ok(Self::new(root))
    }

    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub async fn save(&self, manifest: &TransferManifest) -> Result<()> {
        tokio::fs::create_dir_all(&self.root).await?;
        let path = self.manifest_path(&manifest.metadata.transfer_id);
        let text = serde_json::to_string_pretty(manifest)?;
        tokio::fs::write(path, text).await?;
        Ok(())
    }

    pub async fn load(&self, transfer_id: &str) -> Result<Option<TransferManifest>> {
        let path = self.manifest_path(transfer_id);
        match tokio::fs::read_to_string(path).await {
            Ok(text) => Ok(Some(serde_json::from_str(&text)?)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn manifest_path(&self, transfer_id: &str) -> PathBuf {
        self.root.join(format!("{transfer_id}.json"))
    }
}

pub async fn metadata_for_path(path: &Path) -> Result<FileMetadata> {
    let metadata = tokio::fs::metadata(path)
        .await
        .with_context(|| format!("无法读取文件信息：{}", path.display()))?;

    if !metadata.is_file() {
        anyhow::bail!("只能发送普通文件：{}", path.display());
    }

    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| "unnamed-file".into());
    let file_size = metadata.len();
    let modified_millis = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .and_then(|duration| u64::try_from(duration.as_millis()).ok());
    let sample_hash = sample_hash(path, file_size).await?;
    let file_hash = hash_file(path).await?;
    let total_chunks = file_size.div_ceil(DEFAULT_CHUNK_SIZE);
    let transfer_id = transfer_id_for(&file_name, file_size, modified_millis, &sample_hash);

    Ok(FileMetadata {
        transfer_id,
        file_name,
        file_size,
        chunk_size: DEFAULT_CHUNK_SIZE,
        total_chunks,
        modified_millis,
        sample_hash,
        file_hash,
    })
}

pub async fn open_chunk_source(path: &Path) -> Result<tokio::fs::File> {
    tokio::fs::File::open(path)
        .await
        .with_context(|| format!("无法打开文件：{}", path.display()))
}

pub async fn read_chunk_from(
    file: &mut tokio::fs::File,
    index: u64,
    chunk_size: u64,
) -> Result<RawChunk> {
    let offset = index.saturating_mul(chunk_size);
    file.seek(std::io::SeekFrom::Start(offset)).await?;

    let mut bytes = vec![0; chunk_size as usize];
    let mut read = 0;
    while read < bytes.len() {
        let count = file.read(&mut bytes[read..]).await?;
        if count == 0 {
            break;
        }
        read += count;
    }
    bytes.truncate(read);

    Ok(RawChunk {
        index,
        offset,
        bytes,
    })
}

pub async fn open_chunk_sink(path: &Path) -> Result<tokio::fs::File> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let mut options = tokio::fs::OpenOptions::new();
    options.create(true).write(true).read(true);
    Ok(options.open(path).await?)
}

pub async fn write_chunk_to(
    file: &mut tokio::fs::File,
    chunk: &RawChunk,
    chunk_size: u64,
) -> Result<()> {
    let offset = chunk.index.saturating_mul(chunk_size);
    if offset != chunk.offset {
        anyhow::bail!("chunk {} 偏移不匹配", chunk.index);
    }
    file.seek(std::io::SeekFrom::Start(offset)).await?;
    file.write_all(&chunk.bytes).await?;
    Ok(())
}

pub async fn hash_file(path: &Path) -> Result<String> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        use std::io::Read;

        let mut file = std::fs::File::open(&path)?;
        let mut hash = Sha256::new();
        let mut buffer = vec![0; 1024 * 1024];

        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hash.update(&buffer[..read]);
        }

        Ok(bytes_to_hex(hash.finalize().as_slice()))
    })
    .await?
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hash = Sha256::new();
    hash.update(bytes);
    bytes_to_hex(hash.finalize().as_slice())
}

/// 校验对端 FileOffer 的元数据，防止恶意 chunk_size/total_chunks 造成超大稀疏文件
/// 或带路径的文件名逃出保存目录。通过时会把 file_name 归一为纯文件名。
pub fn validate_offer_metadata(metadata: &mut FileMetadata) -> std::result::Result<(), String> {
    let Some(file_name) = Path::new(&metadata.file_name)
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
    else {
        return Err(format!("文件名不合法：{}", metadata.file_name));
    };
    metadata.file_name = file_name.to_owned();

    if metadata.chunk_size == 0 || metadata.chunk_size > MAX_CHUNK_SIZE {
        return Err(format!("分块大小不合法：{}", metadata.chunk_size));
    }
    if metadata.total_chunks != metadata.file_size.div_ceil(metadata.chunk_size) {
        return Err("分块数量与文件大小不一致".into());
    }

    Ok(())
}

pub fn part_path(path: &Path) -> PathBuf {
    let mut name = path
        .file_name()
        .and_then(|value| value.to_str())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    name.push_str(".part");

    path.with_file_name(name)
}

fn transfer_id_for(
    file_name: &str,
    file_size: u64,
    modified_millis: Option<u64>,
    sample_hash: &str,
) -> String {
    let seed = format!("{file_name}:{file_size}:{modified_millis:?}:{sample_hash}");
    let hash = sha256_hex(seed.as_bytes());
    format!("file-{}", &hash[..24])
}

async fn sample_hash(path: &Path, file_size: u64) -> Result<String> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        use std::io::{Read, Seek};

        let mut file = std::fs::File::open(&path)?;
        let mut hash = Sha256::new();
        let sample_len = 4096_u64.min(file_size);

        if sample_len > 0 {
            let mut head = vec![0; sample_len as usize];
            file.read_exact(&mut head)?;
            hash.update(head);
        }

        if file_size > sample_len {
            file.seek(std::io::SeekFrom::Start(
                file_size.saturating_sub(sample_len),
            ))?;
            let mut tail = vec![0; sample_len as usize];
            file.read_exact(&mut tail)?;
            hash.update(tail);
        }

        Ok(bytes_to_hex(hash.finalize().as_slice()))
    })
    .await?
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    use std::fmt::Write;

    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(text, "{byte:02x}");
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges_merge_and_report_missing_chunks() {
        let mut set = RangeSet::default();
        set.insert(2);
        set.insert(0);
        set.insert(1);
        set.insert_range(ChunkRange::new(5, 7));

        assert_eq!(
            set.ranges(),
            &[ChunkRange::new(0, 3), ChunkRange::new(5, 7)]
        );
        assert_eq!(
            set.missing_ranges(8),
            vec![ChunkRange::new(3, 5), ChunkRange::new(7, 8)]
        );
    }

    #[test]
    fn file_metadata_reads_current_camel_case_fields() {
        let camel_case = r#"{
            "transferId": "file-test",
            "fileName": "photo.png",
            "fileSize": 42,
            "chunkSize": 32768,
            "totalChunks": 1,
            "modifiedMillis": 7,
            "sampleHash": "sample",
            "fileHash": "hash"
        }"#;

        let camel: FileMetadata = serde_json::from_str(camel_case).unwrap();

        assert_eq!(camel.transfer_id, "file-test");
        assert_eq!(camel.file_name, "photo.png");
        assert_eq!(camel.file_size, 42);
    }

    #[tokio::test]
    async fn manifest_round_trips_from_store() {
        let root = std::env::temp_dir().join(format!("p2p-transfer-test-{}", Uuid::new_v4()));
        let store = TransferStore::new(root.clone());
        let manifest = TransferManifest::new_sender(
            FileMetadata {
                transfer_id: "file-test".into(),
                file_name: "a.bin".into(),
                file_size: 3,
                chunk_size: DEFAULT_CHUNK_SIZE,
                total_chunks: 1,
                modified_millis: Some(1),
                sample_hash: "sample".into(),
                file_hash: "hash".into(),
            },
            PathBuf::from("/tmp/a.bin"),
        );

        store.save(&manifest).await.unwrap();
        assert_eq!(store.load("file-test").await.unwrap(), Some(manifest));
        tokio::fs::remove_dir_all(root).await.unwrap();
    }

    #[tokio::test]
    async fn metadata_transfer_id_is_stable_for_same_file() {
        let root = std::env::temp_dir().join(format!("p2p-metadata-test-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&root).await.unwrap();
        let file = root.join("sample.txt");
        tokio::fs::write(&file, b"hello world").await.unwrap();

        let first = metadata_for_path(&file).await.unwrap();
        let second = metadata_for_path(&file).await.unwrap();
        assert_eq!(first.transfer_id, second.transfer_id);
        assert_eq!(first.total_chunks, 1);

        tokio::fs::remove_dir_all(root).await.unwrap();
    }

    #[test]
    fn offer_metadata_validation_rejects_malicious_values() {
        let make =
            |file_name: &str, file_size: u64, chunk_size: u64, total_chunks: u64| FileMetadata {
                transfer_id: "file-test".into(),
                file_name: file_name.into(),
                file_size,
                chunk_size,
                total_chunks,
                modified_millis: None,
                sample_hash: "sample".into(),
                file_hash: "hash".into(),
            };

        let mut valid = make("a.bin", DEFAULT_CHUNK_SIZE + 1, DEFAULT_CHUNK_SIZE, 2);
        assert!(validate_offer_metadata(&mut valid).is_ok());

        let mut zero_chunk_size = make("a.bin", 10, 0, 1);
        assert!(validate_offer_metadata(&mut zero_chunk_size).is_err());

        let mut oversized_chunk = make("a.bin", 10, MAX_CHUNK_SIZE + 1, 1);
        assert!(validate_offer_metadata(&mut oversized_chunk).is_err());

        let mut mismatched_chunks = make("a.bin", DEFAULT_CHUNK_SIZE, DEFAULT_CHUNK_SIZE, 999);
        assert!(validate_offer_metadata(&mut mismatched_chunks).is_err());

        let mut traversal = make("../../evil.bin", 10, DEFAULT_CHUNK_SIZE, 1);
        assert!(validate_offer_metadata(&mut traversal).is_ok());
        assert_eq!(traversal.file_name, "evil.bin");

        let mut parent_only = make("..", 10, DEFAULT_CHUNK_SIZE, 1);
        assert!(validate_offer_metadata(&mut parent_only).is_err());
    }

    #[tokio::test]
    async fn zero_byte_file_has_no_chunks_and_counts_as_complete() {
        let root = std::env::temp_dir().join(format!("p2p-empty-test-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&root).await.unwrap();
        let file = root.join("empty.bin");
        tokio::fs::write(&file, b"").await.unwrap();

        let metadata = metadata_for_path(&file).await.unwrap();
        assert_eq!(metadata.total_chunks, 0);
        assert_eq!(metadata.file_size, 0);

        let manifest = TransferManifest::new_receiver(metadata, root.join("saved.bin"));
        assert!(manifest
            .completed_set()
            .missing_ranges(manifest.metadata.total_chunks)
            .is_empty());
        assert!(manifest.is_complete());

        tokio::fs::remove_dir_all(root).await.unwrap();
    }

    #[tokio::test]
    async fn chunks_write_out_of_order_and_rebuild_file_hash() {
        let root = std::env::temp_dir().join(format!("p2p-chunk-test-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&root).await.unwrap();
        let source = root.join("source.bin");
        let output = root.join("output.bin.part");
        let chunk_size = 32 * 1024;
        let bytes = (0..70_000)
            .map(|value| (value % 251) as u8)
            .collect::<Vec<_>>();
        tokio::fs::write(&source, &bytes).await.unwrap();

        let mut reader = open_chunk_source(&source).await.unwrap();
        let second = read_chunk_from(&mut reader, 1, chunk_size).await.unwrap();
        let first = read_chunk_from(&mut reader, 0, chunk_size).await.unwrap();
        let third = read_chunk_from(&mut reader, 2, chunk_size).await.unwrap();

        let mut sink = open_chunk_sink(&output).await.unwrap();
        for chunk in [second, first, third] {
            write_chunk_to(&mut sink, &chunk, chunk_size).await.unwrap();
        }
        sink.flush().await.unwrap();

        assert_eq!(
            hash_file(&source).await.unwrap(),
            hash_file(&output).await.unwrap()
        );
        tokio::fs::remove_dir_all(root).await.unwrap();
    }
}
