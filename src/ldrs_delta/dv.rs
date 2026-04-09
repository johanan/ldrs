use anyhow::anyhow;
use object_store::{ObjectStore, PutPayload};
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const PORTABLE_ROARING_MAGIC: u32 = 1681511377;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeletionVectorDescriptor {
    storage_type: String,
    path_or_inline_dv: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    offset: Option<i32>,
    size_in_bytes: i32,
    cardinality: i64,
}

pub(crate) fn serialize_dv(bitmap: &RoaringTreemap) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&PORTABLE_ROARING_MAGIC.to_le_bytes());
    bitmap.serialize_into(&mut buf).expect("roaring serialize");
    buf
}

pub(crate) fn build_dv_inline(bitmap: &RoaringTreemap) -> DeletionVectorDescriptor {
    let bytes = serialize_dv(bitmap);
    DeletionVectorDescriptor {
        storage_type: "i".to_string(),
        path_or_inline_dv: z85::encode(&bytes),
        offset: None,
        size_in_bytes: bytes.len() as i32,
        cardinality: bitmap.len() as i64,
    }
}

pub(crate) fn load_dv_inline(
    descriptor: &DeletionVectorDescriptor,
) -> anyhow::Result<RoaringTreemap> {
    let bytes = z85::decode(&descriptor.path_or_inline_dv)
        .map_err(|e| anyhow::anyhow!("z85 decode failed: {}", e))?;
    deserialize_dv(&bytes)
}

pub(crate) async fn build_dv_file(
    store: &dyn ObjectStore,
    base_path: &object_store::path::Path,
    bitmap: &RoaringTreemap,
) -> anyhow::Result<DeletionVectorDescriptor> {
    let uuid = Uuid::new_v4();
    let dv_path = base_path.child(format!("deletion_vector_{}.bin", uuid));

    let buf = serialize_dv(bitmap);
    let mut file_bytes = Vec::with_capacity(4 + buf.len());
    file_bytes.extend_from_slice(&(buf.len() as u32).to_le_bytes());
    file_bytes.extend_from_slice(&buf);
    store.put(&dv_path, PutPayload::from(file_bytes)).await?;

    Ok(DeletionVectorDescriptor {
        storage_type: "u".to_string(),
        path_or_inline_dv: z85::encode(uuid.as_bytes()),
        offset: Some(1),
        size_in_bytes: buf.len() as i32,
        cardinality: bitmap.len() as i64,
    })
}

pub(crate) async fn load_dv_file(
    store: &dyn ObjectStore,
    base_path: &object_store::path::Path,
    descriptor: &DeletionVectorDescriptor,
) -> anyhow::Result<RoaringTreemap> {
    let uuid_bytes = z85::decode(&descriptor.path_or_inline_dv)
        .map_err(|e| anyhow::anyhow!("z85 decode failed: {}", e))?;
    let uuid = Uuid::from_slice(&uuid_bytes)
        .map_err(|e| anyhow::anyhow!("invalid UUID in DV descriptor: {}", e))?;
    let dv_path = base_path.child(format!("deletion_vector_{}.bin", uuid));

    let bytes = store.get(&dv_path).await?.bytes().await?;
    let offset_bytes = descriptor.offset.unwrap_or(0) as usize * 4;
    anyhow::ensure!(bytes.len() > offset_bytes, "DV file too short");
    deserialize_dv(&bytes[offset_bytes..])
}

pub(crate) fn deserialize_dv(bytes: &[u8]) -> anyhow::Result<RoaringTreemap> {
    anyhow::ensure!(bytes.len() >= 4, "DV too short: {} bytes", bytes.len());
    let magic = u32::from_le_bytes(bytes[0..4].try_into()?);
    anyhow::ensure!(
        magic == PORTABLE_ROARING_MAGIC,
        "bad DV magic: expected {}, got {}",
        PORTABLE_ROARING_MAGIC,
        magic
    );
    RoaringTreemap::deserialize_from(&bytes[4..])
        .map_err(|e| anyhow!("failed to deserialize roaring bitmap: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[test]
    fn roundtrip_with_values() {
        let mut bitmap = RoaringTreemap::new();
        bitmap.insert(0);
        bitmap.insert(5);
        bitmap.insert(100);

        let bytes = serialize_dv(&bitmap);
        let restored = deserialize_dv(&bytes).unwrap();
        assert_eq!(bitmap, restored);
    }

    #[test]
    fn roundtrip_empty() {
        let bitmap = RoaringTreemap::new();
        let bytes = serialize_dv(&bitmap);
        let restored = deserialize_dv(&bytes).unwrap();
        assert_eq!(restored.len(), 0);
    }

    #[test]
    fn inline_roundtrip() {
        let mut bitmap = RoaringTreemap::new();
        bitmap.insert(42);
        bitmap.insert(43);

        let descriptor = build_dv_inline(&bitmap);
        assert_eq!(descriptor.cardinality, 2);
        assert!(descriptor.offset.is_none());

        let restored = load_dv_inline(&descriptor).unwrap();
        assert_eq!(bitmap, restored);
    }

    #[test]
    fn bad_magic_rejected() {
        let mut bytes = vec![0u8; 8];
        bytes[0..4].copy_from_slice(&999u32.to_le_bytes());
        assert!(deserialize_dv(&bytes).is_err());
    }

    #[tokio::test]
    async fn file_roundtrip() {
        let store = InMemory::new();
        let base_path = object_store::path::Path::from("/table");

        let mut bitmap = RoaringTreemap::new();
        bitmap.insert(7);
        bitmap.insert(42);
        bitmap.insert(1000);

        let descriptor = build_dv_file(&store, &base_path, &bitmap).await.unwrap();
        assert_eq!(descriptor.storage_type, "u");
        assert_eq!(descriptor.cardinality, 3);
        assert!(descriptor.offset.is_some());

        let restored = load_dv_file(&store, &base_path, &descriptor).await.unwrap();
        assert_eq!(bitmap, restored);
    }
}
