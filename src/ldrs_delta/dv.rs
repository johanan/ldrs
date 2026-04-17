use crc::{Crc, CRC_32_ISO_HDLC};
use object_store::{ObjectStore, PutPayload};
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const PORTABLE_ROARING_MAGIC: u32 = 1681511377;
const DV_FILE_VERSION: u8 = 1;

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

pub(crate) fn build_dv_inline(bytes: &[u8], cardinality: i64) -> DeletionVectorDescriptor {
    DeletionVectorDescriptor {
        storage_type: "i".to_string(),
        path_or_inline_dv: z85::encode(bytes),
        offset: None,
        size_in_bytes: bytes.len() as i32,
        cardinality,
    }
}

pub(crate) async fn build_dv_file(
    store: &dyn ObjectStore,
    base_path: &object_store::path::Path,
    bytes: &[u8],
    cardinality: i64,
) -> anyhow::Result<DeletionVectorDescriptor> {
    let uuid = Uuid::new_v4();
    let dv_path = base_path.child(format!("deletion_vector_{}.bin", uuid));

    // File format:
    // [version: 1 byte] [dv_size: 4 bytes BE] [bytes: magic + bitmap] [crc32: 4 bytes BE]
    let crc32 = Crc::<u32>::new(&CRC_32_ISO_HDLC);
    let checksum = crc32.checksum(bytes);

    let mut file_bytes = Vec::with_capacity(1 + 4 + bytes.len() + 4);
    file_bytes.push(DV_FILE_VERSION);
    file_bytes.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    file_bytes.extend_from_slice(bytes);
    file_bytes.extend_from_slice(&checksum.to_be_bytes());
    store.put(&dv_path, PutPayload::from(file_bytes)).await?;

    Ok(DeletionVectorDescriptor {
        storage_type: "u".to_string(),
        path_or_inline_dv: z85::encode(uuid.as_bytes()),
        offset: Some(1),
        size_in_bytes: bytes.len() as i32,
        cardinality,
    })
}
