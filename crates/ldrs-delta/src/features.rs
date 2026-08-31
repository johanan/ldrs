use delta_kernel::table_features::{TableFeature, MAX_VALID_WRITER_VERSION};
use delta_kernel::Snapshot;

pub(crate) fn check_writer_version(snapshot: &Snapshot) -> Result<(), anyhow::Error> {
    let version = snapshot
        .table_configuration()
        .protocol()
        .min_writer_version();
    match version > MAX_VALID_WRITER_VERSION {
        true => Err(anyhow::anyhow!(
            "it is at writer version {version}, above the highest known version {MAX_VALID_WRITER_VERSION}"
        )),
        false => Ok(()),
    }
}

/// The table's declared writer features. Empty when the protocol names none.
pub(crate) fn writer_features(snapshot: &Snapshot) -> &[TableFeature] {
    snapshot
        .table_configuration()
        .protocol()
        .writer_features()
        .unwrap_or_default()
}

pub(crate) fn files_are_enumerable(feature: &TableFeature) -> bool {
    use TableFeature::*;
    match feature {
        AppendOnly
        | Invariants
        | CheckConstraints
        | GeneratedColumns
        | IdentityColumns
        | InCommitTimestamp
        | DomainMetadata
        | RowTracking
        | ColumnMapping
        | TypeWidening
        | TypeWideningPreview
        | VariantType
        | VariantTypePreview
        | VariantShredding
        | VariantShreddingPreview
        | GeospatialType
        | MaterializePartitionColumns
        | AllowColumnDefaults
        | ClusteredTable
        | TimestampWithoutTimezone
        | DeletionVectors
        | VacuumProtocolCheck
        | ChangeDataFeed
        | V2Checkpoint => true,
        // Iceberg metadata sits outside `_delta_log` and is not named by any add action.
        IcebergCompatV1 | IcebergCompatV2 | IcebergCompatV3 | AdaptiveMetadataPreview => false,
        // Commits can live outside `_delta_log`, so a snapshot read from the store may be stale
        // and a committed file could look unreferenced.
        CatalogManaged | CatalogOwnedPreview => false,
        Unknown(_) => false,
    }
}

pub(crate) fn rewrite_is_supported(feature: &TableFeature) -> bool {
    use TableFeature::*;
    match feature {
        AppendOnly
        | Invariants
        | CheckConstraints
        | GeneratedColumns
        | IdentityColumns
        | InCommitTimestamp
        | DomainMetadata
        | ColumnMapping
        | TypeWidening
        | TypeWideningPreview
        | VariantType
        | VariantTypePreview
        | VariantShredding
        | VariantShreddingPreview
        | GeospatialType
        | MaterializePartitionColumns
        | AllowColumnDefaults
        | TimestampWithoutTimezone
        | DeletionVectors
        | VacuumProtocolCheck
        | ChangeDataFeed
        | V2Checkpoint
        | IcebergCompatV1
        | IcebergCompatV2
        | IcebergCompatV3
        | AdaptiveMetadataPreview
        | CatalogManaged
        | CatalogOwnedPreview => true,
        // Each rewritten row keeps its row id, so the new file has to materialize them into a
        // hidden column and claim a `baseRowId` from the `domainMetadata` high watermark.
        RowTracking => false,
        // The table declares a clustering layout that bin-packing discards while its metadata keeps
        // claiming it.
        ClusteredTable => false,
        Unknown(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enumerable_and_rewritable_differ_on_row_tracking_and_clustering() {
        for feature in [TableFeature::RowTracking, TableFeature::ClusteredTable] {
            assert!(files_are_enumerable(&feature));
            assert!(!rewrite_is_supported(&feature));
        }
    }

    #[test]
    fn an_unknown_feature_is_refused_by_both() {
        let feature = TableFeature::Unknown("someFutureFeature".to_string());
        assert!(!files_are_enumerable(&feature));
        assert!(!rewrite_is_supported(&feature));
    }
}
