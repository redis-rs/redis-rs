//! Defines the options and builder for vector fields using the FLAT indexing algorithm.
use super::{SchemaVectorField, VectorField};
use crate::{RedisWrite, ToRedisArgs};

/// Options for vectors using the FLAT indexing algorithm
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct FlatVectorOptions {
    block_size: Option<u32>,
}

impl ToRedisArgs for FlatVectorOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if let Some(block_size) = self.block_size {
            out.write_arg(b"BLOCK_SIZE");
            block_size.write_redis_args(out);
        }
    }

    fn num_of_args(&self) -> usize {
        let mut count = 0;
        if self.block_size.is_some() {
            count += 2;
        }
        count
    }
}

/// Builder for FLAT vector fields
#[must_use = "The builder has no effect until .build() is called"]
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct FlatVectorFieldBuilder {
    base: SchemaVectorField,
    block_size: Option<u32>,
}

impl FlatVectorFieldBuilder {
    pub(super) fn new(base: SchemaVectorField) -> Self {
        Self {
            base,
            block_size: None,
        }
    }

    /// Sets the block size for the FLAT index.
    ///
    /// The block size determines how vectors are organized in memory. BLOCK_SIZE amount of vectors are stored in a contiguous array.
    /// This is useful when the index is dynamic with respect to addition and deletion.
    /// The default block size is 1024.
    pub fn block_size(mut self, block_size: u32) -> Self {
        self.block_size = Some(block_size);
        self
    }

    /// Set the alias for the field.
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.base.base = self.base.base.alias(alias);
        self
    }

    /// Set index missing. This allows searching for missing values - documents that do not contain a specific field.
    pub fn index_missing(mut self, index_missing: bool) -> Self {
        self.base.base = self.base.base.index_missing(index_missing);
        self
    }

    /// Build the vector field.
    pub fn build(self) -> VectorField {
        VectorField::Flat(
            self.base,
            FlatVectorOptions {
                block_size: self.block_size,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::super::{DistanceMetric, VectorType};
    use super::*;
    use crate::schema;
    use crate::search::FtCreateCommand;

    static INDEX_NAME: &str = "index";
    static VECTOR_FIELD_NAME: &str = "embedding";
    static CUSTOM_ALIAS: &str = "custom_alias";

    // ============================================================================
    // VECTOR Field Tests
    // ============================================================================
    #[test]
    #[should_panic(expected = "Vector dimension must be positive (greater than 0)")]
    fn test_flat_vector_zero_dimension_panics() {
        let _ = VectorField::flat(VectorType::Float32, 0, DistanceMetric::Cosine);
    }

    #[test]
    fn test_vector_field_with_valid_dimension_one() {
        let schema = schema! {
            VECTOR_FIELD_NAME => VectorField::flat(VectorType::Float32, 1, DistanceMetric::Cosine)
                .build(),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 1 DISTANCE_METRIC COSINE"
        );
    }

    #[test]
    fn test_vector_field_with_alias() {
        let schema = schema! {
            VECTOR_FIELD_NAME => VectorField::flat(VectorType::Float32, 2, DistanceMetric::L2)
                .alias(CUSTOM_ALIAS)
                .build(),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA embedding AS custom_alias VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2"
        );
    }

    #[test]
    fn test_vector_field_with_indexmissing() {
        let schema = schema! {
            VECTOR_FIELD_NAME => VectorField::flat(VectorType::Float32, 2, DistanceMetric::L2)
                .index_missing(true)
                .build(),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2 INDEXMISSING"
        );
    }

    #[test]
    fn test_vector_field_flat_algorithm() {
        let schema = schema! {
            VECTOR_FIELD_NAME => VectorField::flat(VectorType::Float32, 2, DistanceMetric::L2)
                .block_size(1000)
                .build(),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA embedding VECTOR FLAT 8 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2 BLOCK_SIZE 1000"
        );
    }

    /// Every vector type must serialize to the token FT.CREATE expects.
    #[test]
    fn test_vector_types() {
        for (vector_type, expected) in [
            (VectorType::Float32, "FLOAT32"),
            (VectorType::Float64, "FLOAT64"),
            (VectorType::BFloat16, "BFLOAT16"),
            (VectorType::Float16, "FLOAT16"),
            (VectorType::Int8, "INT8"),
            (VectorType::UInt8, "UINT8"),
        ] {
            let ft_create = FtCreateCommand::new(
                INDEX_NAME,
                schema! {
                    VECTOR_FIELD_NAME => VectorField::flat(vector_type, 2, DistanceMetric::L2).build(),
                },
            );
            assert_eq!(
                ft_create.into_args(),
                format!(
                    "FT.CREATE index SCHEMA embedding VECTOR FLAT 6 TYPE {expected} DIM 2 DISTANCE_METRIC L2"
                )
            );
        }
    }

    /// Every distance metric must serialize to the token FT.CREATE expects.
    #[test]
    fn test_distance_metrics() {
        for (distance_metric, expected) in [
            (DistanceMetric::L2, "L2"),
            (DistanceMetric::IP, "IP"),
            (DistanceMetric::Cosine, "COSINE"),
        ] {
            let ft_create = FtCreateCommand::new(
                INDEX_NAME,
                schema! {
                    VECTOR_FIELD_NAME => VectorField::flat(VectorType::Float32, 2, distance_metric).build(),
                },
            );
            assert_eq!(
                ft_create.into_args(),
                format!(
                    "FT.CREATE index SCHEMA embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC {expected}"
                )
            );
        }
    }
}
