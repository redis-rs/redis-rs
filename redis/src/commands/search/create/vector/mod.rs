//! Defines the vector field types used with the FT.CREATE command.
//!
//! A vector field is written as `VECTOR <algorithm> <attribute_count> <attributes...>`, where the
//! count covers the shared attributes (`TYPE`, `DIM`, `DISTANCE_METRIC`) plus whichever
//! algorithm-specific ones were set. [`VectorField`] writes the whole field. Each algorithm's
//! options and builder live in their own module; the options write their own attributes and
//! report their argument count through `ToRedisArgs::num_of_args`, which [`VectorField`] adds to
//! the shared count to produce the count written on the wire.
use super::fields::{BaseSchemaField, FieldType};
use crate::{RedisWrite, ToRedisArgs};

mod flat;

pub use flat::*;

/// The indexing algorithm of a vector field, with its algorithm-specific options.
#[derive(Debug, Clone)]
pub(crate) enum VectorAlgorithm {
    Flat(FlatVectorOptions),
}

impl VectorAlgorithm {
    fn name(&self) -> &'static [u8] {
        match self {
            Self::Flat(_) => b"FLAT",
        }
    }

    /// The number of algorithm-specific arguments.
    fn num_of_args(&self) -> usize {
        match self {
            Self::Flat(options) => options.num_of_args(),
        }
    }

    fn write_options<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        match self {
            Self::Flat(options) => options.write_redis_args(out),
        }
    }
}

/// Vector type for vector fields
#[derive(Debug, Copy, Clone)]
#[non_exhaustive]
#[allow(missing_docs)]
pub enum VectorType {
    Float32,
    Float64,
    BFloat16,
    Float16,
    Int8,
    UInt8,
}

impl ToRedisArgs for VectorType {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(match self {
            Self::Float32 => b"FLOAT32",
            Self::Float64 => b"FLOAT64",
            Self::BFloat16 => b"BFLOAT16",
            Self::Float16 => b"FLOAT16",
            Self::Int8 => b"INT8",
            Self::UInt8 => b"UINT8",
        });
    }
}

/// [Distance metric](https://redis.io/docs/latest/develop/ai/search-and-query/vectors/#distance-metrics/) for vector fields
#[derive(Debug, Copy, Clone)]
#[non_exhaustive]
pub enum DistanceMetric {
    /// Euclidean distance between two vectors.
    L2,
    /// Inner product of two vectors.
    IP,
    /// Cosine distance of two vectors.
    Cosine,
}

impl ToRedisArgs for DistanceMetric {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(match self {
            Self::L2 => b"L2",
            Self::IP => b"IP",
            Self::Cosine => b"COSINE",
        });
    }
}

/// The attributes that every vector field has, whatever its algorithm.
#[derive(Debug, Clone)]
pub(crate) struct VectorFieldCommon {
    base: BaseSchemaField,
    vector_type: VectorType,
    dim: u32,
    distance_metric: DistanceMetric,
}

impl VectorFieldCommon {
    /// The number of arguments written for `TYPE`, `DIM` and `DISTANCE_METRIC`.
    const NUM_OF_ARGS: usize = 6;

    pub(crate) fn alias(mut self, alias: impl Into<String>) -> Self {
        self.base = self.base.alias(alias);
        self
    }

    pub(crate) fn index_missing(mut self, index_missing: bool) -> Self {
        self.base = self.base.index_missing(index_missing);
        self
    }
}

/// Represents a vector field in the schema, built through a per-algorithm builder.
///
/// # Algorithms
///
/// - **FLAT**: Brute-force exact search. Best for small datasets (< 1M vectors) where perfect accuracy is required.
///
/// # Examples
///
/// ```rust
/// use redis::search::*;
///
/// // FLAT index for exact search
/// let flat_field = VectorField::flat(VectorType::Float32, 128, DistanceMetric::Cosine)
///     .block_size(1000)
///     .build();
/// ```
#[must_use = "Vector field has no effect unless inserted into a schema"]
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct VectorField {
    common: VectorFieldCommon,
    algorithm: VectorAlgorithm,
}

impl VectorField {
    /// Set the alias for the field.
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.common = self.common.alias(alias);
        self
    }

    /// Set index missing. This allows searching for missing values - documents that do not contain a specific field.
    pub fn index_missing(mut self, index_missing: bool) -> Self {
        self.common = self.common.index_missing(index_missing);
        self
    }
}

impl ToRedisArgs for VectorField {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        let common = &self.common;

        if let Some(alias) = &common.base.alias {
            out.write_arg(b"AS");
            alias.write_redis_args(out);
        }
        common.base.field_type.write_redis_args(out);
        out.write_arg(self.algorithm.name());

        let attributes_count = VectorFieldCommon::NUM_OF_ARGS + self.algorithm.num_of_args();
        attributes_count.write_redis_args(out);

        out.write_arg(b"TYPE");
        common.vector_type.write_redis_args(out);
        out.write_arg(b"DIM");
        common.dim.write_redis_args(out);
        out.write_arg(b"DISTANCE_METRIC");
        common.distance_metric.write_redis_args(out);
        self.algorithm.write_options(out);

        if common.base.index_missing {
            out.write_arg(b"INDEXMISSING");
        }
    }
}

impl VectorField {
    /// Create a new FLAT vector field
    pub fn flat(
        vector_type: VectorType,
        dim: u32,
        distance_metric: DistanceMetric,
    ) -> FlatVectorFieldBuilder {
        FlatVectorFieldBuilder::new(VectorFieldCommon {
            base: BaseSchemaField::new(FieldType::Vector),
            vector_type,
            dim,
            distance_metric,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema;
    use crate::search::FtCreateCommand;

    /// The dimension is not checked on the client; the server rejects invalid values.
    #[test]
    fn test_zero_dimension_is_sent_to_server() {
        let schema = schema! {
            "embedding" => VectorField::flat(VectorType::Float32, 0, DistanceMetric::Cosine).build(),
        };
        let ft_create = FtCreateCommand::new("index", schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 0 DISTANCE_METRIC COSINE"
        );
    }
}
