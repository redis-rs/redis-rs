//! Defines the vector field types used with the FT.CREATE command.
//!
//! A vector field is written as `VECTOR <algorithm> <attribute_count> <attributes...>`, where the
//! count covers the shared attributes (`TYPE`, `DIM`, `DISTANCE_METRIC`) plus whichever
//! algorithm-specific ones were set. Each algorithm's options and builder therefore live in their
//! own module and report their argument count through `ToRedisArgs::num_of_args`, which
//! [`VectorField`] sums to produce the count written on the wire.
use super::fields::{BaseSchemaField, FieldType};
use crate::{RedisWrite, ToRedisArgs};

mod flat;

pub use flat::*;

#[derive(Debug, Clone)]
#[non_exhaustive]
pub(crate) enum VectorAlgorithm {
    Flat,
}

impl ToRedisArgs for VectorAlgorithm {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(match self {
            Self::Flat => b"FLAT",
        });
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

/// Represents a vector field in the schema.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SchemaVectorField {
    pub(crate) base: BaseSchemaField,
    algorithm: VectorAlgorithm,
    vector_type: VectorType,
    dim: u32,
    distance_metric: DistanceMetric,
}

impl ToRedisArgs for SchemaVectorField {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if let Some(alias) = &self.base.alias {
            out.write_arg(b"AS");
            alias.write_redis_args(out);
        }

        self.base.field_type.write_redis_args(out);

        self.algorithm.write_redis_args(out);
        // Note: The attribute count will be written by the VectorField implementation
        // which knows about both base and algorithm-specific attributes
        // That is:
        /*
            out.write_arg(b"TYPE");
            self.vector_type.write_redis_args(out);
            out.write_arg(b"DIM");
            self.dim.write_redis_args(out);
            out.write_arg(b"DISTANCE_METRIC");
            self.distance_metric.write_redis_args(out);
        */
    }

    fn num_of_args(&self) -> usize {
        // Count the number of attribute pairs (key-value pairs) for this vector field.
        // Base attributes are: TYPE, DIM, DISTANCE_METRIC (3 pairs = 6 args)
        6
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
pub enum VectorField {
    /// FLAT (brute-force) vector index for exact nearest neighbor search.
    /// Best for small datasets (< 1M vectors) where perfect accuracy is required.
    Flat(SchemaVectorField, FlatVectorOptions),
}

impl VectorField {
    /// Set the alias for the field.
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        match self {
            Self::Flat(ref mut base, _) => base.base = base.base.clone().alias(alias),
        }
        self
    }

    /// Set index missing. This allows searching for missing values - documents that do not contain a specific field.
    pub fn index_missing(mut self, index_missing: bool) -> Self {
        match self {
            Self::Flat(ref mut base, _) => {
                base.base = base.base.clone().index_missing(index_missing);
            }
        }
        self
    }
}

impl ToRedisArgs for VectorField {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        let base = match self {
            Self::Flat(base, _) => base,
        };
        base.write_redis_args(out);

        let attributes_count = match self {
            Self::Flat(base, flat_vector_options) => {
                base.num_of_args() + flat_vector_options.num_of_args()
            }
        };
        attributes_count.write_redis_args(out);

        out.write_arg(b"TYPE");
        base.vector_type.write_redis_args(out);
        out.write_arg(b"DIM");
        base.dim.write_redis_args(out);
        out.write_arg(b"DISTANCE_METRIC");
        base.distance_metric.write_redis_args(out);

        // Write algorithm-specific attributes
        match self {
            Self::Flat(_, flat_vector_options) => {
                flat_vector_options.write_redis_args(out);
            }
        }

        if base.base.index_missing {
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
        assert!(
            dim > 0,
            "Vector dimension must be positive (greater than 0)"
        );

        FlatVectorFieldBuilder::new(SchemaVectorField {
            base: BaseSchemaField::new(FieldType::Vector),
            algorithm: VectorAlgorithm::Flat,
            vector_type,
            dim,
            distance_metric,
        })
    }
}
