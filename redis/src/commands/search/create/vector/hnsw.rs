//! Defines the options and builder for vector fields using the HNSW indexing algorithm.
use super::{VectorAlgorithm, VectorField, VectorFieldCommon};
use crate::{RedisWrite, ToRedisArgs};

/// Options for vectors using the HNSW indexing algorithm
#[derive(Debug, Clone)]
pub(crate) struct HnswVectorOptions {
    m: Option<u32>,
    ef_construction: Option<u32>,
    ef_runtime: Option<u32>,
    epsilon: Option<f64>,
}

impl ToRedisArgs for HnswVectorOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if let Some(m) = self.m {
            out.write_arg(b"M");
            m.write_redis_args(out);
        }
        if let Some(ef_construction) = self.ef_construction {
            out.write_arg(b"EF_CONSTRUCTION");
            ef_construction.write_redis_args(out);
        }
        if let Some(ef_runtime) = self.ef_runtime {
            out.write_arg(b"EF_RUNTIME");
            ef_runtime.write_redis_args(out);
        }
        if let Some(epsilon) = self.epsilon {
            out.write_arg(b"EPSILON");
            epsilon.write_redis_args(out);
        }
    }

    fn num_of_args(&self) -> usize {
        let mut count = 0;
        if self.m.is_some() {
            count += 2;
        }
        if self.ef_construction.is_some() {
            count += 2;
        }
        if self.ef_runtime.is_some() {
            count += 2;
        }
        if self.epsilon.is_some() {
            count += 2;
        }
        count
    }
}

/// Builder for HNSW vector fields
#[must_use = "The builder has no effect until .build() is called"]
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct HnswVectorFieldBuilder {
    common: VectorFieldCommon,
    m: Option<u32>,
    ef_construction: Option<u32>,
    ef_runtime: Option<u32>,
    epsilon: Option<f64>,
}

impl HnswVectorFieldBuilder {
    pub(super) fn new(common: VectorFieldCommon) -> Self {
        Self {
            common,
            m: None,
            ef_construction: None,
            ef_runtime: None,
            epsilon: None,
        }
    }

    /// Max number of outgoing edges (connections) for each node in a graph layer. On layer zero, the max number of connections will be 2 * M.
    /// Higher values increase accuracy, but also increase memory usage and index build time.
    /// The default is 16.
    pub fn m(mut self, m: u32) -> Self {
        self.m = Some(m);
        self
    }

    /// Max number of connected neighbors to consider during graph building.
    /// Higher values increase accuracy, but also increase index build time.
    /// The default is 200.
    pub fn ef_construction(mut self, ef_construction: u32) -> Self {
        self.ef_construction = Some(ef_construction);
        self
    }

    /// Max top candidates during KNN search. Higher values increase accuracy, but also increase search latency.
    /// The default is 10.
    pub fn ef_runtime(mut self, ef_runtime: u32) -> Self {
        self.ef_runtime = Some(ef_runtime);
        self
    }

    /// Relative factor that sets the boundaries in which a range query may search for candidates.
    /// That is, vector candidates whose distance from the query vector is radius * (1 + EPSILON) are potentially scanned,
    /// allowing more extensive search and more accurate results (on the expense of runtime).
    /// The default is 0.01.
    pub fn epsilon(mut self, epsilon: f64) -> Self {
        self.epsilon = Some(epsilon);
        self
    }

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

    /// Build the vector field.
    pub fn build(self) -> VectorField {
        VectorField {
            common: self.common,
            algorithm: VectorAlgorithm::Hnsw(HnswVectorOptions {
                m: self.m,
                ef_construction: self.ef_construction,
                ef_runtime: self.ef_runtime,
                epsilon: self.epsilon,
            }),
        }
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

    #[test]
    fn test_vector_field_hnsw_algorithm() {
        let schema = schema! {
            VECTOR_FIELD_NAME => VectorField::hnsw(VectorType::Float32, 2, DistanceMetric::L2)
                .m(40)
                .ef_construction(250)
                .ef_runtime(20)
                .build(),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA embedding VECTOR HNSW 12 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2 M 40 EF_CONSTRUCTION 250 EF_RUNTIME 20"
        );
    }

    /// Each option contributes two arguments to the count that precedes the attributes.
    #[test]
    fn test_hnsw_attribute_count_tracks_the_options_set() {
        for (field, expected_count, expected_tail) in [
            (
                VectorField::hnsw(VectorType::Float32, 2, DistanceMetric::L2).build(),
                6,
                "",
            ),
            (
                VectorField::hnsw(VectorType::Float32, 2, DistanceMetric::L2)
                    .epsilon(0.05)
                    .build(),
                8,
                " EPSILON 0.05",
            ),
            (
                VectorField::hnsw(VectorType::Float32, 2, DistanceMetric::L2)
                    .m(16)
                    .ef_construction(200)
                    .ef_runtime(10)
                    .epsilon(0.05)
                    .build(),
                14,
                " M 16 EF_CONSTRUCTION 200 EF_RUNTIME 10 EPSILON 0.05",
            ),
        ] {
            let ft_create = FtCreateCommand::new(
                INDEX_NAME,
                schema! {
                    VECTOR_FIELD_NAME => field,
                },
            );
            assert_eq!(
                ft_create.into_args(),
                format!(
                    "FT.CREATE index SCHEMA embedding VECTOR HNSW {expected_count} TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2{expected_tail}"
                )
            );
        }
    }
}
