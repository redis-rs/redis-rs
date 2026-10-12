//! Defines the options and builder for vector fields using the SVS-VAMANA indexing algorithm.
use super::{VectorAlgorithm, VectorField, VectorFieldCommon, VectorType};
use crate::{RedisWrite, ToRedisArgs};

/// Vector types supported by the VAMANA algorithm.
#[derive(Debug, Copy, Clone)]
#[non_exhaustive]
#[allow(missing_docs)]
pub enum VamanaVectorType {
    Float16,
    Float32,
}

impl From<VamanaVectorType> for VectorType {
    fn from(vt: VamanaVectorType) -> Self {
        match vt {
            VamanaVectorType::Float16 => Self::Float16,
            VamanaVectorType::Float32 => Self::Float32,
        }
    }
}

/// Compression algorithm for VAMANA vector indexes.
/// <https://redis.io/docs/latest/develop/ai/search-and-query/vectors/svs-compression/>
#[derive(Debug, Copy, Clone)]
#[non_exhaustive]
#[allow(missing_docs)]
pub enum CompressionType {
    LVQ8,
    LVQ4,
    LVQ4x4,
    LVQ4x8,
    LeanVec4x8,
    LeanVec8x8,
}

impl ToRedisArgs for CompressionType {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(match self {
            Self::LVQ8 => b"LVQ8",
            Self::LVQ4 => b"LVQ4",
            Self::LVQ4x4 => b"LVQ4x4",
            Self::LVQ4x8 => b"LVQ4x8",
            Self::LeanVec4x8 => b"LeanVec4x8",
            Self::LeanVec8x8 => b"LeanVec8x8",
        });
    }
}

/// Options for vectors using the VAMANA indexing algorithm
#[derive(Debug, Clone)]
pub(crate) struct VamanaVectorOptions {
    compression: Option<CompressionType>,
    construction_window_size: Option<u32>,
    graph_max_degree: Option<u32>,
    search_window_size: Option<u32>,
    epsilon: Option<f64>,
    training_threshold: Option<u32>,
    reduce: Option<u32>,
}

impl ToRedisArgs for VamanaVectorOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if let Some(compression) = &self.compression {
            out.write_arg(b"COMPRESSION");
            compression.write_redis_args(out);
        }
        if let Some(construction_window_size) = self.construction_window_size {
            out.write_arg(b"CONSTRUCTION_WINDOW_SIZE");
            construction_window_size.write_redis_args(out);
        }
        if let Some(graph_max_degree) = self.graph_max_degree {
            out.write_arg(b"GRAPH_MAX_DEGREE");
            graph_max_degree.write_redis_args(out);
        }
        if let Some(search_window_size) = self.search_window_size {
            out.write_arg(b"SEARCH_WINDOW_SIZE");
            search_window_size.write_redis_args(out);
        }
        if let Some(epsilon) = self.epsilon {
            out.write_arg(b"EPSILON");
            epsilon.write_redis_args(out);
        }
        if let Some(training_threshold) = self.training_threshold {
            out.write_arg(b"TRAINING_THRESHOLD");
            training_threshold.write_redis_args(out);
        }
        if let Some(reduce) = self.reduce {
            out.write_arg(b"REDUCE");
            reduce.write_redis_args(out);
        }
    }

    fn num_of_args(&self) -> usize {
        let mut count = 0;
        if self.compression.is_some() {
            count += 2;
        }
        if self.construction_window_size.is_some() {
            count += 2;
        }
        if self.graph_max_degree.is_some() {
            count += 2;
        }
        if self.search_window_size.is_some() {
            count += 2;
        }
        if self.epsilon.is_some() {
            count += 2;
        }
        if self.training_threshold.is_some() {
            count += 2;
        }
        if self.reduce.is_some() {
            count += 2;
        }
        count
    }
}

/// Builder for VAMANA vector fields
#[must_use = "The builder has no effect until .build() is called"]
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct VamanaVectorFieldBuilder {
    common: VectorFieldCommon,
    compression: Option<CompressionType>,
    construction_window_size: Option<u32>,
    graph_max_degree: Option<u32>,
    search_window_size: Option<u32>,
    epsilon: Option<f64>,
    training_threshold: Option<u32>,
    reduce: Option<u32>,
}

impl VamanaVectorFieldBuilder {
    pub(super) fn new(common: VectorFieldCommon) -> Self {
        Self {
            common,
            compression: None,
            construction_window_size: None,
            graph_max_degree: None,
            search_window_size: None,
            epsilon: None,
            training_threshold: None,
            reduce: None,
        }
    }

    /// Set the compression algorithm for the VAMANA index.
    ///
    /// Compression reduces memory usage at the cost of some accuracy.
    pub fn compression(mut self, compression: CompressionType) -> Self {
        self.compression = Some(compression);
        self
    }

    /// The search window size to use during graph construction.
    /// A higher search window size will yield a higher quality graph since more overall vertexes are considered, but will increase construction time.
    /// The default is 200.
    pub fn construction_window_size(mut self, construction_window_size: u32) -> Self {
        self.construction_window_size = Some(construction_window_size);
        self
    }

    /// Sets the maximum number of edges per node; equivalent to HNSW’s M*2.
    /// A higher max degree may yield a higher quality graph in terms of recall for performance, but the memory footprint of the graph is directly proportional to the maximum degree.
    /// The default is 32.
    pub fn graph_max_degree(mut self, graph_max_degree: u32) -> Self {
        self.graph_max_degree = Some(graph_max_degree);
        self
    }

    /// The size of the search window; the same as HSNW's EF_RUNTIME (Max top candidates during KNN search).
    /// Increasing the search window size and capacity generally yields more accurate but slower search results.
    /// The default is 10.
    pub fn search_window_size(mut self, search_window_size: u32) -> Self {
        self.search_window_size = Some(search_window_size);
        self
    }

    /// The range search approximation factor; the same as HSNW's EPSILON.
    /// The default is 0.01.
    pub fn epsilon(mut self, epsilon: f64) -> Self {
        self.epsilon = Some(epsilon);
        self
    }

    /// Number of vectors needed to learn compression parameters.
    /// Increase if recall is low. Note: setting this too high may slow down search.
    /// The default is 10240.
    ///
    /// Applies only together with [`compression`](Self::compression).
    /// The server rejects the index otherwise.
    pub fn training_threshold(mut self, training_threshold: u32) -> Self {
        self.training_threshold = Some(training_threshold);
        self
    }

    /// The dimension used when using LeanVec4x8 or LeanVec8x8 compression for dimensionality reduction.
    /// It should be less than DIM. Lowering it can speed up search and reduce memory use.
    /// The default is DIM / 2.
    ///
    /// Applies only together with [`CompressionType::LeanVec4x8`] or [`CompressionType::LeanVec8x8`].
    /// The server rejects the index otherwise.
    pub fn reduce(mut self, reduce: u32) -> Self {
        self.reduce = Some(reduce);
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
            algorithm: VectorAlgorithm::Vamana(VamanaVectorOptions {
                compression: self.compression,
                construction_window_size: self.construction_window_size,
                graph_max_degree: self.graph_max_degree,
                search_window_size: self.search_window_size,
                epsilon: self.epsilon,
                training_threshold: self.training_threshold,
                reduce: self.reduce,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::DistanceMetric;
    use super::*;
    use crate::schema;
    use crate::search::FtCreateCommand;

    static INDEX_NAME: &str = "index";
    static VECTOR_FIELD_NAME: &str = "embedding";

    #[test]
    fn test_vector_field_vamana_algorithm() {
        let ft_create = FtCreateCommand::new(
            INDEX_NAME,
            schema! {
                VECTOR_FIELD_NAME => VectorField::vamana(VamanaVectorType::Float32, 1024, DistanceMetric::Cosine)
                    .compression(CompressionType::LeanVec4x8)
                    .construction_window_size(300)
                    .graph_max_degree(128)
                    .search_window_size(20)
                    .epsilon(0.02)
                    .training_threshold(2048)
                    .reduce(512)
                    .build(),
            },
        );
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA embedding VECTOR SVS-VAMANA 20 TYPE FLOAT32 DIM 1024 DISTANCE_METRIC COSINE COMPRESSION LeanVec4x8 CONSTRUCTION_WINDOW_SIZE 300 GRAPH_MAX_DEGREE 128 SEARCH_WINDOW_SIZE 20 EPSILON 0.02 TRAINING_THRESHOLD 2048 REDUCE 512"
        );
    }

    /// The builder sends the values as given and leaves validation to the server,
    /// even for combinations that the server rejects.
    #[test]
    fn test_conditional_parameters_are_sent_as_given() {
        let ft_create = FtCreateCommand::new(
            INDEX_NAME,
            schema! {
                VECTOR_FIELD_NAME => VectorField::vamana(VamanaVectorType::Float32, 1024, DistanceMetric::Cosine)
                    .training_threshold(200_000)
                    .reduce(0)
                    .build(),
            },
        );
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA embedding VECTOR SVS-VAMANA 10 TYPE FLOAT32 DIM 1024 DISTANCE_METRIC COSINE TRAINING_THRESHOLD 200000 REDUCE 0"
        );
    }

    /// `compression` can be set before or after the parameters that depend on it.
    #[test]
    fn test_conditional_parameters_do_not_depend_on_call_order() {
        let compression_first =
            VectorField::vamana(VamanaVectorType::Float32, 1024, DistanceMetric::Cosine)
                .compression(CompressionType::LeanVec8x8)
                .training_threshold(2048)
                .reduce(256)
                .build();
        let compression_last =
            VectorField::vamana(VamanaVectorType::Float32, 1024, DistanceMetric::Cosine)
                .training_threshold(2048)
                .reduce(256)
                .compression(CompressionType::LeanVec8x8)
                .build();

        let rendered = |field| {
            FtCreateCommand::new(INDEX_NAME, schema! { VECTOR_FIELD_NAME => field }).into_args()
        };
        let expected = "FT.CREATE index SCHEMA embedding VECTOR SVS-VAMANA 12 TYPE FLOAT32 DIM 1024 DISTANCE_METRIC COSINE COMPRESSION LeanVec8x8 TRAINING_THRESHOLD 2048 REDUCE 256";
        assert_eq!(rendered(compression_first), expected);
        assert_eq!(rendered(compression_last), expected);
    }

    /// Every compression type must serialize to the token FT.CREATE expects.
    #[test]
    fn test_compression_types() {
        for (compression, expected) in [
            (CompressionType::LVQ8, "LVQ8"),
            (CompressionType::LVQ4, "LVQ4"),
            (CompressionType::LVQ4x4, "LVQ4x4"),
            (CompressionType::LVQ4x8, "LVQ4x8"),
            (CompressionType::LeanVec4x8, "LeanVec4x8"),
            (CompressionType::LeanVec8x8, "LeanVec8x8"),
        ] {
            let ft_create = FtCreateCommand::new(
                INDEX_NAME,
                schema! {
                    VECTOR_FIELD_NAME => VectorField::vamana(VamanaVectorType::Float32, 8, DistanceMetric::L2)
                        .compression(compression)
                        .build(),
                },
            );
            assert_eq!(
                ft_create.into_args(),
                format!(
                    "FT.CREATE index SCHEMA embedding VECTOR SVS-VAMANA 8 TYPE FLOAT32 DIM 8 DISTANCE_METRIC L2 COMPRESSION {expected}"
                )
            );
        }
    }

    /// VAMANA accepts only FLOAT16 and FLOAT32, which the dedicated type enforces.
    #[test]
    fn test_vamana_vector_types() {
        for (vector_type, expected) in [
            (VamanaVectorType::Float16, "FLOAT16"),
            (VamanaVectorType::Float32, "FLOAT32"),
        ] {
            let ft_create = FtCreateCommand::new(
                INDEX_NAME,
                schema! {
                    VECTOR_FIELD_NAME => VectorField::vamana(vector_type, 8, DistanceMetric::L2).build(),
                },
            );
            assert_eq!(
                ft_create.into_args(),
                format!(
                    "FT.CREATE index SCHEMA embedding VECTOR SVS-VAMANA 6 TYPE {expected} DIM 8 DISTANCE_METRIC L2"
                )
            );
        }
    }
}
