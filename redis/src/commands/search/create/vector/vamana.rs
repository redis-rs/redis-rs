//! Defines the options and builder for vector fields using the SVS-VAMANA indexing algorithm.
use super::{VectorAlgorithm, VectorField, VectorFieldCommon, VectorType};
use crate::{RedisWrite, ToRedisArgs};
use log::warn;

const DEFAULT_BLOCK_SIZE: u32 = 1024;
/// Maximum value for `training_threshold` parameter (102,400)
pub const MAX_TRAINING_THRESHOLD: u32 = 100 * DEFAULT_BLOCK_SIZE;

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
///
/// Note: Intel's proprietary LVQ and LeanVec optimizations are not available in Redis Open Source.
/// On non-Intel platforms, these will fall back to basic 8-bit scalar quantization.
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
    ///
    /// Note: Intel's proprietary LVQ and LeanVec optimizations are not available in Redis Open Source.
    /// On non-Intel platforms, this will fall back to basic 8-bit scalar quantization.
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
    /// Applicable only when used with COMPRESSION. Increase if recall is low.
    /// Note: setting this too high may slow down search. If a value is provided, it must be less than 100 * DEFAULT_BLOCK_SIZE, where DEFAULT_BLOCK_SIZE is 1024.
    /// The default is 10 * DEFAULT_BLOCK_SIZE.
    pub fn training_threshold(mut self, training_threshold: u32) -> Self {
        if self.compression.is_some() {
            let clamped = std::cmp::min(training_threshold, MAX_TRAINING_THRESHOLD);
            if clamped != training_threshold {
                warn!(
                    "training_threshold exceeded the maximum allowed value; clamped from {training_threshold} to {clamped}."
                );
            }
            self.training_threshold = Some(clamped);
        } else {
            warn!("training_threshold ignored: applies only when compression is enabled.");
        }
        self
    }

    /// The dimension used when using LeanVec4x8 or LeanVec8x8 compression for dimensionality reduction.
    /// If a value is provided, it should be less than DIM. Lowering it can speed up search and reduce memory use.
    /// The default is DIM / 2.
    pub fn reduce(mut self, reduce: u32) -> Self {
        if self
            .compression
            .is_some_and(|c| matches!(c, CompressionType::LeanVec4x8 | CompressionType::LeanVec8x8))
        {
            let max_reduce = self.common.dim.saturating_sub(1).max(1);
            let clamped = std::cmp::min(reduce, max_reduce).max(1);
            if clamped != reduce {
                warn!(
                    "reduce value {reduce} out of valid range 1..={max_reduce}; clamped to {clamped}."
                );
            }
            self.reduce = Some(clamped);
        } else {
            warn!("reduce ignored: applies only to LeanVec4x8 and LeanVec8x8 compression types.");
        }
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
        let reduce = 512;
        let vamana_field_builder =
            VectorField::vamana(VamanaVectorType::Float32, 1024, DistanceMetric::Cosine)
                .compression(CompressionType::LVQ8)
                .construction_window_size(300)
                .graph_max_degree(128)
                .search_window_size(20)
                .epsilon(0.02)
                .training_threshold(2048)
                .reduce(reduce); // Note: reduce is only applied for LeanVec4x8 and LeanVec8x8 compression
        let ft_create = FtCreateCommand::new(
            INDEX_NAME,
            schema! {
                VECTOR_FIELD_NAME => vamana_field_builder.clone().build(),
            },
        );
        // Note: REDUCE should not be included because it only applies to LeanVec4x8 and LeanVec8x8 compression types.
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA embedding VECTOR SVS-VAMANA 18 TYPE FLOAT32 DIM 1024 DISTANCE_METRIC COSINE COMPRESSION LVQ8 CONSTRUCTION_WINDOW_SIZE 300 GRAPH_MAX_DEGREE 128 SEARCH_WINDOW_SIZE 20 EPSILON 0.02 TRAINING_THRESHOLD 2048"
        );
        // Test that LeanVec4x8 compression includes REDUCE.
        let vamana_field_builder = vamana_field_builder
            .compression(CompressionType::LeanVec4x8)
            .reduce(reduce);
        let ft_create = FtCreateCommand::new(
            INDEX_NAME,
            schema! {
                VECTOR_FIELD_NAME => vamana_field_builder.clone().build(),
            },
        );
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA embedding VECTOR SVS-VAMANA 20 TYPE FLOAT32 DIM 1024 DISTANCE_METRIC COSINE COMPRESSION LeanVec4x8 CONSTRUCTION_WINDOW_SIZE 300 GRAPH_MAX_DEGREE 128 SEARCH_WINDOW_SIZE 20 EPSILON 0.02 TRAINING_THRESHOLD 2048 REDUCE 512"
        );
        // Test that bigger reduce parameters are clamped to dim - 1.
        let vamana_field_builder = vamana_field_builder.reduce(1024);
        let ft_create = FtCreateCommand::new(
            INDEX_NAME,
            schema! {
                VECTOR_FIELD_NAME => vamana_field_builder.clone().build(),
            },
        );
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA embedding VECTOR SVS-VAMANA 20 TYPE FLOAT32 DIM 1024 DISTANCE_METRIC COSINE COMPRESSION LeanVec4x8 CONSTRUCTION_WINDOW_SIZE 300 GRAPH_MAX_DEGREE 128 SEARCH_WINDOW_SIZE 20 EPSILON 0.02 TRAINING_THRESHOLD 2048 REDUCE 1023"
        );
        // Test that the minimal reduction is 1.
        let vamana_field_builder = vamana_field_builder.reduce(0);
        let ft_create = FtCreateCommand::new(
            INDEX_NAME,
            schema! {
                VECTOR_FIELD_NAME => vamana_field_builder.clone().build(),
            },
        );
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA embedding VECTOR SVS-VAMANA 20 TYPE FLOAT32 DIM 1024 DISTANCE_METRIC COSINE COMPRESSION LeanVec4x8 CONSTRUCTION_WINDOW_SIZE 300 GRAPH_MAX_DEGREE 128 SEARCH_WINDOW_SIZE 20 EPSILON 0.02 TRAINING_THRESHOLD 2048 REDUCE 1"
        );
        // Test that training threshold is clamped to 100 * DEFAULT_BLOCK_SIZE, where DEFAULT_BLOCK_SIZE is 1024.
        let vamana_field_builder =
            vamana_field_builder.training_threshold(MAX_TRAINING_THRESHOLD + 1);
        let ft_create = FtCreateCommand::new(
            INDEX_NAME,
            schema! {
                VECTOR_FIELD_NAME => vamana_field_builder.build(),
            },
        );
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA embedding VECTOR SVS-VAMANA 20 TYPE FLOAT32 DIM 1024 DISTANCE_METRIC COSINE COMPRESSION LeanVec4x8 CONSTRUCTION_WINDOW_SIZE 300 GRAPH_MAX_DEGREE 128 SEARCH_WINDOW_SIZE 20 EPSILON 0.02 TRAINING_THRESHOLD 102400 REDUCE 1"
        );
        // Test that training threshold is only applicable when there is a compression type.
        let vamana_field_builder =
            VectorField::vamana(VamanaVectorType::Float32, 1024, DistanceMetric::Cosine)
                .training_threshold(2048);
        let ft_create = FtCreateCommand::new(
            INDEX_NAME,
            schema! {
                VECTOR_FIELD_NAME => vamana_field_builder.build(),
            },
        );
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA embedding VECTOR SVS-VAMANA 6 TYPE FLOAT32 DIM 1024 DISTANCE_METRIC COSINE"
        );
    }

    /// The conditional parameters are resolved when they are set, so `compression` has to come
    /// first. Setting it afterwards leaves the earlier value dropped.
    #[test]
    fn test_conditional_parameters_depend_on_call_order() {
        let after = VectorField::vamana(VamanaVectorType::Float32, 1024, DistanceMetric::Cosine)
            .compression(CompressionType::LVQ8)
            .training_threshold(2048)
            .build();
        let before = VectorField::vamana(VamanaVectorType::Float32, 1024, DistanceMetric::Cosine)
            .training_threshold(2048)
            .compression(CompressionType::LVQ8)
            .build();

        let rendered = |field| {
            FtCreateCommand::new(INDEX_NAME, schema! { VECTOR_FIELD_NAME => field }).into_args()
        };
        assert_eq!(
            rendered(after),
            "FT.CREATE index SCHEMA embedding VECTOR SVS-VAMANA 10 TYPE FLOAT32 DIM 1024 DISTANCE_METRIC COSINE COMPRESSION LVQ8 TRAINING_THRESHOLD 2048"
        );
        assert_eq!(
            rendered(before),
            "FT.CREATE index SCHEMA embedding VECTOR SVS-VAMANA 8 TYPE FLOAT32 DIM 1024 DISTANCE_METRIC COSINE COMPRESSION LVQ8"
        );
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
