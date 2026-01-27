#![cfg(feature = "search")]

#[path = "../support/mod.rs"]
mod support;
use crate::support::*;
use redis::Commands;
use redis::schema;
use redis::search::*;

static TEXT_FIELD_NAME: &str = "title";
static NUMERIC_FIELD_NAME: &str = "price";
static TAG_FIELD_NAME: &str = "condition";
static GEO_FIELD_NAME: &str = "location";
static VECTOR_FIELD_NAME: &str = "embedding";
static GEOSHAPE_FIELD_NAME: &str = "area";

#[test]
fn test_ft_create_with_an_empty_index_name() {
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = ctx.connection();
    let schema = schema! {
        TEXT_FIELD_NAME => SchemaTextField::new()
    };
    assert_eq!(
        con.ft_create("", &CreateOptions::new(), &schema),
        Ok("OK".to_string())
    );
}

#[test]
fn test_simple_ft_create() {
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = ctx.connection();
    let index_name = "index";
    let schema = schema! {
        TEXT_FIELD_NAME => SchemaTextField::new()
    };
    let options = CreateOptions::new();
    // Check that the first call succeeds but the second one fails because the index already exists
    assert_eq!(
        con.ft_create(index_name, &options, &schema),
        Ok("OK".to_string())
    );
    assert!(
        con.ft_create::<_, String>(index_name, &options, &schema)
            .is_err()
    );
}

#[test]
fn test_ft_create_create_options() {
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = ctx.connection();
    let schema = schema! {
        TEXT_FIELD_NAME => SchemaTextField::new()
    };

    type CreateOptionsModifier = fn(CreateOptions) -> CreateOptions;
    let option_modifiers: Vec<(&'static str, CreateOptionsModifier)> = vec![
        ("on_hash", |opts| opts.on(IndexDataType::Hash)),
        ("single_prefix", |opts| opts.prefix("pref1")),
        ("multiple_prefixes", |opts| {
            opts.prefix("pref2").prefix("pref3")
        }),
        ("filter", |opts| opts.filter("@field: value")),
        ("language", |opts| opts.language(SearchLanguage::English)),
        ("language_field", |opts| {
            opts.language_field("language_field")
        }),
        ("score", |opts| opts.score(1.0)),
        ("score_field", |opts| opts.score_field("score_field")),
        ("max_text_fields", |opts| opts.max_text_fields()),
        ("no_offsets", |opts| opts.no_offsets()),
        ("temporary", |opts| opts.temporary(1)),
        ("no_highlight", |opts| opts.no_highlight()),
        ("no_fields", |opts| opts.no_fields()),
        ("no_freqs", |opts| opts.no_freqs()),
        ("single_stopword", |opts| opts.stopword("stopword1")),
        ("multiple_stopwords", |opts| {
            opts.stopword("stopword2").stopword("stopword3")
        }),
        ("skip_initial_scan", |opts| opts.skip_initial_scan()),
    ];

    // Test each option individually
    for (suffix, modifier) in &option_modifiers {
        let index_name = format!("index_with_{}", suffix);
        let options = modifier(CreateOptions::new());

        assert_eq!(
            con.ft_create(&index_name, &options, &schema),
            Ok("OK".to_string())
        );
    }

    // Test all options combined
    let mut combined_options = CreateOptions::new();
    for (suffix, modifier) in &option_modifiers {
        let combined_index_name = format!("combined_index_until_{}", suffix);
        combined_options = modifier(combined_options);

        assert_eq!(
            con.ft_create(&combined_index_name, &combined_options, &schema),
            Ok("OK".to_string())
        );
    }
}

#[test]
fn test_ft_create_schema_text_field() {
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = ctx.connection();

    type SchemaTextFieldModifier = fn(SchemaTextField) -> SchemaTextField;
    let field_modifiers: Vec<(&'static str, SchemaTextFieldModifier)> = vec![
        // Common modifiers
        ("alias", |field| field.alias("text_alias")),
        ("sortable", |field| field.sortable(Sortable::Yes)),
        ("sortable_unf", |field| field.sortable(Sortable::Unf)),
        // Text field modifiers
        ("no_stem", |field| field.no_stem(true)),
        ("weight", |field| field.weight(1.0)),
        ("phonetic", |field| field.phonetic(Phonetic::DmEn)),
        ("with_suffix_trie", |field| field.with_suffix_trie(true)),
        ("index_empty", |field| field.index_empty(true)),
    ];

    // Common modifiers that are mutually exclusive
    let mutually_exclusive_common_modifiers: Vec<(&'static str, SchemaTextFieldModifier)> = vec![
        ("index_missing", |field| field.index_missing(true)),
        ("no_index", |field| field.no_index(true)),
    ];

    // Test each common field modifier individually
    for (suffix, modifier) in &field_modifiers {
        let index_name = format!("index_for_text_field_with_{}", suffix);
        let schema = schema! {
            TEXT_FIELD_NAME => modifier(SchemaTextField::new())
        };
        assert_eq!(
            con.ft_create(&index_name, &CreateOptions::new(), &schema),
            Ok("OK".to_string())
        );
    }

    // Test each mutually exclusive modifier individually
    for (suffix, modifier) in &mutually_exclusive_common_modifiers {
        let index_name = format!("index_for_text_field_with_{}", suffix);
        let schema = schema! {
            TEXT_FIELD_NAME => modifier(SchemaTextField::new())
        };
        assert_eq!(
            con.ft_create(&index_name, &CreateOptions::new(), &schema),
            Ok("OK".to_string())
        );
    }

    // Test all combinations of field modifiers that are not mutually exclusive
    let mut combined_schema_text_field = SchemaTextField::new();
    for (suffix, modifier) in &field_modifiers {
        let combined_index_name = format!("index_for_text_field_combined_until_{}", suffix);
        combined_schema_text_field = modifier(combined_schema_text_field);
        let schema = schema! {
            TEXT_FIELD_NAME => combined_schema_text_field.clone()
        };
        assert_eq!(
            con.ft_create(&combined_index_name, &CreateOptions::new(), &schema),
            Ok("OK".to_string())
        );
    }

    // After all of the modifiers above have been applied, add each of the mutually exclusive modifiers individually
    for (suffix, modifier) in &mutually_exclusive_common_modifiers {
        let combined_index_name = format!("index_for_text_field_all_combined_with_{}", suffix);
        let schema = schema! {
            TEXT_FIELD_NAME =>  modifier(combined_schema_text_field.clone())
        };
        assert_eq!(
            con.ft_create(&combined_index_name, &CreateOptions::new(), &schema),
            Ok("OK".to_string())
        );
    }
    // Test that mutually exclusive modifiers are mutually exclusive indeed
    assert!(
        con.ft_create::<_, String>(
            "invalid_index",
            &CreateOptions::new(),
            &schema! {
                TEXT_FIELD_NAME => SchemaTextField::new().no_index(true).index_missing(true)
            }
        )
        .is_err()
    );
}

#[test]
fn test_ft_create_schema_tag_field() {
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = ctx.connection();

    type SchemaTagFieldModifier = fn(SchemaTagField) -> SchemaTagField;
    let field_modifiers: Vec<(&'static str, SchemaTagFieldModifier)> = vec![
        // Common modifiers
        ("alias", |field| field.alias("tag_alias")),
        ("sortable", |field| field.sortable(Sortable::Yes)),
        ("sortable_unf", |field| field.sortable(Sortable::Unf)),
        // Tag field modifiers
        ("separator", |field| field.separator(',')),
        ("case_sensitive", |field| field.case_sensitive(true)),
        ("with_suffix_trie", |field| field.with_suffix_trie(true)),
        ("index_empty", |field| field.index_empty(true)),
    ];

    // Common modifiers that are mutually exclusive
    let mutually_exclusive_common_modifiers: Vec<(&'static str, SchemaTagFieldModifier)> = vec![
        ("index_missing", |field| field.index_missing(true)),
        ("no_index", |field| field.no_index(true)),
    ];

    // Test each common field modifier individually
    for (suffix, modifier) in &field_modifiers {
        let index_name = format!("index_for_tag_field_with_{}", suffix);
        let schema = schema! {
            TAG_FIELD_NAME => modifier(SchemaTagField::new())
        };
        assert_eq!(
            con.ft_create(&index_name, &CreateOptions::new(), &schema),
            Ok("OK".to_string())
        );
    }

    // Test each mutually exclusive modifier individually
    for (suffix, modifier) in &mutually_exclusive_common_modifiers {
        let index_name = format!("index_for_tag_field_with_{}", suffix);
        let schema = schema! {
            TAG_FIELD_NAME => modifier(SchemaTagField::new())
        };
        assert_eq!(
            con.ft_create(&index_name, &CreateOptions::new(), &schema),
            Ok("OK".to_string())
        );
    }

    // Test all combinations of field modifiers that are not mutually exclusive
    let mut combined_schema_tag_field = SchemaTagField::new();
    for (suffix, modifier) in &field_modifiers {
        let combined_index_name = format!("index_for_tag_field_combined_until_{}", suffix);
        combined_schema_tag_field = modifier(combined_schema_tag_field);
        let schema = schema! {
            TAG_FIELD_NAME => combined_schema_tag_field.clone()
        };
        assert_eq!(
            con.ft_create(&combined_index_name, &CreateOptions::new(), &schema),
            Ok("OK".to_string())
        );
    }

    // After all of the modifiers above have been applied, add each of the mutually exclusive modifiers individually
    for (suffix, modifier) in &mutually_exclusive_common_modifiers {
        let combined_index_name = format!("index_for_tag_field_all_combined_with_{}", suffix);
        let schema = schema! {
            TAG_FIELD_NAME =>  modifier(combined_schema_tag_field.clone())
        };
        assert_eq!(
            con.ft_create(&combined_index_name, &CreateOptions::new(), &schema),
            Ok("OK".to_string())
        );
    }
    // Test that mutually exclusive modifiers are mutually exclusive indeed
    assert!(
        con.ft_create::<_, String>(
            "invalid_index",
            &CreateOptions::new(),
            &schema! {
                TAG_FIELD_NAME => SchemaTagField::new().no_index(true).index_missing(true)
            }
        )
        .is_err()
    );
}

#[test]
fn test_ft_create_schema_numeric_field() {
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = ctx.connection();

    type SchemaNumericFieldModifier = fn(SchemaNumericField) -> SchemaNumericField;
    let field_modifiers: Vec<(&'static str, SchemaNumericFieldModifier)> = vec![
        // Common modifiers
        ("alias", |field| field.alias("numeric_alias")),
        ("sortable", |field| field.sortable(Sortable::Yes)),
        ("sortable_unf", |field| field.sortable(Sortable::Unf)),
    ];

    // Common modifiers that are mutually exclusive
    let mutually_exclusive_common_modifiers: Vec<(&'static str, SchemaNumericFieldModifier)> = vec![
        ("index_missing", |field| field.index_missing(true)),
        ("no_index", |field| field.no_index(true)),
    ];

    // Test each common field modifier individually
    for (suffix, modifier) in &field_modifiers {
        let index_name = format!("index_for_numeric_field_with_{}", suffix);
        let schema = schema! {
            NUMERIC_FIELD_NAME => modifier(SchemaNumericField::new())
        };
        assert_eq!(
            con.ft_create(&index_name, &CreateOptions::new(), &schema),
            Ok("OK".to_string())
        );
    }

    // Test each mutually exclusive modifier individually
    for (suffix, modifier) in &mutually_exclusive_common_modifiers {
        let index_name = format!("index_for_numeric_field_with_{}", suffix);
        let schema = schema! {
            NUMERIC_FIELD_NAME => modifier(SchemaNumericField::new())
        };
        assert_eq!(
            con.ft_create(&index_name, &CreateOptions::new(), &schema),
            Ok("OK".to_string())
        );
    }

    // Test all combinations of field modifiers that are not mutually exclusive
    let mut combined_schema_numeric_field = SchemaNumericField::new();
    for (suffix, modifier) in &field_modifiers {
        let combined_index_name = format!("index_for_numeric_field_combined_until_{}", suffix);
        combined_schema_numeric_field = modifier(combined_schema_numeric_field);
        let schema = schema! {
            NUMERIC_FIELD_NAME => combined_schema_numeric_field.clone()
        };
        assert_eq!(
            con.ft_create(&combined_index_name, &CreateOptions::new(), &schema),
            Ok("OK".to_string())
        );
    }

    // After all of the modifiers above have been applied, add each of the mutually exclusive modifiers individually
    for (suffix, modifier) in &mutually_exclusive_common_modifiers {
        let combined_index_name = format!("index_for_numeric_field_all_combined_with_{}", suffix);
        let schema = schema! {
            NUMERIC_FIELD_NAME =>  modifier(combined_schema_numeric_field.clone())
        };
        assert_eq!(
            con.ft_create(&combined_index_name, &CreateOptions::new(), &schema),
            Ok("OK".to_string())
        );
    }
    // Test that mutually exclusive modifiers are mutually exclusive indeed
    assert!(
        con.ft_create::<_, String>(
            "invalid_index",
            &CreateOptions::new(),
            &schema! {
                NUMERIC_FIELD_NAME => SchemaNumericField::new().no_index(true).index_missing(true)
            }
        )
        .is_err()
    );
}

#[test]
fn test_ft_create_schema_geo_field() {
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = ctx.connection();

    type SchemaGeoFieldModifier = fn(SchemaGeoField) -> SchemaGeoField;
    let field_modifiers: Vec<(&'static str, SchemaGeoFieldModifier)> = vec![
        // Common modifiers
        ("alias", |field| field.alias("geo_alias")),
        ("sortable", |field| field.sortable(Sortable::Yes)),
        ("sortable_unf", |field| field.sortable(Sortable::Unf)),
    ];

    // Common modifiers that are mutually exclusive
    let mutually_exclusive_common_modifiers: Vec<(&'static str, SchemaGeoFieldModifier)> = vec![
        ("index_missing", |field| field.index_missing(true)),
        ("no_index", |field| field.no_index(true)),
    ];

    // Test each common field modifier individually
    for (suffix, modifier) in &field_modifiers {
        let index_name = format!("index_for_geo_field_with_{}", suffix);
        let schema = schema! {
            GEO_FIELD_NAME => modifier(SchemaGeoField::new())
        };
        assert_eq!(
            con.ft_create(&index_name, &CreateOptions::new(), &schema),
            Ok("OK".to_string())
        );
    }

    // Test each mutually exclusive modifier individually
    for (suffix, modifier) in &mutually_exclusive_common_modifiers {
        let index_name = format!("index_for_geo_field_with_{}", suffix);
        let schema = schema! {
            GEO_FIELD_NAME => modifier(SchemaGeoField::new())
        };
        assert_eq!(
            con.ft_create(&index_name, &CreateOptions::new(), &schema),
            Ok("OK".to_string())
        );
    }

    // Test all combinations of field modifiers that are not mutually exclusive
    let mut combined_schema_geo_field = SchemaGeoField::new();
    for (suffix, modifier) in &field_modifiers {
        let combined_index_name = format!("index_for_geo_field_combined_until_{}", suffix);
        combined_schema_geo_field = modifier(combined_schema_geo_field);
        let schema = schema! {
            GEO_FIELD_NAME => combined_schema_geo_field.clone()
        };
        assert_eq!(
            con.ft_create(&combined_index_name, &CreateOptions::new(), &schema),
            Ok("OK".to_string())
        );
    }

    // After all of the modifiers above have been applied, add each of the mutually exclusive modifiers individually
    for (suffix, modifier) in &mutually_exclusive_common_modifiers {
        let combined_index_name = format!("index_for_geo_field_all_combined_with_{}", suffix);
        let schema = schema! {
            GEO_FIELD_NAME =>  modifier(combined_schema_geo_field.clone())
        };
        assert_eq!(
            con.ft_create(&combined_index_name, &CreateOptions::new(), &schema),
            Ok("OK".to_string())
        );
    }
    // Test that mutually exclusive modifiers are mutually exclusive indeed
    assert!(
        con.ft_create::<_, String>(
            "invalid_index",
            &CreateOptions::new(),
            &schema! {
                GEO_FIELD_NAME => SchemaGeoField::new().no_index(true).index_missing(true)
            }
        )
        .is_err()
    );
}

type VectorFieldModifier = fn(VectorField) -> VectorField;

#[test]
fn test_ft_create_schema_flat_vector_field() {
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = ctx.connection();

    const DIM: u32 = 128;

    type FlatVectorFieldBuilderModifier = fn(FlatVectorFieldBuilder) -> FlatVectorFieldBuilder;
    // FLAT-specific builder modifiers (applied before .build())
    let builder_modifiers: Vec<(&'static str, FlatVectorFieldBuilderModifier)> =
        vec![("block_size", |builder| builder.block_size(1000))];

    // Common field modifiers (applied after .build()) - not mutually exclusive
    let field_modifiers: Vec<(&'static str, VectorFieldModifier)> = vec![
        ("alias", |field| field.alias("vector_alias")),
        ("index_missing", |field| field.index_missing(true)),
    ];

    // For each Vector type
    for (vector_type_name, vector_type) in [
        ("float32", VectorType::Float32),
        ("float64", VectorType::Float64),
        ("bfloat16", VectorType::BFloat16),
        ("float16", VectorType::Float16),
        ("int8", VectorType::Int8),
        ("uint8", VectorType::UInt8),
    ] {
        // For each distance metric
        for (distance_metric_name, distance_metric) in [
            ("l2", DistanceMetric::L2),
            ("ip", DistanceMetric::IP),
            ("cosine", DistanceMetric::Cosine),
        ] {
            let base_name = format!("idx_flat_{}_{}", vector_type_name, distance_metric_name);

            // 1. Test each builder modifier individually
            for (builder_suffix, builder_modifier) in &builder_modifiers {
                let index_name = format!("{}_builder_{}", base_name, builder_suffix);
                let schema = schema! {
                    VECTOR_FIELD_NAME => builder_modifier(VectorField::flat(vector_type, DIM, distance_metric)).build()
                };
                assert_eq!(
                    con.ft_create(&index_name, &CreateOptions::new(), &schema),
                    Ok("OK".to_string())
                );
            }

            // 2. Test each common field modifier individually
            for (field_suffix, field_modifier) in &field_modifiers {
                let index_name = format!("{}_field_{}", base_name, field_suffix);
                let schema = schema! {
                    VECTOR_FIELD_NAME => field_modifier(VectorField::flat(vector_type, DIM, distance_metric).build())
                };
                assert_eq!(
                    con.ft_create(&index_name, &CreateOptions::new(), &schema),
                    Ok("OK".to_string())
                );
            }

            // 3. Test all builder modifiers combined progressively
            let mut combined_builder = VectorField::flat(vector_type, DIM, distance_metric);
            for (builder_suffix, builder_modifier) in &builder_modifiers {
                combined_builder = builder_modifier(combined_builder);
                let index_name = format!("{}_builders_until_{}", base_name, builder_suffix);
                let schema = schema! {
                    VECTOR_FIELD_NAME => combined_builder.clone().build()
                };
                assert_eq!(
                    con.ft_create(&index_name, &CreateOptions::new(), &schema),
                    Ok("OK".to_string())
                );
            }

            // 4. Test all builder modifiers + each field modifier
            for (field_suffix, field_modifier) in &field_modifiers {
                let index_name = format!("{}_all_builders_with_{}", base_name, field_suffix);
                let schema = schema! {
                    VECTOR_FIELD_NAME => field_modifier(combined_builder.clone().build())
                };
                assert_eq!(
                    con.ft_create(&index_name, &CreateOptions::new(), &schema),
                    Ok("OK".to_string())
                );
            }

            // 5. Test all builder modifiers + all field modifiers combined progressively
            let mut combined_field = combined_builder.clone().build();
            for (field_suffix, field_modifier) in &field_modifiers {
                combined_field = field_modifier(combined_field);
                let index_name =
                    format!("{}_all_builders_fields_until_{}", base_name, field_suffix);
                let schema = schema! {
                    VECTOR_FIELD_NAME => combined_field.clone()
                };
                assert_eq!(
                    con.ft_create(&index_name, &CreateOptions::new(), &schema),
                    Ok("OK".to_string())
                );
            }
        }
    }
}

#[test]
fn test_ft_create_schema_hnsw_vector_field() {
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = ctx.connection();

    const DIM: u32 = 128;

    type HnswVectorFieldBuilderModifier = fn(HnswVectorFieldBuilder) -> HnswVectorFieldBuilder;
    // HNSW-specific builder modifiers (applied before .build())
    let builder_modifiers: Vec<(&'static str, HnswVectorFieldBuilderModifier)> = vec![
        ("m", |builder| builder.m(16)),
        ("ef_construction", |builder| builder.ef_construction(200)),
        ("ef_runtime", |builder| builder.ef_runtime(20)),
        ("epsilon", |builder| builder.epsilon(0.01)),
    ];

    // Common field modifiers (applied after .build()) - not mutually exclusive
    let field_modifiers: Vec<(&'static str, VectorFieldModifier)> = vec![
        ("alias", |field| field.alias("vector_alias")),
        ("index_missing", |field| field.index_missing(true)),
    ];

    // For each Vector type
    for (vector_type_name, vector_type) in [
        ("float32", VectorType::Float32),
        ("float64", VectorType::Float64),
        ("bfloat16", VectorType::BFloat16),
        ("float16", VectorType::Float16),
        ("int8", VectorType::Int8),
        ("uint8", VectorType::UInt8),
    ] {
        // For each distance metric
        for (distance_metric_name, distance_metric) in [
            ("l2", DistanceMetric::L2),
            ("ip", DistanceMetric::IP),
            ("cosine", DistanceMetric::Cosine),
        ] {
            let base_name = format!("idx_hnsw_{}_{}", vector_type_name, distance_metric_name);

            // 1. Test each builder modifier individually
            for (builder_suffix, builder_modifier) in &builder_modifiers {
                let index_name = format!("{}_builder_{}", base_name, builder_suffix);
                let schema = schema! {
                    VECTOR_FIELD_NAME => builder_modifier(VectorField::hnsw(vector_type, DIM, distance_metric)).build()
                };
                assert_eq!(
                    con.ft_create(&index_name, &CreateOptions::new(), &schema),
                    Ok("OK".to_string())
                );
            }

            // 2. Test each common field modifier individually
            for (field_suffix, field_modifier) in &field_modifiers {
                let index_name = format!("{}_field_{}", base_name, field_suffix);
                let schema = schema! {
                    VECTOR_FIELD_NAME => field_modifier(VectorField::hnsw(vector_type, DIM, distance_metric).build())
                };
                assert_eq!(
                    con.ft_create(&index_name, &CreateOptions::new(), &schema),
                    Ok("OK".to_string())
                );
            }

            // 3. Test all builder modifiers combined progressively
            let mut combined_builder = VectorField::hnsw(vector_type, DIM, distance_metric);
            for (builder_suffix, builder_modifier) in &builder_modifiers {
                combined_builder = builder_modifier(combined_builder);
                let index_name = format!("{}_builders_until_{}", base_name, builder_suffix);
                let schema = schema! {
                    VECTOR_FIELD_NAME => combined_builder.clone().build()
                };
                assert_eq!(
                    con.ft_create(&index_name, &CreateOptions::new(), &schema),
                    Ok("OK".to_string())
                );
            }

            // 4. Test all builder modifiers + each field modifier
            for (field_suffix, field_modifier) in &field_modifiers {
                let index_name = format!("{}_all_builders_with_{}", base_name, field_suffix);
                let schema = schema! {
                    VECTOR_FIELD_NAME => field_modifier(combined_builder.clone().build())
                };
                assert_eq!(
                    con.ft_create(&index_name, &CreateOptions::new(), &schema),
                    Ok("OK".to_string())
                );
            }

            // 5. Test all builder modifiers + all field modifiers combined progressively
            let mut combined_field = combined_builder.clone().build();
            for (field_suffix, field_modifier) in &field_modifiers {
                combined_field = field_modifier(combined_field);
                let index_name =
                    format!("{}_all_builders_fields_until_{}", base_name, field_suffix);
                let schema = schema! {
                    VECTOR_FIELD_NAME => combined_field.clone()
                };
                assert_eq!(
                    con.ft_create(&index_name, &CreateOptions::new(), &schema),
                    Ok("OK".to_string())
                );
            }
        }
    }
}

#[test]
fn test_ft_create_schema_vamana_vector_field() {
    // The VAMANA index was introduced in Redis 8.2
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_2);
    let mut con = ctx.connection();

    const DIM: u32 = 128;

    // Common field modifiers (applied after .build()) - not mutually exclusive
    let field_modifiers: Vec<(&'static str, VectorFieldModifier)> = vec![
        ("alias", |field| field.alias("vector_alias")),
        ("index_missing", |field| field.index_missing(true)),
    ];

    // All compression types
    let compression_types: Vec<(&'static str, CompressionType)> = vec![
        ("lvq4", CompressionType::LVQ4),
        ("lvq8", CompressionType::LVQ8),
        ("lvq4x4", CompressionType::LVQ4x4),
        ("lvq4x8", CompressionType::LVQ4x8),
        ("leanvec4x8", CompressionType::LeanVec4x8),
        ("leanvec8x8", CompressionType::LeanVec8x8),
    ];

    // Note: VAMANA only supports float types (Float16, Float32)
    // Uses VamanaVectorType for compile-time type safety
    for (vector_type_name, vector_type) in [
        ("float16", VamanaVectorType::Float16),
        ("float32", VamanaVectorType::Float32),
    ] {
        // For each distance metric
        for (distance_metric_name, distance_metric) in [
            ("l2", DistanceMetric::L2),
            ("ip", DistanceMetric::IP),
            ("cosine", DistanceMetric::Cosine),
        ] {
            // For each compression type
            for (compression_name, compression_type) in &compression_types {
                let base_name = format!(
                    "idx_vamana_{}_{}_{}",
                    vector_type_name, distance_metric_name, compression_name
                );

                // Build the list of builder modifiers based on compression type
                // reduce only applies to LeanVec4x8 and LeanVec8x8
                let is_leanvec = matches!(
                    compression_type,
                    CompressionType::LeanVec4x8 | CompressionType::LeanVec8x8
                );

                type VamanaVectorFieldBuilderModifier =
                    Box<dyn Fn(VamanaVectorFieldBuilder) -> VamanaVectorFieldBuilder>;
                let builder_modifiers: Vec<(&'static str, VamanaVectorFieldBuilderModifier)> = {
                    let compression = *compression_type;
                    let mut modifiers: Vec<(&'static str, VamanaVectorFieldBuilderModifier)> = vec![
                        (
                            "compression",
                            Box::new(move |builder| builder.compression(compression)),
                        ),
                        (
                            "construction_window_size",
                            Box::new(|builder| builder.construction_window_size(300)),
                        ),
                        (
                            "graph_max_degree",
                            Box::new(|builder| builder.graph_max_degree(128)),
                        ),
                        (
                            "search_window_size",
                            Box::new(|builder| builder.search_window_size(20)),
                        ),
                        ("epsilon", Box::new(|builder| builder.epsilon(0.02))),
                        (
                            "training_threshold",
                            Box::new(|builder| builder.training_threshold(2048)),
                        ),
                    ];

                    if is_leanvec {
                        modifiers.push(("reduce", Box::new(|builder| builder.reduce(64))));
                    }

                    modifiers
                };

                // 1. Test each builder modifier individually
                for (builder_suffix, builder_modifier) in &builder_modifiers {
                    let index_name = format!("{}_builder_{}", base_name, builder_suffix);
                    let schema = schema! {
                        VECTOR_FIELD_NAME => builder_modifier(VectorField::vamana(vector_type, DIM, distance_metric)).build()
                    };
                    assert_eq!(
                        con.ft_create(&index_name, &CreateOptions::new(), &schema),
                        Ok("OK".to_string())
                    );
                }

                // 2. Test each common field modifier individually
                for (field_suffix, field_modifier) in &field_modifiers {
                    let index_name = format!("{}_field_{}", base_name, field_suffix);
                    let schema = schema! {
                        VECTOR_FIELD_NAME => field_modifier(VectorField::vamana(vector_type, DIM, distance_metric).build())
                    };
                    assert_eq!(
                        con.ft_create(&index_name, &CreateOptions::new(), &schema),
                        Ok("OK".to_string())
                    );
                }

                // 3. Test all builder modifiers combined progressively
                let mut combined_builder = VectorField::vamana(vector_type, DIM, distance_metric);
                for (builder_suffix, builder_modifier) in &builder_modifiers {
                    combined_builder = builder_modifier(combined_builder);
                    let index_name = format!("{}_builders_until_{}", base_name, builder_suffix);
                    let schema = schema! {
                        VECTOR_FIELD_NAME => combined_builder.clone().build()
                    };
                    assert_eq!(
                        con.ft_create(&index_name, &CreateOptions::new(), &schema),
                        Ok("OK".to_string())
                    );
                }

                // 4. Test all builder modifiers + each field modifier
                for (field_suffix, field_modifier) in &field_modifiers {
                    let index_name = format!("{}_all_builders_with_{}", base_name, field_suffix);
                    let schema = schema! {
                        VECTOR_FIELD_NAME => field_modifier(combined_builder.clone().build())
                    };
                    assert_eq!(
                        con.ft_create(&index_name, &CreateOptions::new(), &schema),
                        Ok("OK".to_string())
                    );
                }

                // 5. Test all builder modifiers + all field modifiers combined progressively
                let mut combined_field = combined_builder.clone().build();
                for (field_suffix, field_modifier) in &field_modifiers {
                    combined_field = field_modifier(combined_field);
                    let index_name =
                        format!("{}_all_builders_fields_until_{}", base_name, field_suffix);
                    let schema = schema! {
                        VECTOR_FIELD_NAME => combined_field.clone()
                    };
                    assert_eq!(
                        con.ft_create(&index_name, &CreateOptions::new(), &schema),
                        Ok("OK".to_string())
                    );
                }
            }
        }
    }
}

#[test]
fn test_ft_create_schema_geoshape_field() {
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = ctx.connection();

    type SchemaGeoShapeFieldModifier = fn(SchemaGeoShapeField) -> SchemaGeoShapeField;
    let field_modifiers: Vec<(&'static str, SchemaGeoShapeFieldModifier)> = vec![
        // Common modifiers
        ("alias", |field| field.alias("geo_shape_alias")),
    ];

    // Common modifiers that are mutually exclusive
    let mutually_exclusive_common_modifiers: Vec<(&'static str, SchemaGeoShapeFieldModifier)> = vec![
        ("index_missing", |field| field.index_missing(true)),
        ("no_index", |field| field.no_index(true)),
    ];

    // For each coordinate system
    for coord_system in &[CoordSystem::Spherical, CoordSystem::Flat] {
        // Test each common field modifier individually
        for (suffix, modifier) in &field_modifiers {
            let index_name = format!(
                "index_for_geoshape_{:?}_field_with_{}",
                coord_system, suffix
            );
            let schema = schema! {
                GEOSHAPE_FIELD_NAME => modifier(SchemaGeoShapeField::new().coord_system(coord_system.clone()))
            };
            assert_eq!(
                con.ft_create(&index_name, &CreateOptions::new(), &schema),
                Ok("OK".to_string())
            );
        }

        // Test each mutually exclusive modifier individually
        for (suffix, modifier) in &mutually_exclusive_common_modifiers {
            let index_name = format!(
                "index_for_geoshape_{:?}_field_with_{}",
                coord_system, suffix
            );
            let schema = schema! {
                GEOSHAPE_FIELD_NAME => modifier(SchemaGeoShapeField::new().coord_system(coord_system.clone()))
            };
            assert_eq!(
                con.ft_create(&index_name, &CreateOptions::new(), &schema),
                Ok("OK".to_string())
            );
        }

        // Test all modifiers that are not mutually exclusive
        let mut combined_field = SchemaGeoShapeField::new().coord_system(coord_system.clone());
        for (_suffix, modifier) in &field_modifiers {
            combined_field = modifier(combined_field);
        }
        let combined_index_name =
            format!("index_for_geoshape_{:?}_field_all_combined", coord_system);
        let schema = schema! {
            GEOSHAPE_FIELD_NAME => combined_field.clone()
        };
        assert_eq!(
            con.ft_create(&combined_index_name, &CreateOptions::new(), &schema),
            Ok("OK".to_string())
        );

        // After all of the modifiers above have been applied, add each of the mutually exclusive modifiers individually
        for (suffix, modifier) in &mutually_exclusive_common_modifiers {
            let combined_index_name = format!(
                "index_for_geoshape_{:?}_field_all_combined_with_{}",
                coord_system, suffix
            );
            let schema = schema! {
                GEOSHAPE_FIELD_NAME =>  modifier(combined_field.clone())
            };
            assert_eq!(
                con.ft_create(&combined_index_name, &CreateOptions::new(), &schema),
                Ok("OK".to_string())
            );
        }

        // Test that mutually exclusive modifiers are mutually exclusive indeed
        assert!(
            con.ft_create::<_, String>(
                "invalid_index",
                &CreateOptions::new(),
                &schema! {
                    GEOSHAPE_FIELD_NAME => SchemaGeoShapeField::new().coord_system(coord_system.clone()).no_index(true).index_missing(true)
                }
            )
            .is_err()
        );
    }
}
