/*
Architecture Overview:
Notes:
- redis-rs can use FtCreateCommand::into_cmd() to send the command to a Redis server
- The schema! macro uses the builder pattern for Vector fields and into() for other field types

Builder
│
├── builder methods set options
│
└── FieldDefinition
|   └── For Vector fields produced by VectorBuilders' build()
|   └── Other types rely on into()
│
    │ inserted by schema! macro or manual insert
    ▼
RediSearchSchema = Vec<(String, FieldDefinition)>
      │
      │ Each FieldDefinition implements ToRedisArgs
      ▼
ToRedisArgs::write_redis_args() serializes to Redis protocol
      │
      └── Vector fields: writes algorithm, attribute count, then attributes
      └── GeoShape fields: writes field type and coordinate system, then modifiers
      └── Other fields: writes field type, then modifiers
      ▼
FtCreateCommand collects all args and offers conversion to redis::Cmd
*/

mod create_tests {
    use crate::create::*;
    use crate::schema;
    use crate::search::{
        CompressionType, DistanceMetric, RediSearchSchema, SchemaNumericField, SchemaTagField,
        SchemaTextField, VamanaVectorType, VectorField, VectorType,
    };

    static INDEX_NAME: &str = "index";
    static TEXT_FIELD_NAME: &str = "title";
    static NUMERIC_FIELD_NAME: &str = "price";
    static TAG_FIELD_NAME: &str = "condition";
    static GEO_FIELD_NAME: &str = "location";
    static VECTOR_FIELD_NAME: &str = "embedding";
    static GEOSHAPE_FIELD_NAME: &str = "area";
    static CUSTOM_ALIAS: &str = "custom_alias";

    #[test]
    #[should_panic(expected = "FT.CREATE command requires at least one field in the schema")]
    fn test_empty_schema_panics() {
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(RediSearchSchema::new());

        // This should panic because the schema is empty
        ft_create.into_cmd();
    }

    #[test]
    #[should_panic(expected = "FT.CREATE command requires a non-empty index name")]
    fn test_empty_index_name_panics() {
        let ft_create = FtCreateCommand::new("").schema(schema! {
            TEXT_FIELD_NAME => SchemaTextField::new()
        });

        // This should panic because the index name is empty
        ft_create.into_cmd();
    }

    // ============================================================================
    // CreateOptions Tests
    // ============================================================================
    #[test]
    fn test_option_on() {
        let ft_create = FtCreateCommand::new(INDEX_NAME)
            .options(CreateOptions::new().on(IndexDataType::Hash))
            .schema(schema! {
                TEXT_FIELD_NAME => SchemaTextField::new()
            });
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index ON HASH SCHEMA title TEXT"
        );
    }

    #[test]
    fn test_option_prefix_single() {
        let ft_create = FtCreateCommand::new(INDEX_NAME)
            .options(CreateOptions::new().prefix("prefix"))
            .schema(schema! {
                TEXT_FIELD_NAME => SchemaTextField::new()
            });
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index PREFIX 1 prefix SCHEMA title TEXT"
        );
    }

    #[test]
    fn test_option_prefix_multiple() {
        let ft_create = FtCreateCommand::new(INDEX_NAME)
            .options(CreateOptions::new().prefix("pref1").prefix("pref2"))
            .schema(schema! {
                TEXT_FIELD_NAME => SchemaTextField::new()
            });
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index PREFIX 2 pref1 pref2 SCHEMA title TEXT"
        );
    }

    #[test]
    fn test_option_filter() {
        let ft_create = FtCreateCommand::new(INDEX_NAME)
            .options(CreateOptions::new().filter("@field != \"\""))
            .schema(schema! {
                TEXT_FIELD_NAME => SchemaTextField::new()
            });
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index FILTER '@field != \"\"' SCHEMA title TEXT"
        );
    }

    #[test]
    fn test_option_language() {
        let ft_create = FtCreateCommand::new(INDEX_NAME)
            .options(CreateOptions::new().language(SearchLanguage::Arabic))
            .schema(schema! {
                TEXT_FIELD_NAME => SchemaTextField::new()
            });
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index LANGUAGE ARABIC SCHEMA title TEXT"
        );
    }

    #[test]
    fn test_option_language_field() {
        let ft_create = FtCreateCommand::new(INDEX_NAME)
            .options(CreateOptions::new().language_field("@field"))
            .schema(schema! {
                TEXT_FIELD_NAME => SchemaTextField::new()
            });
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index LANGUAGE_FIELD @field SCHEMA title TEXT"
        );
    }

    #[test]
    fn test_option_score() {
        let ft_create = FtCreateCommand::new(INDEX_NAME)
            .options(CreateOptions::new().score(1.0))
            .schema(schema! {
                TEXT_FIELD_NAME => SchemaTextField::new()
            });
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCORE 1.0 SCHEMA title TEXT"
        );
    }

    #[test]
    fn test_option_score_field() {
        let ft_create = FtCreateCommand::new(INDEX_NAME)
            .options(CreateOptions::new().score_field("@field"))
            .schema(schema! {
                TEXT_FIELD_NAME => SchemaTextField::new()
            });
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCORE_FIELD @field SCHEMA title TEXT"
        );
    }

    #[test]
    fn test_option_maxtextfields() {
        let ft_create = FtCreateCommand::new(INDEX_NAME)
            .options(CreateOptions::new().max_text_fields())
            .schema(schema! {
                TEXT_FIELD_NAME => SchemaTextField::new()
            });
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index MAXTEXTFIELDS SCHEMA title TEXT"
        );
    }

    #[test]
    fn test_option_temporary() {
        let ft_create = FtCreateCommand::new(INDEX_NAME)
            .options(CreateOptions::new().temporary(1))
            .schema(schema! {
                TEXT_FIELD_NAME => SchemaTextField::new()
            });
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index TEMPORARY 1 SCHEMA title TEXT"
        );
    }

    #[test]
    fn test_option_nooffsets() {
        let ft_create = FtCreateCommand::new(INDEX_NAME)
            .options(CreateOptions::new().no_offsets())
            .schema(schema! {
                TEXT_FIELD_NAME => SchemaTextField::new()
            });
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index NOOFFSETS SCHEMA title TEXT"
        );
    }

    #[test]
    fn test_option_nohl() {
        let ft_create = FtCreateCommand::new(INDEX_NAME)
            .options(CreateOptions::new().no_highlight())
            .schema(schema! {
                TEXT_FIELD_NAME => SchemaTextField::new()
            });
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index NOHL SCHEMA title TEXT"
        );
    }

    #[test]
    fn test_option_nofields() {
        let ft_create = FtCreateCommand::new(INDEX_NAME)
            .options(CreateOptions::new().no_fields())
            .schema(schema! {
                TEXT_FIELD_NAME => SchemaTextField::new()
            });
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index NOFIELDS SCHEMA title TEXT"
        );
    }

    #[test]
    fn test_option_nofreqs() {
        let ft_create = FtCreateCommand::new(INDEX_NAME)
            .options(CreateOptions::new().no_freqs())
            .schema(schema! {
                TEXT_FIELD_NAME => SchemaTextField::new()
            });
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index NOFREQS SCHEMA title TEXT"
        );
    }

    #[test]
    fn test_option_skipinitialscan() {
        let ft_create = FtCreateCommand::new(INDEX_NAME)
            .options(CreateOptions::new().skip_initial_scan())
            .schema(schema! {
                TEXT_FIELD_NAME => SchemaTextField::new()
            });
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SKIPINITIALSCAN SCHEMA title TEXT"
        );
    }

    #[test]
    fn test_option_stopwords_single() {
        let ft_create = FtCreateCommand::new(INDEX_NAME)
            .options(CreateOptions::new().stopword("stopword"))
            .schema(schema! {
                TEXT_FIELD_NAME => SchemaTextField::new()
            });
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index STOPWORDS 1 stopword SCHEMA title TEXT"
        );
    }

    #[test]
    fn test_option_stopwords_multiple() {
        let ft_create = FtCreateCommand::new(INDEX_NAME)
            .options(CreateOptions::new().stopword("one").stopword("two"))
            .schema(schema! {
                TEXT_FIELD_NAME => SchemaTextField::new()
            });
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index STOPWORDS 2 one two SCHEMA title TEXT"
        );
    }

    // ============================================================================
    // TEXT Field Tests
    // ============================================================================
    #[test]
    fn test_text_field() {
        let schema = schema! {
            TEXT_FIELD_NAME => SchemaTextField::new(),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(ft_create.into_args(), "FT.CREATE index SCHEMA title TEXT");
    }

    #[test]
    fn test_text_field_with_nostem() {
        let schema = schema! {
            TEXT_FIELD_NAME => SchemaTextField::new().no_stem(true),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA title TEXT NOSTEM"
        );
    }

    #[test]
    fn test_text_field_with_weight() {
        let schema = schema! {
            TEXT_FIELD_NAME => SchemaTextField::new().weight(1.0),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA title TEXT WEIGHT 1.0"
        );
    }

    #[test]
    fn test_text_field_with_phonetic() {
        let schema = schema! {
            TEXT_FIELD_NAME => SchemaTextField::new().phonetic(Phonetic::DmEn),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA title TEXT PHONETIC dm:en"
        );
    }

    #[test]
    fn test_text_field_with_withsuffixtrie() {
        let schema = schema! {
            TEXT_FIELD_NAME => SchemaTextField::new().with_suffix_trie(true),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA title TEXT WITHSUFFIXTRIE"
        );
    }

    #[test]
    fn test_text_field_with_indexempty() {
        let schema = schema! {
            TEXT_FIELD_NAME => SchemaTextField::new().index_empty(true),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA title TEXT INDEXEMPTY"
        );
    }

    #[test]
    fn test_text_field_with_alias() {
        let schema = schema! {
            TEXT_FIELD_NAME => SchemaTextField::new().alias(CUSTOM_ALIAS),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA title AS custom_alias TEXT"
        );
    }

    #[test]
    fn test_text_field_with_indexmissing() {
        let schema = schema! {
            TEXT_FIELD_NAME => SchemaTextField::new().index_missing(true),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA title TEXT INDEXMISSING"
        );
    }

    #[test]
    fn test_text_field_with_sortable() {
        let schema = schema! {
            TEXT_FIELD_NAME => SchemaTextField::new().sortable(Sortable::Yes),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA title TEXT SORTABLE"
        );
    }

    #[test]
    fn test_text_field_with_sortable_unf() {
        let schema = schema! {
            TEXT_FIELD_NAME => SchemaTextField::new().sortable(Sortable::Unf),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA title TEXT SORTABLE UNF"
        );
    }

    #[test]
    fn test_text_field_with_noindex() {
        let schema = schema! {
            TEXT_FIELD_NAME => SchemaTextField::new().no_index(true),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA title TEXT NOINDEX"
        );
    }

    #[test]
    fn test_text_field_with_all_options() {
        let field = SchemaTextField::new()
            .no_stem(true)
            .weight(1.0)
            .phonetic(Phonetic::DmEn)
            .with_suffix_trie(true)
            .index_empty(true)
            .alias(CUSTOM_ALIAS)
            .index_missing(true)
            .sortable(Sortable::Unf);
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema! {
            TEXT_FIELD_NAME => field.clone(),
        });
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA title AS custom_alias TEXT INDEXMISSING NOSTEM WEIGHT 1.0 PHONETIC dm:en WITHSUFFIXTRIE INDEXEMPTY SORTABLE UNF"
        );
        // Index missing and no index are mutually exclusive
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema! {
            TEXT_FIELD_NAME => field.index_missing(false).no_index(true),
        });
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA title AS custom_alias TEXT NOSTEM WEIGHT 1.0 PHONETIC dm:en WITHSUFFIXTRIE INDEXEMPTY SORTABLE UNF NOINDEX"
        );
    }

    // ============================================================================
    // NUMERIC Field Tests
    // ============================================================================
    #[test]
    fn test_numeric_field() {
        let schema = schema! {
            NUMERIC_FIELD_NAME => SchemaNumericField::new(),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA price NUMERIC"
        );
    }

    #[test]
    fn test_numeric_field_with_alias() {
        let schema = schema! {
            NUMERIC_FIELD_NAME => SchemaNumericField::new().alias(CUSTOM_ALIAS),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA price AS custom_alias NUMERIC"
        );
    }

    #[test]
    fn test_numeric_field_with_indexmissing() {
        let schema = schema! {
            NUMERIC_FIELD_NAME => SchemaNumericField::new().index_missing(true),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA price NUMERIC INDEXMISSING"
        );
    }

    #[test]
    fn test_numeric_field_with_sortable() {
        let schema = schema! {
            NUMERIC_FIELD_NAME => SchemaNumericField::new().sortable(Sortable::Yes),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA price NUMERIC SORTABLE"
        );
    }

    #[test]
    fn test_numeric_field_with_sortable_unf() {
        let schema = schema! {
            NUMERIC_FIELD_NAME => SchemaNumericField::new().sortable(Sortable::Unf),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA price NUMERIC SORTABLE UNF"
        );
    }

    #[test]
    fn test_numeric_field_with_noindex() {
        let schema = schema! {
            NUMERIC_FIELD_NAME => SchemaNumericField::new().no_index(true),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA price NUMERIC NOINDEX"
        );
    }

    #[test]
    fn test_numeric_field_with_all_options() {
        let field = SchemaNumericField::new()
            .alias(CUSTOM_ALIAS)
            .index_missing(true)
            .sortable(Sortable::Unf);
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema! {
            NUMERIC_FIELD_NAME => field.clone(),
        });
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA price AS custom_alias NUMERIC INDEXMISSING SORTABLE UNF"
        );
        // Index missing and no index are mutually exclusive
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema! {
            NUMERIC_FIELD_NAME => field.index_missing(false).no_index(true),
        });
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA price AS custom_alias NUMERIC SORTABLE UNF NOINDEX"
        );
    }

    // ============================================================================
    // GEO Field Tests
    // ============================================================================
    #[test]
    fn test_geo_field() {
        let schema = schema! {
            GEO_FIELD_NAME => SchemaGeoField::new(),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(ft_create.into_args(), "FT.CREATE index SCHEMA location GEO");
    }

    #[test]
    fn test_geo_field_with_alias() {
        let schema = schema! {
            GEO_FIELD_NAME => SchemaGeoField::new().alias(CUSTOM_ALIAS),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA location AS custom_alias GEO"
        );
    }

    #[test]
    fn test_geo_field_with_indexmissing() {
        let schema = schema! {
            GEO_FIELD_NAME => SchemaGeoField::new().index_missing(true),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA location GEO INDEXMISSING"
        );
    }

    #[test]
    fn test_geo_field_with_sortable() {
        let schema = schema! {
            GEO_FIELD_NAME => SchemaGeoField::new().sortable(Sortable::Yes),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA location GEO SORTABLE"
        );
    }

    #[test]
    fn test_geo_field_with_sortable_unf() {
        let schema = schema! {
            GEO_FIELD_NAME => SchemaGeoField::new().sortable(Sortable::Unf),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA location GEO SORTABLE UNF"
        );
    }

    #[test]
    fn test_geo_field_with_noindex() {
        let schema = schema! {
            GEO_FIELD_NAME => SchemaGeoField::new().no_index(true),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA location GEO NOINDEX"
        );
    }

    #[test]
    fn test_geo_field_with_all_options() {
        let field = SchemaGeoField::new()
            .alias(CUSTOM_ALIAS)
            .index_missing(true)
            .sortable(Sortable::Unf);
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema! {
            GEO_FIELD_NAME => field.clone(),
        });
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA location AS custom_alias GEO INDEXMISSING SORTABLE UNF"
        );
        // Index missing and no index are mutually exclusive
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema! {
            GEO_FIELD_NAME => field.index_missing(false).no_index(true),
        });
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA location AS custom_alias GEO SORTABLE UNF NOINDEX"
        );
    }

    // ============================================================================
    // TAG Field Tests
    // ============================================================================
    #[test]
    fn test_tag_field() {
        let schema = schema! {
            TAG_FIELD_NAME => SchemaTagField::new(),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA condition TAG"
        );
    }

    #[test]
    fn test_tag_field_with_separator() {
        let schema = schema! {
            TAG_FIELD_NAME => SchemaTagField::new().separator(','),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA condition TAG SEPARATOR ,"
        );
    }

    #[test]
    fn test_tag_field_with_casesensitive() {
        let schema = schema! {
            TAG_FIELD_NAME => SchemaTagField::new().case_sensitive(true),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA condition TAG CASESENSITIVE"
        );
    }

    #[test]
    fn test_tag_field_with_withsuffixtrie() {
        let schema = schema! {
            TAG_FIELD_NAME => SchemaTagField::new().with_suffix_trie(true),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA condition TAG WITHSUFFIXTRIE"
        );
    }

    #[test]
    fn test_tag_field_with_indexempty() {
        let schema = schema! {
            TAG_FIELD_NAME => SchemaTagField::new().index_empty(true),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA condition TAG INDEXEMPTY"
        );
    }

    #[test]
    fn test_tag_field_with_alias() {
        let schema = schema! {
            TAG_FIELD_NAME => SchemaTagField::new().alias(CUSTOM_ALIAS),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA condition AS custom_alias TAG"
        );
    }

    #[test]
    fn test_tag_field_with_indexmissing() {
        let schema = schema! {
            TAG_FIELD_NAME => SchemaTagField::new().index_missing(true),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA condition TAG INDEXMISSING"
        );
    }

    #[test]
    fn test_tag_field_with_sortable() {
        let schema = schema! {
            TAG_FIELD_NAME => SchemaTagField::new().sortable(Sortable::Yes),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA condition TAG SORTABLE"
        );
    }

    #[test]
    fn test_tag_field_with_sortable_unf() {
        let schema = schema! {
            TAG_FIELD_NAME => SchemaTagField::new().sortable(Sortable::Unf),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA condition TAG SORTABLE UNF"
        );
    }

    #[test]
    fn test_tag_field_with_noindex() {
        let schema = schema! {
            TAG_FIELD_NAME => SchemaTagField::new().no_index(true),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA condition TAG NOINDEX"
        );
    }

    #[test]
    fn test_tag_field_with_all_options() {
        let field = SchemaTagField::new()
            .alias(CUSTOM_ALIAS)
            .separator(',')
            .case_sensitive(true)
            .with_suffix_trie(true)
            .index_empty(true)
            .sortable(Sortable::Unf);
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema! {
            TAG_FIELD_NAME => field.clone(),
        });
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA condition AS custom_alias TAG SEPARATOR , CASESENSITIVE WITHSUFFIXTRIE INDEXEMPTY SORTABLE UNF"
        );
        // Index missing and no index are mutually exclusive
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema! {
            TAG_FIELD_NAME => field.index_missing(false).no_index(true),
        });
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA condition AS custom_alias TAG SEPARATOR , CASESENSITIVE WITHSUFFIXTRIE INDEXEMPTY SORTABLE UNF NOINDEX"
        );
    }

    // ============================================================================
    // VECTOR Field Tests
    // ============================================================================
    #[test]
    #[should_panic(expected = "Vector dimension must be positive (greater than 0)")]
    fn test_flat_vector_zero_dimension_panics() {
        VectorField::flat(VectorType::Float32, 0, DistanceMetric::Cosine);
    }

    #[test]
    #[should_panic(expected = "Vector dimension must be positive (greater than 0)")]
    fn test_hnsw_vector_zero_dimension_panics() {
        VectorField::hnsw(VectorType::Float32, 0, DistanceMetric::L2);
    }

    #[test]
    #[should_panic(expected = "Vector dimension must be positive (greater than 0)")]
    fn test_vamana_vector_zero_dimension_panics() {
        VectorField::vamana(VamanaVectorType::Float32, 0, DistanceMetric::IP);
    }

    #[test]
    fn test_vector_field_with_valid_dimension_one() {
        let schema = schema! {
            VECTOR_FIELD_NAME => VectorField::flat(VectorType::Float32, 1, DistanceMetric::Cosine)
                .build(),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
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
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
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
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
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
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA embedding VECTOR FLAT 8 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2 BLOCK_SIZE 1000"
        );
    }

    #[test]
    fn test_vector_field_hnsw_algorithm() {
        let schema = schema! {
            VECTOR_FIELD_NAME => VectorField::hnsw(VectorType::Float32, 2, DistanceMetric::L2)
                .m(40)
                .ef_construction(250)
                .ef_runtime(20)
                .build(),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA embedding VECTOR HNSW 12 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2 M 40 EF_CONSTRUCTION 250 EF_RUNTIME 20"
        );
    }

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
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema! {
            VECTOR_FIELD_NAME => vamana_field_builder.clone().build(),
        });
        // Note: REDUCE should not be included because it only applies to LeanVec4x8 and LeanVec8x8 compression types
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA embedding VECTOR SVS-VAMANA 18 TYPE FLOAT32 DIM 1024 DISTANCE_METRIC COSINE COMPRESSION LVQ8 CONSTRUCTION_WINDOW_SIZE 300 GRAPH_MAX_DEGREE 128 SEARCH_WINDOW_SIZE 20 EPSILON 0.02 TRAINING_THRESHOLD 2048"
        );
        // Test with LeanVec4x8 compression which should include REDUCE
        let vamana_field_builder = vamana_field_builder
            .compression(CompressionType::LeanVec4x8)
            .reduce(reduce);
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema! {
            VECTOR_FIELD_NAME => vamana_field_builder.clone().build(),
        });
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA embedding VECTOR SVS-VAMANA 20 TYPE FLOAT32 DIM 1024 DISTANCE_METRIC COSINE COMPRESSION LeanVec4x8 CONSTRUCTION_WINDOW_SIZE 300 GRAPH_MAX_DEGREE 128 SEARCH_WINDOW_SIZE 20 EPSILON 0.02 TRAINING_THRESHOLD 2048 REDUCE 512"
        );
        // Test that bigger reduce parameters are clamped to dim - 1
        let vamana_field_builder = vamana_field_builder.reduce(1024);
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema! {
            VECTOR_FIELD_NAME => vamana_field_builder.clone().build(),
        });
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA embedding VECTOR SVS-VAMANA 20 TYPE FLOAT32 DIM 1024 DISTANCE_METRIC COSINE COMPRESSION LeanVec4x8 CONSTRUCTION_WINDOW_SIZE 300 GRAPH_MAX_DEGREE 128 SEARCH_WINDOW_SIZE 20 EPSILON 0.02 TRAINING_THRESHOLD 2048 REDUCE 1023"
        );
        // Test that the minimal reduction is 1
        let vamana_field_builder = vamana_field_builder.reduce(0);
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema! {
            VECTOR_FIELD_NAME => vamana_field_builder.clone().build(),
        });
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA embedding VECTOR SVS-VAMANA 20 TYPE FLOAT32 DIM 1024 DISTANCE_METRIC COSINE COMPRESSION LeanVec4x8 CONSTRUCTION_WINDOW_SIZE 300 GRAPH_MAX_DEGREE 128 SEARCH_WINDOW_SIZE 20 EPSILON 0.02 TRAINING_THRESHOLD 2048 REDUCE 1"
        );
        // Test that training threshold is clamped to 100 * DEFAULT_BLOCK_SIZE, where DEFAULT_BLOCK_SIZE is 1024
        let vamana_field_builder = vamana_field_builder.training_threshold(100 * 1024 + 1);
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema! {
            VECTOR_FIELD_NAME => vamana_field_builder.clone().build(),
        });
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA embedding VECTOR SVS-VAMANA 20 TYPE FLOAT32 DIM 1024 DISTANCE_METRIC COSINE COMPRESSION LeanVec4x8 CONSTRUCTION_WINDOW_SIZE 300 GRAPH_MAX_DEGREE 128 SEARCH_WINDOW_SIZE 20 EPSILON 0.02 TRAINING_THRESHOLD 102400 REDUCE 1"
        );
        // Test that training threshold is only applicable when there is a compression type
        let vamana_field_builder =
            VectorField::vamana(VamanaVectorType::Float32, 1024, DistanceMetric::Cosine)
                .training_threshold(2048);
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema! {
            VECTOR_FIELD_NAME => vamana_field_builder.clone().build(),
        });
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA embedding VECTOR SVS-VAMANA 6 TYPE FLOAT32 DIM 1024 DISTANCE_METRIC COSINE"
        );
    }

    // ============================================================================
    // GEOSHAPE Field Tests
    // ============================================================================
    #[test]
    fn test_geoshape_field_without_options_defaults_coord_system_to_spherical() {
        let schema = schema! {
            GEOSHAPE_FIELD_NAME => SchemaGeoShapeField::new(),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA area GEOSHAPE SPHERICAL"
        );
    }

    #[test]
    fn test_geoshape_field_with_flat_coord_system() {
        let schema = schema! {
            GEOSHAPE_FIELD_NAME => SchemaGeoShapeField::new().coord_system(CoordSystem::Flat),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA area GEOSHAPE FLAT"
        );
    }

    #[test]
    fn test_geoshape_field_with_alias() {
        let schema = schema! {
            GEOSHAPE_FIELD_NAME => SchemaGeoShapeField::new().alias(CUSTOM_ALIAS),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA area AS custom_alias GEOSHAPE SPHERICAL"
        );
    }

    #[test]
    fn test_geoshape_field_with_indexmissing() {
        let schema = schema! {
            GEOSHAPE_FIELD_NAME => SchemaGeoShapeField::new().index_missing(true),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA area GEOSHAPE SPHERICAL INDEXMISSING"
        );
    }

    #[test]
    fn test_geoshape_field_with_noindex() {
        let schema = schema! {
            GEOSHAPE_FIELD_NAME => SchemaGeoShapeField::new().no_index(true),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA area GEOSHAPE SPHERICAL NOINDEX"
        );
    }

    // ============================================================================
    // Other Tests
    // ============================================================================
    #[test]
    fn test_multiple_fields() {
        let mut schema = RediSearchSchema::new();
        schema.insert(TEXT_FIELD_NAME, SchemaTextField::new().weight(2.0));
        schema.insert(NUMERIC_FIELD_NAME, SchemaNumericField::new());
        schema.insert(TAG_FIELD_NAME, SchemaTagField::new().separator(','));

        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA title TEXT WEIGHT 2.0 price NUMERIC condition TAG SEPARATOR ,"
        );
    }

    // ============================================================================
    // Website examples
    // <https://redis.io/docs/latest/commands/ft.create/#examples>
    // ============================================================================
    #[test]
    fn test_create_blog_post_index() {
        /*
        Create an index that stores the title, publication date, and categories of blog post hashes whose keys start with blog:post: (for example, blog:post:1).
        FT.CREATE idx ON HASH PREFIX 1 blog:post: SCHEMA title TEXT SORTABLE published_at NUMERIC SORTABLE category TAG SORTABLE
        */
        let schema = schema! {
            "title" =>  SchemaTextField::new().sortable(Sortable::Yes),
            "published_at" => SchemaNumericField::new().sortable(Sortable::Yes),
            "category" => SchemaTagField::new().sortable(Sortable::Yes),
        };
        let options = CreateOptions::new()
            .on(IndexDataType::Hash)
            .prefix("blog:post:");

        let ft_create = FtCreateCommand::new("idx").options(options).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE idx ON HASH PREFIX 1 blog:post: SCHEMA title TEXT SORTABLE published_at NUMERIC SORTABLE category TAG SORTABLE"
        );
    }

    #[test]
    fn test_attribute_with_alias_and_dual_index() {
        /*
        Index the sku attribute from a hash as both a TAG and as TEXT.
        FT.CREATE idx ON HASH PREFIX 1 blog:post: SCHEMA sku AS sku_text TEXT sku AS sku_tag TAG SORTABLE
        */
        let sku_text_field = SchemaTextField::new().alias("sku_text");

        let sku_tag_field = SchemaTagField::new()
            .alias("sku_tag")
            .sortable(Sortable::Yes);

        let schema = schema! {
            "sku" => sku_text_field,
            "sku" => sku_tag_field,
        };

        let options = CreateOptions::new()
            .on(IndexDataType::Hash)
            .prefix("blog:post:");

        let ft_create = FtCreateCommand::new("idx").options(options).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE idx ON HASH PREFIX 1 blog:post: SCHEMA sku AS sku_text TEXT sku AS sku_tag TAG SORTABLE"
        );
    }

    #[test]
    fn test_index_two_hashes_within_the_same_index() {
        /*
        Index two different hashes, one containing author data and one containing books, in the same index.
        FT.CREATE author-books-idx ON HASH PREFIX 2 author:details: book:details: SCHEMA author_id TAG SORTABLE author_ids TAG title TEXT name TEXT
        */
        let ft_create = FtCreateCommand::new("author-books-idx")
            .options(
                CreateOptions::new()
                    .on(IndexDataType::Hash)
                    .prefix("author:details:")
                    .prefix("book:details:"),
            )
            .schema(schema! {
                "author_id" =>  SchemaTagField::new().sortable(Sortable::Yes),
                "author_ids" => SchemaTagField::new(),
                "title" => SchemaTextField::new(),
                "name" => SchemaTextField::new(),
            });

        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE author-books-idx ON HASH PREFIX 2 author:details: book:details: SCHEMA author_id TAG SORTABLE author_ids TAG title TEXT name TEXT"
        );
    }

    #[test]
    fn test_index_with_filter() {
        // In this example, keys for author data use the key pattern author:details:<id> while keys for book data use the pattern book:details:<id>.

        /*
        Index authors whose names start with G.
        FT.CREATE g-authors-idx ON HASH PREFIX 1 author:details FILTER 'startswith(@name, "G")' SCHEMA name TEXT
        */
        let ft_create = FtCreateCommand::new("g-authors-idx")
            .options(
                CreateOptions::new()
                    .on(IndexDataType::Hash)
                    .prefix("author:details")
                    .filter("startswith(@name, \"G\")"),
            )
            .schema(schema! {
                "name" =>  SchemaTextField::new(),
            });

        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE g-authors-idx ON HASH PREFIX 1 author:details FILTER 'startswith(@name, \"G\")' SCHEMA name TEXT"
        );

        /*
        Index only books that have a subtitle.
        FT.CREATE subtitled-books-idx ON HASH PREFIX 1 book:details FILTER '@subtitle != ""' SCHEMA title TEXT
        */
        let ft_create = FtCreateCommand::new("subtitled-books-idx")
            .options(
                CreateOptions::new()
                    .on(IndexDataType::Hash)
                    .prefix("book:details")
                    .filter("@subtitle != \"\""),
            )
            .schema(schema! {
                "title" =>  SchemaTextField::new(),
            });

        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE subtitled-books-idx ON HASH PREFIX 1 book:details FILTER '@subtitle != \"\"' SCHEMA title TEXT"
        );
    }

    #[test]
    fn test_index_with_separator() {
        /*
        In this example, keys for author data use the key pattern author:details:<id> while keys for book data use the pattern book:details:<id>.
        Index books that have a "categories" attribute where each category is separated by a ; character.
        FT.CREATE books-idx ON HASH PREFIX 1 book:details SCHEMA title TEXT categories TAG SEPARATOR ;
        */
        let ft_create = FtCreateCommand::new("books-idx")
            .options(
                CreateOptions::new()
                    .on(IndexDataType::Hash)
                    .prefix("book:details"),
            )
            .schema(schema! {
                "title" =>  SchemaTextField::new(),
                "categories" => SchemaTagField::new().separator(';'),
            });

        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE books-idx ON HASH PREFIX 1 book:details SCHEMA title TEXT categories TAG SEPARATOR ;"
        );
    }

    #[test]
    fn test_index_json() {
        /*
        Index a JSON document using a JSON Path expression
        The following example uses data similar to the hash examples above but uses JSON instead.
        FT.CREATE idx ON JSON SCHEMA $.title AS title TEXT $.categories AS categories TAG
        */
        let ft_create = FtCreateCommand::new("idx")
            .options(CreateOptions::new().on(IndexDataType::Json))
            .schema(schema! {
                "$.title" =>  SchemaTextField::new().alias("title"),
                "$.categories" => SchemaTagField::new().alias("categories"),
            });

        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE idx ON JSON SCHEMA $.title AS title TEXT $.categories AS categories TAG"
        );
    }
}
