#![cfg(feature = "search_unfinished")]

mod support;
use crate::support::*;
use redis::Commands;
use redis::schema;
use redis::search::*;
use redis_test::server::Module;

static TEXT_FIELD_NAME: &str = "title";
static NUMERIC_FIELD_NAME: &str = "price";
static TAG_FIELD_NAME: &str = "condition";
static GEO_FIELD_NAME: &str = "location";
static GEOSHAPE_FIELD_NAME: &str = "area";

fn assert_no_index_and_index_missing_exclusivity_for_field(
    result: redis::RedisResult<String>,
    field_name: &str,
) {
    let server_error = redis::ServerError::try_from(result.unwrap_err()).unwrap();
    assert!(server_error.details().is_some_and(|details| {
        details.contains(
            format!("cannot be defined with both `NOINDEX` and `INDEXMISSING` `{field_name}`")
                .as_str(),
        )
    }));
}

#[test]
fn test_module_search_ft_create_with_an_empty_index_name() {
    let ctx = run_test_if_version_supported!(
        [&[REDIS_CE_8_0][..], &[REDIS_SEARCH_8_0]],
        &[Module::Search]
    );
    let mut con = ctx.connection();
    let empty_index_name = "";
    let options = CreateOptions::new();
    let schema = schema! {
        TEXT_FIELD_NAME => SchemaTextField::new()
    };
    // Check that the first call succeeds but the second one fails because the index already exists
    assert_eq!(
        con.ft_create(empty_index_name, &options, &schema),
        Ok("OK".to_string())
    );
    con.ft_create::<_, String>(empty_index_name, &options, &schema)
        .unwrap_err();
}

fn run_simple_ft_create<C, F>(con: &mut C, index_name: &str, mut on_created: F)
where
    C: redis::ConnectionLike,
    F: FnMut(&str),
{
    let options = CreateOptions::new();
    let schema = schema! {
        TEXT_FIELD_NAME => SchemaTextField::new()
    };
    // Check that the first call succeeds but the second one fails because the index already exists
    assert_eq!(
        con.ft_create(index_name, &options, &schema),
        Ok("OK".to_string())
    );
    on_created(index_name);
    con.ft_create::<_, String>(index_name, &options, &schema)
        .unwrap_err();
}

#[test]
fn test_module_search_simple_ft_create() {
    let ctx = run_test_if_version_supported!(
        [&[REDIS_CE_8_0][..], &[REDIS_SEARCH_8_0]],
        &[Module::Search]
    );
    run_simple_ft_create(&mut ctx.connection(), "index", |_| {});
}

#[test]
fn test_module_search_ft_create_create_options() {
    let ctx = run_test_if_version_supported!(
        [&[REDIS_CE_8_0][..], &[REDIS_SEARCH_8_0]],
        &[Module::Search]
    );
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
        ("no_offsets", |opts| opts.no_offsets()),
        ("temporary", |opts| opts.temporary(1)),
        ("no_highlight", |opts| opts.no_highlight()),
        ("no_freqs", |opts| opts.no_freqs()),
        ("single_stopword", |opts| opts.stopword("stopword1")),
        ("multiple_stopwords", |opts| {
            opts.stopword("stopword2").stopword("stopword3")
        }),
        ("skip_initial_scan", |opts| opts.skip_initial_scan()),
    ];

    // `max_text_fields` (MAXTEXTFIELDS) and `no_fields` (NOFIELDS) are mutually exclusive on
    // newer versions of RediSearch, so they are shouldn't be combined with each other.
    let mutually_exclusive_modifiers: Vec<(&'static str, CreateOptionsModifier)> = vec![
        ("max_text_fields", |opts| opts.max_text_fields()),
        ("no_fields", |opts| opts.no_fields()),
    ];

    // Test each option individually
    for (suffix, modifier) in option_modifiers.iter().chain(&mutually_exclusive_modifiers) {
        let index_name = format!("index_with_{suffix}");
        let options = modifier(CreateOptions::new());

        assert_eq!(
            con.ft_create(&index_name, &options, &schema),
            Ok("OK".to_string())
        );
    }

    // Combine all non-mutually-exclusive options cumulatively
    let mut combined_options = CreateOptions::new();
    for (suffix, modifier) in &option_modifiers {
        let combined_index_name = format!("combined_index_until_{suffix}");
        combined_options = modifier(combined_options);

        assert_eq!(
            con.ft_create(&combined_index_name, &combined_options, &schema),
            Ok("OK".to_string())
        );
    }

    // Add each mutually exclusive option onto the fully combined base individually
    for (suffix, modifier) in &mutually_exclusive_modifiers {
        let combined_index_name = format!("combined_index_with_{suffix}");
        let options = modifier(combined_options.clone());

        assert_eq!(
            con.ft_create(&combined_index_name, &options, &schema),
            Ok("OK".to_string())
        );
    }
}

fn run_ft_create_schema_text_field<C, F>(con: &mut C, mut on_created: F)
where
    C: redis::ConnectionLike,
    F: FnMut(&str),
{
    type SchemaTextFieldModifier = fn(SchemaTextField) -> SchemaTextField;
    let field_modifiers: Vec<(&'static str, SchemaTextFieldModifier)> = vec![
        // Common modifiers
        ("alias", |field| field.alias("text_alias")),
        ("sortable", |field| field.sortable(Sortable::Yes)),
        ("sortable_unf", |field| field.sortable(Sortable::Unf)),
        // Text field modifiers
        ("no_stem", |field| field.no_stem(true)),
        ("weight", |field| field.weight(1.0)),
        ("phonetic", |field| field.phonetic(Phonetic::DmEnglish)),
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
        let index_name = format!("index_for_text_field_with_{suffix}");
        let schema = schema! {
            TEXT_FIELD_NAME => modifier(SchemaTextField::new())
        };
        assert_eq!(
            con.ft_create(&index_name, &CreateOptions::new(), &schema),
            Ok("OK".to_string())
        );
        on_created(&index_name);
    }

    // Test each mutually exclusive modifier individually
    for (suffix, modifier) in &mutually_exclusive_common_modifiers {
        let index_name = format!("index_for_text_field_with_{suffix}");
        let schema = schema! {
            TEXT_FIELD_NAME => modifier(SchemaTextField::new())
        };
        assert_eq!(
            con.ft_create(&index_name, &CreateOptions::new(), &schema),
            Ok("OK".to_string())
        );
        on_created(&index_name);
    }

    // Test all combinations of field modifiers that are not mutually exclusive
    let mut combined_schema_text_field = SchemaTextField::new();
    for (suffix, modifier) in &field_modifiers {
        let combined_index_name = format!("index_for_text_field_combined_until_{suffix}");
        combined_schema_text_field = modifier(combined_schema_text_field);
        let schema = schema! {
            TEXT_FIELD_NAME => combined_schema_text_field.clone()
        };
        assert_eq!(
            con.ft_create(&combined_index_name, &CreateOptions::new(), &schema),
            Ok("OK".to_string())
        );
        on_created(&combined_index_name);
    }

    // After all of the modifiers above have been applied, add each of the mutually exclusive modifiers individually
    for (suffix, modifier) in &mutually_exclusive_common_modifiers {
        let combined_index_name = format!("index_for_text_field_all_combined_with_{suffix}");
        let schema = schema! {
            TEXT_FIELD_NAME =>  modifier(combined_schema_text_field.clone())
        };
        assert_eq!(
            con.ft_create(&combined_index_name, &CreateOptions::new(), &schema),
            Ok("OK".to_string())
        );
        on_created(&combined_index_name);
    }
    // Test that mutually exclusive modifiers are mutually exclusive indeed
    assert_no_index_and_index_missing_exclusivity_for_field(
        con.ft_create::<_, String>(
            "invalid_index",
            &CreateOptions::new(),
            &schema! {
                TEXT_FIELD_NAME => SchemaTextField::new().no_index(true).index_missing(true)
            },
        ),
        TEXT_FIELD_NAME,
    );
}

#[test]
fn test_module_search_ft_create_schema_text_field() {
    let ctx = run_test_if_version_supported!(
        [&[REDIS_CE_8_0][..], &[REDIS_SEARCH_8_0]],
        &[Module::Search]
    );
    run_ft_create_schema_text_field(&mut ctx.connection(), |_| {});
}

fn run_ft_create_schema_tag_field<C, F>(con: &mut C, mut on_created: F)
where
    C: redis::ConnectionLike,
    F: FnMut(&str),
{
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
        let index_name = format!("index_for_tag_field_with_{suffix}");
        let schema = schema! {
            TAG_FIELD_NAME => modifier(SchemaTagField::new())
        };
        assert_eq!(
            con.ft_create(&index_name, &CreateOptions::new(), &schema),
            Ok("OK".to_string())
        );
        on_created(&index_name);
    }

    // Test each mutually exclusive modifier individually
    for (suffix, modifier) in &mutually_exclusive_common_modifiers {
        let index_name = format!("index_for_tag_field_with_{suffix}");
        let schema = schema! {
            TAG_FIELD_NAME => modifier(SchemaTagField::new())
        };
        assert_eq!(
            con.ft_create(&index_name, &CreateOptions::new(), &schema),
            Ok("OK".to_string())
        );
        on_created(&index_name);
    }

    // Test all combinations of field modifiers that are not mutually exclusive
    let mut combined_schema_tag_field = SchemaTagField::new();
    for (suffix, modifier) in &field_modifiers {
        let combined_index_name = format!("index_for_tag_field_combined_until_{suffix}");
        combined_schema_tag_field = modifier(combined_schema_tag_field);
        let schema = schema! {
            TAG_FIELD_NAME => combined_schema_tag_field.clone()
        };
        assert_eq!(
            con.ft_create(&combined_index_name, &CreateOptions::new(), &schema),
            Ok("OK".to_string())
        );
        on_created(&combined_index_name);
    }

    // After all of the modifiers above have been applied, add each of the mutually exclusive modifiers individually
    for (suffix, modifier) in &mutually_exclusive_common_modifiers {
        let combined_index_name = format!("index_for_tag_field_all_combined_with_{suffix}");
        let schema = schema! {
            TAG_FIELD_NAME =>  modifier(combined_schema_tag_field.clone())
        };
        assert_eq!(
            con.ft_create(&combined_index_name, &CreateOptions::new(), &schema),
            Ok("OK".to_string())
        );
        on_created(&combined_index_name);
    }
    // Test that mutually exclusive modifiers are mutually exclusive indeed
    assert_no_index_and_index_missing_exclusivity_for_field(
        con.ft_create::<_, String>(
            "invalid_index",
            &CreateOptions::new(),
            &schema! {
                TAG_FIELD_NAME => SchemaTagField::new().no_index(true).index_missing(true)
            },
        ),
        TAG_FIELD_NAME,
    );
}

#[test]
fn test_module_search_ft_create_schema_tag_field() {
    let ctx = run_test_if_version_supported!(
        [&[REDIS_CE_8_0][..], &[REDIS_SEARCH_8_0]],
        &[Module::Search]
    );
    run_ft_create_schema_tag_field(&mut ctx.connection(), |_| {});
}

fn run_ft_create_schema_numeric_field<C, F>(con: &mut C, mut on_created: F)
where
    C: redis::ConnectionLike,
    F: FnMut(&str),
{
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
        let index_name = format!("index_for_numeric_field_with_{suffix}");
        let schema = schema! {
            NUMERIC_FIELD_NAME => modifier(SchemaNumericField::new())
        };
        assert_eq!(
            con.ft_create(&index_name, &CreateOptions::new(), &schema),
            Ok("OK".to_string())
        );
        on_created(&index_name);
    }

    // Test each mutually exclusive modifier individually
    for (suffix, modifier) in &mutually_exclusive_common_modifiers {
        let index_name = format!("index_for_numeric_field_with_{suffix}");
        let schema = schema! {
            NUMERIC_FIELD_NAME => modifier(SchemaNumericField::new())
        };
        assert_eq!(
            con.ft_create(&index_name, &CreateOptions::new(), &schema),
            Ok("OK".to_string())
        );
        on_created(&index_name);
    }

    // Test all combinations of field modifiers that are not mutually exclusive
    let mut combined_schema_numeric_field = SchemaNumericField::new();
    for (suffix, modifier) in &field_modifiers {
        let combined_index_name = format!("index_for_numeric_field_combined_until_{suffix}");
        combined_schema_numeric_field = modifier(combined_schema_numeric_field);
        let schema = schema! {
            NUMERIC_FIELD_NAME => combined_schema_numeric_field.clone()
        };
        assert_eq!(
            con.ft_create(&combined_index_name, &CreateOptions::new(), &schema),
            Ok("OK".to_string())
        );
        on_created(&combined_index_name);
    }

    // After all of the modifiers above have been applied, add each of the mutually exclusive modifiers individually
    for (suffix, modifier) in &mutually_exclusive_common_modifiers {
        let combined_index_name = format!("index_for_numeric_field_all_combined_with_{suffix}");
        let schema = schema! {
            NUMERIC_FIELD_NAME =>  modifier(combined_schema_numeric_field.clone())
        };
        assert_eq!(
            con.ft_create(&combined_index_name, &CreateOptions::new(), &schema),
            Ok("OK".to_string())
        );
        on_created(&combined_index_name);
    }
    // Test that mutually exclusive modifiers are mutually exclusive indeed
    assert_no_index_and_index_missing_exclusivity_for_field(
        con.ft_create::<_, String>(
            "invalid_index",
            &CreateOptions::new(),
            &schema! {
                NUMERIC_FIELD_NAME => SchemaNumericField::new().no_index(true).index_missing(true)
            },
        ),
        NUMERIC_FIELD_NAME,
    );
}

#[test]
fn test_module_search_ft_create_schema_numeric_field() {
    let ctx = run_test_if_version_supported!(
        [&[REDIS_CE_8_0][..], &[REDIS_SEARCH_8_0]],
        &[Module::Search]
    );
    run_ft_create_schema_numeric_field(&mut ctx.connection(), |_| {});
}

fn run_ft_create_schema_geo_field<C, F>(con: &mut C, mut on_created: F)
where
    C: redis::ConnectionLike,
    F: FnMut(&str),
{
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
        let index_name = format!("index_for_geo_field_with_{suffix}");
        let schema = schema! {
            GEO_FIELD_NAME => modifier(SchemaGeoField::new())
        };
        assert_eq!(
            con.ft_create(&index_name, &CreateOptions::new(), &schema),
            Ok("OK".to_string())
        );
        on_created(&index_name);
    }

    // Test each mutually exclusive modifier individually
    for (suffix, modifier) in &mutually_exclusive_common_modifiers {
        let index_name = format!("index_for_geo_field_with_{suffix}");
        let schema = schema! {
            GEO_FIELD_NAME => modifier(SchemaGeoField::new())
        };
        assert_eq!(
            con.ft_create(&index_name, &CreateOptions::new(), &schema),
            Ok("OK".to_string())
        );
        on_created(&index_name);
    }

    // Test all combinations of field modifiers that are not mutually exclusive
    let mut combined_schema_geo_field = SchemaGeoField::new();
    for (suffix, modifier) in &field_modifiers {
        let combined_index_name = format!("index_for_geo_field_combined_until_{suffix}");
        combined_schema_geo_field = modifier(combined_schema_geo_field);
        let schema = schema! {
            GEO_FIELD_NAME => combined_schema_geo_field.clone()
        };
        assert_eq!(
            con.ft_create(&combined_index_name, &CreateOptions::new(), &schema),
            Ok("OK".to_string())
        );
        on_created(&combined_index_name);
    }

    // After all of the modifiers above have been applied, add each of the mutually exclusive modifiers individually
    for (suffix, modifier) in &mutually_exclusive_common_modifiers {
        let combined_index_name = format!("index_for_geo_field_all_combined_with_{suffix}");
        let schema = schema! {
            GEO_FIELD_NAME =>  modifier(combined_schema_geo_field.clone())
        };
        assert_eq!(
            con.ft_create(&combined_index_name, &CreateOptions::new(), &schema),
            Ok("OK".to_string())
        );
        on_created(&combined_index_name);
    }
    // Test that mutually exclusive modifiers are mutually exclusive indeed
    assert_no_index_and_index_missing_exclusivity_for_field(
        con.ft_create::<_, String>(
            "invalid_index",
            &CreateOptions::new(),
            &schema! {
                GEO_FIELD_NAME => SchemaGeoField::new().no_index(true).index_missing(true)
            },
        ),
        GEO_FIELD_NAME,
    );
}

#[test]
fn test_module_search_ft_create_schema_geo_field() {
    let ctx = run_test_if_version_supported!(
        [&[REDIS_CE_8_0][..], &[REDIS_SEARCH_8_0]],
        &[Module::Search]
    );
    run_ft_create_schema_geo_field(&mut ctx.connection(), |_| {});
}

fn run_ft_create_schema_geoshape_field<C, F>(con: &mut C, mut on_created: F)
where
    C: redis::ConnectionLike,
    F: FnMut(&str),
{
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
            let index_name = format!("index_for_geoshape_{coord_system:?}_field_with_{suffix}");
            let schema = schema! {
                GEOSHAPE_FIELD_NAME => modifier(SchemaGeoShapeField::new().coord_system(coord_system.clone()))
            };
            assert_eq!(
                con.ft_create(&index_name, &CreateOptions::new(), &schema),
                Ok("OK".to_string())
            );
            on_created(&index_name);
        }

        // Test each mutually exclusive modifier individually
        for (suffix, modifier) in &mutually_exclusive_common_modifiers {
            let index_name = format!("index_for_geoshape_{coord_system:?}_field_with_{suffix}");
            let schema = schema! {
                GEOSHAPE_FIELD_NAME => modifier(SchemaGeoShapeField::new().coord_system(coord_system.clone()))
            };
            assert_eq!(
                con.ft_create(&index_name, &CreateOptions::new(), &schema),
                Ok("OK".to_string())
            );
            on_created(&index_name);
        }

        // Test all modifiers that are not mutually exclusive
        let mut combined_field = SchemaGeoShapeField::new().coord_system(coord_system.clone());
        for (_suffix, modifier) in &field_modifiers {
            combined_field = modifier(combined_field);
        }
        let combined_index_name = format!("index_for_geoshape_{coord_system:?}_field_all_combined");
        let schema = schema! {
            GEOSHAPE_FIELD_NAME => combined_field.clone()
        };
        assert_eq!(
            con.ft_create(&combined_index_name, &CreateOptions::new(), &schema),
            Ok("OK".to_string())
        );
        on_created(&combined_index_name);

        // After all of the modifiers above have been applied, add each of the mutually exclusive modifiers individually
        for (suffix, modifier) in &mutually_exclusive_common_modifiers {
            let combined_index_name =
                format!("index_for_geoshape_{coord_system:?}_field_all_combined_with_{suffix}");
            let schema = schema! {
                GEOSHAPE_FIELD_NAME =>  modifier(combined_field.clone())
            };
            assert_eq!(
                con.ft_create(&combined_index_name, &CreateOptions::new(), &schema),
                Ok("OK".to_string())
            );
            on_created(&combined_index_name);
        }

        // Test that mutually exclusive modifiers are mutually exclusive indeed
        assert_no_index_and_index_missing_exclusivity_for_field(
            con.ft_create::<_, String>(
                "invalid_index",
                &CreateOptions::new(),
                &schema! {
                    GEOSHAPE_FIELD_NAME => SchemaGeoShapeField::new().coord_system(coord_system.clone()).no_index(true).index_missing(true)
                }
            ),
            GEOSHAPE_FIELD_NAME,
        );
    }
}

#[test]
fn test_module_search_ft_create_schema_geoshape_field() {
    let ctx = run_test_if_version_supported!(
        [&[REDIS_CE_8_0][..], &[REDIS_SEARCH_8_0]],
        &[Module::Search]
    );
    run_ft_create_schema_geoshape_field(&mut ctx.connection(), |_| {});
}
