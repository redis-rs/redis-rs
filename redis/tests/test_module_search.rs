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

/// One modifier applies a single setting to a field (or to the create options) and returns it.
type Modifier<T> = fn(T) -> T;

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

fn assert_index_already_exists_error(result: redis::RedisResult<String>) {
    let server_error = redis::ServerError::try_from(result.unwrap_err()).unwrap();
    assert!(
        server_error
            .details()
            .is_some_and(|details| details.contains("already exists"))
    );
}

/// Build a single-field index whose field carries both `NOINDEX` and `INDEXMISSING`,
/// then assert the server rejects it as a conflict.
fn assert_no_index_and_index_missing_conflict<C, T>(con: &mut C, field_name: &str, field: T)
where
    C: redis::ConnectionLike,
    T: Into<FieldDefinition>,
{
    let result = con.ft_create::<_, String>(
        "invalid_index",
        &CreateOptions::new(),
        &SearchSchema::new(field_name, field),
    );
    assert_no_index_and_index_missing_exclusivity_for_field(result, field_name);
}

/// Drive the FT.CREATE modifier matrix shared by the create-options test and every field type.
///
/// It runs these blocks in order:
///   1. each portable modifier on its own,
///   2. the portable modifiers combined cumulatively,
///   3. (Redis only) each Redis-only and mutually-exclusive modifier on its own,
///   4. (Redis only) the Redis-only modifiers added onto the combined base cumulatively,
///   5. (Redis only) each mutually-exclusive modifier added onto the fully combined base.
///
/// valkey-search rejects everything past block 2, so the run stops there when `is_valkey`.
/// `base` builds a fresh value, `create` runs FT.CREATE for one accumulated value, and
/// `on_created` receives every created index name (the clustered tests use it to check
/// propagation).
#[allow(clippy::too_many_arguments)]
fn run_modifier_matrix<C, T>(
    con: &mut C,
    index_prefix: &str,
    base: impl Fn() -> T,
    portable: &[(&str, Modifier<T>)],
    redis_only: &[(&str, Modifier<T>)],
    mutually_exclusive: &[(&str, Modifier<T>)],
    is_valkey: bool,
    mut on_created: impl FnMut(&str),
    create: impl Fn(&mut C, &str, &T) -> redis::RedisResult<String>,
) where
    C: redis::ConnectionLike,
    T: Clone,
{
    // 1. Each portable modifier on a fresh base.
    for (suffix, modifier) in portable {
        let index_name = format!("index_for_{index_prefix}_with_{suffix}");
        assert_eq!(
            create(con, &index_name, &modifier(base())),
            Ok("OK".to_string())
        );
        on_created(&index_name);
    }

    // 2. Portable modifiers combined cumulatively.
    let mut combined = base();
    for (suffix, modifier) in portable {
        let index_name = format!("index_for_{index_prefix}_combined_until_{suffix}");
        combined = modifier(combined);
        assert_eq!(create(con, &index_name, &combined), Ok("OK".to_string()));
        on_created(&index_name);
    }

    // valkey-search rejects everything below, so stop here for it.
    if is_valkey {
        return;
    }

    // 3. Each Redis-only and mutually-exclusive modifier on a fresh base.
    for (suffix, modifier) in redis_only.iter().chain(mutually_exclusive) {
        let index_name = format!("index_for_{index_prefix}_with_{suffix}");
        assert_eq!(
            create(con, &index_name, &modifier(base())),
            Ok("OK".to_string())
        );
        on_created(&index_name);
    }

    // 4. Redis-only modifiers added onto the combined base cumulatively.
    for (suffix, modifier) in redis_only {
        let index_name = format!("index_for_{index_prefix}_combined_until_{suffix}");
        combined = modifier(combined);
        assert_eq!(create(con, &index_name, &combined), Ok("OK".to_string()));
        on_created(&index_name);
    }

    // 5. Each mutually-exclusive modifier added onto the fully combined base.
    for (suffix, modifier) in mutually_exclusive {
        let index_name = format!("index_for_{index_prefix}_all_combined_with_{suffix}");
        assert_eq!(
            create(con, &index_name, &modifier(combined.clone())),
            Ok("OK".to_string())
        );
        on_created(&index_name);
    }
}

// Basic create — Redis and Valkey.

#[test]
fn test_module_search_ft_create_with_an_empty_index_name() {
    let ctx = run_test_if_version_supported!(
        &[REDIS_SEARCH_8_0, VALKEY_SEARCH_ANY][..],
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
    assert_index_already_exists_error(con.ft_create::<_, String>(
        empty_index_name,
        &options,
        &schema,
    ));
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
    assert_index_already_exists_error(con.ft_create::<_, String>(index_name, &options, &schema));
}

#[test]
fn test_module_search_simple_ft_create() {
    let ctx = run_test_if_version_supported!(
        &[REDIS_SEARCH_8_0, VALKEY_SEARCH_ANY][..],
        &[Module::Search]
    );
    run_simple_ft_create(&mut ctx.connection(), "index", |_| {});
}

// FT.CREATE create options and per-field-type schema coverage.

#[test]
fn test_module_search_ft_create_create_options() {
    // Portable options on both servers; the full option matrix (e.g. FILTER,
    // TEMPORARY, MAXTEXTFIELDS) on Redis only, as valkey-search rejects it.
    let ctx = run_test_if_version_supported!(
        &[REDIS_SEARCH_8_0, VALKEY_SEARCH_ANY][..],
        &[Module::Search]
    );
    let is_valkey = ctx.supports(VALKEY_SEARCH_ANY);
    let mut con = ctx.connection();
    let schema = schema! {
        TEXT_FIELD_NAME => SchemaTextField::new()
    };

    // Portable options — accepted by both Redis and valkey-search.
    let portable: Vec<(&'static str, Modifier<CreateOptions>)> = vec![
        ("on_hash", |opts| opts.on(IndexDataType::Hash)),
        ("single_prefix", |opts| opts.prefix("pref1")),
        ("multiple_prefixes", |opts| {
            opts.prefix("pref2").prefix("pref3")
        }),
        ("language", |opts| opts.language(SearchLanguage::English)),
        ("score", |opts| opts.score(1.0)),
        ("no_offsets", |opts| opts.no_offsets()),
        ("single_stopword", |opts| opts.stopword("stopword1")),
        ("multiple_stopwords", |opts| {
            opts.stopword("stopword2").stopword("stopword3")
        }),
        ("skip_initial_scan", |opts| opts.skip_initial_scan()),
    ];

    // Redis-only options.
    let redis_only: Vec<(&'static str, Modifier<CreateOptions>)> = vec![
        ("filter", |opts| opts.filter("@field: value")),
        ("language_field", |opts| {
            opts.language_field("language_field")
        }),
        ("score_field", |opts| opts.score_field("score_field")),
        ("temporary", |opts| opts.temporary(1)),
        ("no_highlight", |opts| opts.no_highlight()),
        ("no_freqs", |opts| opts.no_freqs()),
    ];

    // `max_text_fields` (MAXTEXTFIELDS) and `no_fields` (NOFIELDS) are mutually exclusive on
    // newer versions of RediSearch, so they shouldn't be combined with each other.
    let mutually_exclusive: Vec<(&'static str, Modifier<CreateOptions>)> = vec![
        ("max_text_fields", |opts| opts.max_text_fields()),
        ("no_fields", |opts| opts.no_fields()),
    ];

    run_modifier_matrix(
        &mut con,
        "options",
        CreateOptions::new,
        &portable,
        &redis_only,
        &mutually_exclusive,
        is_valkey,
        |_| {},
        |con, index_name, options: &CreateOptions| con.ft_create(index_name, options, &schema),
    );
}

fn run_ft_create_schema_text_field<C, F>(con: &mut C, is_valkey: bool, on_created: F)
where
    C: redis::ConnectionLike,
    F: FnMut(&str),
{
    // Portable modifiers — accepted by both Redis and valkey-search.
    let portable: Vec<(&'static str, Modifier<SchemaTextField>)> = vec![
        ("alias", |field| field.alias("text_alias")),
        ("sortable", |field| field.sortable(Sortable::Yes)),
        ("no_stem", |field| field.no_stem(true)),
        ("weight", |field| field.weight(1.0)),
        ("with_suffix_trie", |field| field.with_suffix_trie(true)),
    ];

    // Redis-only modifiers.
    let redis_only: Vec<(&'static str, Modifier<SchemaTextField>)> = vec![
        ("sortable_unf", |field| field.sortable(Sortable::Unf)),
        ("phonetic", |field| field.phonetic(Phonetic::DmEnglish)),
        ("index_empty", |field| field.index_empty(true)),
    ];

    // Redis-only modifiers that are mutually exclusive.
    let mutually_exclusive: Vec<(&'static str, Modifier<SchemaTextField>)> = vec![
        ("index_missing", |field| field.index_missing(true)),
        ("no_index", |field| field.no_index(true)),
    ];

    run_modifier_matrix(
        con,
        "text_field",
        SchemaTextField::new,
        &portable,
        &redis_only,
        &mutually_exclusive,
        is_valkey,
        on_created,
        |con, index_name, field: &SchemaTextField| {
            con.ft_create(
                index_name,
                &CreateOptions::new(),
                &SearchSchema::new(TEXT_FIELD_NAME, field.clone()),
            )
        },
    );

    // valkey-search never reaches the Redis-only modifiers, so only check the conflict on Redis.
    if !is_valkey {
        assert_no_index_and_index_missing_conflict(
            con,
            TEXT_FIELD_NAME,
            SchemaTextField::new().no_index(true).index_missing(true),
        );
    }
}

#[test]
fn test_module_search_ft_create_schema_text_field() {
    // Portable subset on both servers; the full matrix (e.g. SORTABLE UNF,
    // PHONETIC, INDEXMISSING) on Redis only, as valkey-search rejects it.
    let ctx = run_test_if_version_supported!(
        &[REDIS_SEARCH_8_0, VALKEY_SEARCH_ANY][..],
        &[Module::Search]
    );
    let is_valkey = ctx.supports(VALKEY_SEARCH_ANY);
    run_ft_create_schema_text_field(&mut ctx.connection(), is_valkey, |_| {});
}

fn run_ft_create_schema_tag_field<C, F>(con: &mut C, is_valkey: bool, on_created: F)
where
    C: redis::ConnectionLike,
    F: FnMut(&str),
{
    // Portable modifiers — accepted by both Redis and valkey-search.
    let portable: Vec<(&'static str, Modifier<SchemaTagField>)> = vec![
        ("alias", |field| field.alias("tag_alias")),
        ("sortable", |field| field.sortable(Sortable::Yes)),
        ("separator", |field| field.separator(',')),
        ("case_sensitive", |field| field.case_sensitive(true)),
    ];

    // Redis-only modifiers.
    let redis_only: Vec<(&'static str, Modifier<SchemaTagField>)> = vec![
        ("sortable_unf", |field| field.sortable(Sortable::Unf)),
        ("with_suffix_trie", |field| field.with_suffix_trie(true)),
        ("index_empty", |field| field.index_empty(true)),
    ];

    // Redis-only modifiers that are mutually exclusive.
    let mutually_exclusive: Vec<(&'static str, Modifier<SchemaTagField>)> = vec![
        ("index_missing", |field| field.index_missing(true)),
        ("no_index", |field| field.no_index(true)),
    ];

    run_modifier_matrix(
        con,
        "tag_field",
        SchemaTagField::new,
        &portable,
        &redis_only,
        &mutually_exclusive,
        is_valkey,
        on_created,
        |con, index_name, field: &SchemaTagField| {
            con.ft_create(
                index_name,
                &CreateOptions::new(),
                &SearchSchema::new(TAG_FIELD_NAME, field.clone()),
            )
        },
    );

    // valkey-search never reaches the Redis-only modifiers, so only check the conflict on Redis.
    if !is_valkey {
        assert_no_index_and_index_missing_conflict(
            con,
            TAG_FIELD_NAME,
            SchemaTagField::new().no_index(true).index_missing(true),
        );
    }
}

#[test]
fn test_module_search_ft_create_schema_tag_field() {
    // Portable subset on both servers; the full matrix (e.g. SORTABLE UNF,
    // WITHSUFFIXTRIE, INDEXMISSING) on Redis only, as valkey-search rejects it.
    let ctx = run_test_if_version_supported!(
        &[REDIS_SEARCH_8_0, VALKEY_SEARCH_ANY][..],
        &[Module::Search]
    );
    let is_valkey = ctx.supports(VALKEY_SEARCH_ANY);
    run_ft_create_schema_tag_field(&mut ctx.connection(), is_valkey, |_| {});
}

fn run_ft_create_schema_numeric_field<C, F>(con: &mut C, is_valkey: bool, on_created: F)
where
    C: redis::ConnectionLike,
    F: FnMut(&str),
{
    // Portable modifiers — accepted by both Redis and valkey-search.
    let portable: Vec<(&'static str, Modifier<SchemaNumericField>)> = vec![
        ("alias", |field| field.alias("numeric_alias")),
        ("sortable", |field| field.sortable(Sortable::Yes)),
    ];

    // Redis-only modifiers.
    let redis_only: Vec<(&'static str, Modifier<SchemaNumericField>)> =
        vec![("sortable_unf", |field| field.sortable(Sortable::Unf))];

    // Redis-only modifiers that are mutually exclusive.
    let mutually_exclusive: Vec<(&'static str, Modifier<SchemaNumericField>)> = vec![
        ("index_missing", |field| field.index_missing(true)),
        ("no_index", |field| field.no_index(true)),
    ];

    run_modifier_matrix(
        con,
        "numeric_field",
        SchemaNumericField::new,
        &portable,
        &redis_only,
        &mutually_exclusive,
        is_valkey,
        on_created,
        |con, index_name, field: &SchemaNumericField| {
            con.ft_create(
                index_name,
                &CreateOptions::new(),
                &SearchSchema::new(NUMERIC_FIELD_NAME, field.clone()),
            )
        },
    );

    // valkey-search never reaches the Redis-only modifiers, so only check the conflict on Redis.
    if !is_valkey {
        assert_no_index_and_index_missing_conflict(
            con,
            NUMERIC_FIELD_NAME,
            SchemaNumericField::new().no_index(true).index_missing(true),
        );
    }
}

#[test]
fn test_module_search_ft_create_schema_numeric_field() {
    // Portable subset on both servers; the full matrix (e.g. SORTABLE UNF,
    // INDEXMISSING) on Redis only, as valkey-search rejects it.
    let ctx = run_test_if_version_supported!(
        &[REDIS_SEARCH_8_0, VALKEY_SEARCH_ANY][..],
        &[Module::Search]
    );
    let is_valkey = ctx.supports(VALKEY_SEARCH_ANY);
    run_ft_create_schema_numeric_field(&mut ctx.connection(), is_valkey, |_| {});
}

fn run_ft_create_schema_geo_field<C, F>(con: &mut C, on_created: F)
where
    C: redis::ConnectionLike,
    F: FnMut(&str),
{
    // GEO is Redis-only, so every modifier runs on Redis (`is_valkey` is always false here).
    let field_modifiers: Vec<(&'static str, Modifier<SchemaGeoField>)> = vec![
        ("alias", |field| field.alias("geo_alias")),
        ("sortable", |field| field.sortable(Sortable::Yes)),
        ("sortable_unf", |field| field.sortable(Sortable::Unf)),
    ];

    // Modifiers that are mutually exclusive.
    let mutually_exclusive: Vec<(&'static str, Modifier<SchemaGeoField>)> = vec![
        ("index_missing", |field| field.index_missing(true)),
        ("no_index", |field| field.no_index(true)),
    ];

    run_modifier_matrix(
        con,
        "geo_field",
        SchemaGeoField::new,
        &field_modifiers,
        &[],
        &mutually_exclusive,
        false,
        on_created,
        |con, index_name, field: &SchemaGeoField| {
            con.ft_create(
                index_name,
                &CreateOptions::new(),
                &SearchSchema::new(GEO_FIELD_NAME, field.clone()),
            )
        },
    );

    assert_no_index_and_index_missing_conflict(
        con,
        GEO_FIELD_NAME,
        SchemaGeoField::new().no_index(true).index_missing(true),
    );
}

#[test]
fn test_module_search_ft_create_schema_geo_field() {
    // Redis-only: valkey-search has no GEO field type.
    let ctx = run_test_if_version_supported!(REDIS_SEARCH_8_0, &[Module::Search]);
    run_ft_create_schema_geo_field(&mut ctx.connection(), |_| {});
}

fn run_ft_create_schema_geoshape_field<C, F>(con: &mut C, mut on_created: F)
where
    C: redis::ConnectionLike,
    F: FnMut(&str),
{
    // GEOSHAPE is Redis-only, so every modifier runs on Redis (`is_valkey` is always false here).
    let field_modifiers: Vec<(&'static str, Modifier<SchemaGeoShapeField>)> =
        vec![("alias", |field| field.alias("geo_shape_alias"))];

    // Modifiers that are mutually exclusive.
    let mutually_exclusive: Vec<(&'static str, Modifier<SchemaGeoShapeField>)> = vec![
        ("index_missing", |field| field.index_missing(true)),
        ("no_index", |field| field.no_index(true)),
    ];

    // Every index needs a coordinate system, so run the whole matrix once per system.
    for coord_system in &[CoordSystem::Spherical, CoordSystem::Flat] {
        let index_prefix = format!("geoshape_{coord_system:?}_field");
        run_modifier_matrix(
            con,
            &index_prefix,
            || SchemaGeoShapeField::new().coord_system(coord_system.clone()),
            &field_modifiers,
            &[],
            &mutually_exclusive,
            false,
            &mut on_created,
            |con, index_name, field: &SchemaGeoShapeField| {
                con.ft_create(
                    index_name,
                    &CreateOptions::new(),
                    &SearchSchema::new(GEOSHAPE_FIELD_NAME, field.clone()),
                )
            },
        );

        assert_no_index_and_index_missing_conflict(
            con,
            GEOSHAPE_FIELD_NAME,
            SchemaGeoShapeField::new()
                .coord_system(coord_system.clone())
                .no_index(true)
                .index_missing(true),
        );
    }
}

#[test]
fn test_module_search_ft_create_schema_geoshape_field() {
    // Redis-only: valkey-search has no GEOSHAPE field type.
    let ctx = run_test_if_version_supported!(REDIS_SEARCH_8_0, &[Module::Search]);
    run_ft_create_schema_geoshape_field(&mut ctx.connection(), |_| {});
}
