# Coding Conventions

## General

* All `struct`s with public members and `enum`s have to be marked as `#[non_exhaustive]`.

  (This helps with semantic versioning when extending the `struct`s/`enum`s)

## Redis Commands

* Don't add new options to existing Redis commands. Instead build an `_options` variant with a dedicated type that manages the options.
   E.g.: Instead of adding a third parameter to `foo(bar:..., baz:...)`, leave `foo` alone and instead add a new command `foo_options(bar:..., baz:..., options: FooOptions)`

* If a Rust argument should always translate into a single Redis argument, use `ToSingleRedisArg` instead of `ToRedisArgs`.

## Tests

* Don't use `.expect()`/`.expect_err()`. The contained messages don't contribute value. Use plain `unwrap()`/`unwrap_err()` instead.
