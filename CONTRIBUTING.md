# Contributing

🥳 Welcome and thank you for taking the time to contribute! 🥳

## Overview

Development happens on [GitHub](https://github.com/redis-rs/redis-rs), mostly through [issues](https://github.com/redis-rs/redis-rs/issues) and [pull requests](https://github.com/redis-rs/redis-rs/pulls).

* You have a question? Ask it on a [GitHub issue](https://github.com/redis-rs/redis-rs/issues/new)
* You found a bug? Report it as [GitHub issue](https://github.com/redis-rs/redis-rs/issues/new) (and if possible provide a fix as [pull request](https://github.com/redis-rs/redis-rs/compare) )
* You have an idea for a new feature? Describe it in a [GitHub issue](https://github.com/redis-rs/redis-rs/issues/new)
* There is something else? Reach out through a [GitHub issue](https://github.com/redis-rs/redis-rs/issues/new)

## Pull requests

If you believe your suggestion contains complexity or user-facing decisions that could go either way, start with discussing your idea on a [GitHub issue](https://github.com/redis-rs/redis-rs/issues/new) before working on a pull request.

Be sure to also read the [development docs](DEVELOPMENT.md) and [coding conventions](CODING_CONVENTIONS.md) before working on a pull request.

### Against which branch to open pull-requests (PRs)?

* _Backwards-compatible_ _changes_: Use `main` if you do not care about versioning.

  If you instead need your contribution to be contained in a given release, open your PR against the corresponding branch.

  E.g.: If you use `1.4.1` and have an addition/fix, open the PR it against `1.4.x`.
  This makes sure the next `1.4` release contains your fix.

* _Backwards-incompatible_ _changes_: Discuss in a [GitHub issue](https://github.com/redis-rs/redis-rs/issues/new) first.
  We'll point you towards the right branch if it exists, or open it if not.

### How to check CI before opening a PR?

It's ok to upload your PR to the repo and fix CI failures there.

But if you prefer to check before-hand, CI should be able to run in your fork, as `redis-rs` it does not rely on 3rd party integrations.

### How to check my code locally without GitHub

Running `make style-check lint test` should cover most ground if you did not mess with modules.

If `style-check` or `lint` fail, chances are that running `make fix` can save you some time.

The `test` part takes some time and relies on Redis being installed (`redis-server`, and `redis-cli` are in `$PATH`).

See the [development docs](DEVELOPMENT.md) for more details.

### Are AI contributions allowed?

This is not a firm policy, but, for the time being, PRs get evaluated on their
contents, not on who (or what) wrote them.
So AI contributions are allowed.

But having a conversation with an AI isn't always pleasant - it would be better if you answer PR comments yourself, instead of piping comments and responses.
