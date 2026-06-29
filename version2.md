# Announcing redis-rs & redis-test 2.0.0

With version 1.0.0 behind us, version 2.0.0 begins a new major series, with redis-test tracking it with the same version.

This document highlights the breaking changes in version 2.0.0. For a complete list of changes, see CHANGELOG.md. We appreciate feedback and bug reports — please open an issue for anything you encounter during migration. In order to get the newest version, please specify in your Cargo.toml file

```toml
redis = "2"
```

## Breaking Changes

### Removed `zinterstore_*` and `zunionstore_*` commands in favor of `zinterstore`, `zinterstore_with_weights`, `zunionstore`, and `zunionstore_with_weights`

The following commands have been removed:

- `zinterstore_min`, `zinterstore_max`
- `zinterstore_weights`, `zinterstore_min_weights`, `zinterstore_max_weights`
- `zunionstore_min`, `zunionstore_max`
- `zunionstore_weights`, `zunionstore_min_weights`, `zunionstore_max_weights`

Adding more options to these commands would have caused an exponential explosion of variants. Instead, there are now two variants for each command:

- `zinterstore(dstkey, keys, options)` / `zunionstore(dstkey, keys, options)` — keys without weights
- `zinterstore_with_weights(dstkey, keys_and_weights, options)` / `zunionstore_with_weights(dstkey, keys_and_weights, options)` — keys paired with weights as `&[(key, weight)]`

The `SortedSetStoreOptions` struct carries only the optional `AGGREGATE` modifier and defaults to `SUM`.

**Migration:**

```rust
use redis::{Commands, SortedSetStoreOptions, Aggregate};

// Before:
con.zinterstore("out", &["zset1", "zset2"])?;
con.zinterstore_min("out", &["zset1", "zset2"])?;
con.zinterstore_weights("out", &[("zset1", 2), ("zset2", 3)])?;
con.zinterstore_min_weights("out", &[("zset1", 2), ("zset2", 3)])?;

// After:
con.zinterstore("out", &["zset1", "zset2"], SortedSetStoreOptions::default())?;
con.zinterstore("out", &["zset1", "zset2"], SortedSetStoreOptions::default().aggregate(Aggregate::Min))?;
con.zinterstore_with_weights("out", &[("zset1", 2), ("zset2", 3)], SortedSetStoreOptions::default())?;
con.zinterstore_with_weights("out", &[("zset1", 2), ("zset2", 3)], SortedSetStoreOptions::default().aggregate(Aggregate::Min))?;
```

The same pattern applies to `zunionstore` and `zunionstore_with_weights`.


### `cmd_iter` yields `CmdRef` instead of `&Cmd` (Breaking Change)

**Most users can upgrade to 2.0.0 with no code changes.** The flattening is an internal representation change; the pipeline builder API (`cmd`, `arg`, `add_command`, `ignore`, `query`, `query_async`, `exec`, …) is unchanged. The only adjustments are needed if you iterate a pipeline's commands or call `with_capacity` directly.

Because a pipeline no longer owns a `Vec<Cmd>`, there is no `&Cmd` to hand out. [`Pipeline::cmd_iter`] and [`ClusterPipeline::cmd_iter`] now yield `CmdRef<'_>`, a lightweight, `Copy` view that borrows directly into the pipeline's shared buffers — iterating a pipeline's commands performs no per-command allocation.

`CmdRef` is intentionally opaque so that the underlying storage can keep evolving. It exposes the read-only accessors you previously reached for on `&Cmd`, including `args_iter()`, `arg_idx()`, `data()`, `cursor()`, `is_no_response()`, and `get_packed_command()`. If you genuinely need an owned `Cmd`, call `to_cmd()`.

**Migration:** Update code that iterates a pipeline's commands. Most call sites only need to drop a borrow or call an accessor:

```rust
// Before:
for cmd in pipe.cmd_iter() {
    let name = cmd.arg_idx(0);
    // cmd: &Cmd
}

// After:
for cmd in pipe.cmd_iter() {
    let name = cmd.arg_idx(0);
    // cmd: CmdRef<'_> — same read accessors, Copy
}
```

If you stored or passed the `&Cmd` onward and need an owned value:

```rust
// Before:
let owned: Vec<Cmd> = pipe.cmd_iter().cloned().collect();

// After:
let owned: Vec<Cmd> = pipe.cmd_iter().map(|cmd| cmd.to_cmd()).collect();
```

### `Pipeline::with_capacity` is replaced by `reserve_for_*` methods (Breaking Change)

[`Pipeline::with_capacity`] and [`ClusterPipeline::with_capacity`] have been removed. A flattened pipeline stores its commands across three buffers (commands, arguments, and argument bytes), and a single capacity number no longer maps cleanly onto them. Rather than force you to estimate all three up front, pre-allocation is now opt-in per buffer via chainable methods, so you reserve only the dimensions you actually have a number for:

```rust
pub fn reserve_for_commands(&mut self, additional: usize) -> &mut Self
pub fn reserve_for_args(&mut self, additional: usize) -> &mut Self
pub fn reserve_for_data(&mut self, additional: usize) -> &mut Self // argument bytes
```

**Migration:** Replace `with_capacity` with the reservations you can estimate:

```rust
// Before:
let mut pipe = redis::Pipeline::with_capacity(16); // 16 commands

// After: reserve whichever buffers you have an estimate for
let mut pipe = redis::pipe();
pipe.reserve_for_commands(16).reserve_for_args(48);
```

`Pipeline::new()` and `pipe()` are unchanged.
