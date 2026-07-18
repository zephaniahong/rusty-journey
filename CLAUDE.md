# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

`spsc` is a Rust library crate (edition 2024, no dependencies) implementing a Single-Producer Single-Consumer queue. The crate is in an early scaffolding state: `src/lib.rs` sketches `Producer<T>`, `Consumer<T>`, and a `channel(capacity)` constructor, but the implementation is unfinished and does not currently compile.

Known issues in the current scaffold that a future change will need to address:
- `struct Producer` is declared without a type parameter, but `impl<T> Producer<T>` references `Producer<T>`.
- `channel` returns `(Producer, Consumer<T>)` with an unbound `T` in its signature — it needs to become generic (`fn channel<T>(...) -> (Producer<T>, Consumer<T>)`).
- `Consumer::pop` is `todo!()`; there is no `Producer::push` yet, no shared ring buffer, and no synchronization primitive tying the two ends together.

When implementing the queue, the two halves must share state (typically an `Arc` around a ring buffer with atomic head/tail indices) — the current `capacity: u64` fields on each side are placeholders, not the real design.

## Commands

- Build: `cargo build`
- Run tests: `cargo test`
- Run a single test: `cargo test <test_name>` (e.g. `cargo test it_works`)
- Lint: `cargo clippy`
- Format: `cargo fmt`

## instructions
This is a project for me to learn. Avoid giving full answers. Always guide towards the answer but never give the actual answer unless asked upon.
