## [1.0.0] - 2026-09-28

First stable release. Focuses on correct auth/reactivity under batching and transactions, a smaller public API, and hardening around storage, scheduling, and WebSockets.

### Breaking changes
- `RegisterQuery` no longer accepts a `dependencies` slice; dependencies come only from `TrackCollection` / `TrackTable` inside the query
- `QueryOptions` and `MutationOptions` are replaced by functional options. Pass `Internal()` to `RegisterQuery` or `RegisterMutation` to hide that function from clients
- `storage.UploadOptions` is replaced by functional options. `GetUploadURL` takes `...storage.UploadOption` (`WithMaxBytes`, `WithExpiresIn`). Pass the same options to `SetStorage` as the defaults for every upload. `SetStorage` also takes a base path, so the `/storage` upload and download routes can be mounted somewhere else
- `CreateTable` no longer accepts a table name argument
- `QueryCtx.Scheduler` and `GuardCtx.Profiler` removed (schedule from mutations; profiling is on `MutationCtx`)
- Several engine hooks are no longer exported: `ExecuteQuery`, `ExecuteMutation`, `ExecuteMutationInternal`, `InvalidateTag`, `InvalidateTags`, `OnReceiveMessage`, `OnConnect`, `OnDisconnect`, and `GetDependentQueries`
- `reactivity.Handle` no longer uses an `EngineHandler` interface; it takes a message callback directly
- Query, mutation, and guard handlers return `(any, error)` instead of a single value
- `Auth.VerifyToken` takes a `context.Context`
- `StorageAdapter.Delete` takes a `context.Context`, and `DefaultUploadOptions` is removed. Pass upload defaults to `SetStorage`
- `RegisterMutation`, `RegisterQuery`, and `RegisterGuard` panic when the name is already registered
- `Engine.EphemeralID` is now the method `Engine.EphemeralID()`
- The profiler moves from package `utilities` into package `tether`. `Engine.Profiler` is now the method `Engine.Profiler()`. `utilities.NewProfiler` is no longer available; use the engine's profiler
- Every JSON frame sent to a client includes `protocol_version`
- `QueryCtx.Dependencies` and `GuardCtx.Dependencies` are no longer exported. Record dependencies with `TrackCollection` and `TrackTable`
- `Mutation`, `Query`, and `Guard` are no longer exported
- `local.NewLocalStorage` and `local.LocalStorage` are now `local.New` and `local.Storage`. `s3.NewS3Storage` and `s3.S3Storage` are now `s3.New` and `s3.Storage`
- Minimum Go version is 1.26

### Added
- `Engine.Close()` for graceful shutdown: cancels background loops, stops the profiler and pending timers, waits for in-flight work, and releases scheduled-task claims held by this instance
- `RegisterGuard` accepts `...GuardOption`. No guard options are defined yet; the parameter is reserved so they can be added without changing the signature
- `storage.Name()` method to the storage interface, for adapters to identify themselves in the storage DB for easier migrations in the future
- `ErrEngineClosed` when scheduling work after close, and when calling `SetStorage` after close
- `ErrNoCaller`, returned by `GetIdentity` in a mutation run by the scheduler or a cron. That call used to panic
- Transaction-aware invalidation: tags from writes inside a DB transaction are published only after commit; rollbacks discard them (including Postgres `NOTIFY`, delivered only on commit)
- Read-only database access in queries and guards enforced at the connection pool (PostgreSQL read-only transactions plus SQL statement checks)
- Performance profiling for mutations via `MutationCtx.Profiler`
- Structured WebSocket `error` responses for failed subscribe, query, and mutation handling
- Query execution timestamps and client `AuthEpoch` so stale or wrong-identity results can be dropped safely
- WebSocket limits: 8KB max incoming frame size and 100 subscriptions per client
- Clients that stop reading (full send buffer or write timeout) are disconnected so they reconnect cleanly

### Changed
- Queries and mutations that return an `error` now send a WebSocket `error` frame with the error message instead of an empty `{}` as `data`; internal mutations (scheduled tasks, crons) return it as their error
- Horizontal sync: Postgres listener payloads encoded as JSON, with chunking under the 8000-byte `NOTIFY` limit
- Identity changes (login, logout, token expiry) reset guard subscriptions and auth-scoped tags, then re-run affected queries
- Nested guard execution from within a guard is rejected
- Scheduled-task and cron execution respect engine lifecycle and close
- Removed internal `queryHash` machinery
- Dependency tags on invalidation batches are ordered deterministically
- Sensitive fields (e.g. auth tokens) are redacted in debug logging; raw WebSocket payloads are no longer logged verbatim

### Fixed
- `RegisterCron` validates cron expressions at registration time
- Expired or replaced authentication could leave cached guard decisions in place, so protected subscriptions kept receiving data ([#28](https://github.com/recodeorg/tether/issues/28))
- Auth state bleeding when batching queries that call `GetIdentity()` conditionally
- Guards now fail closed on errors; several guard fingerprinting and return-type bugs
- Invalidation bypasses, wrong ordering, and missed updates (including pre-write snapshots for updates/deletes and delete-by-primary-key tagging)
- Query invalidation while a transaction is open; writes attempted from query/guard paths
- Scheduler deadlocks; invalid cron schedules; task cancellation across horizontally scaled nodes; tasks and timers not stopping on `Close`; timer task deletion and error handling gaps
- Concurrent uploads, storage cleanup loop, and download-token / failed-upload cleanup
- Local storage path traversal; unsafe browser inlining of SVG and other MIME types
- S3 adapter failing on custom `http` endpoints
- Memory leak found during audit
- Panic recovery on several background goroutines
- `SetStorage` returns a table-creation error without shutting down the engine

### Security
- Auth tokens must not appear in logs or profiler metrics (regression test added)
- Storage and download paths hardened against traversal and content-sniffing bypasses
- S3 endpoint configuration restricted to avoid unintended credential exfiltration patterns

## [0.7.0] - 2026-09-26
### Added
- Simple file upload API, provided through adapters
- Official local file storage adapter
- Official S3/S3-compatible adapter

## [0.6.0] - 2026-09-25
### Added
- Postgres support
- An optional -postgres flag for running tests with a provided postgres instance
- Horizontal scaling using listen/notify as a pub/sub

### Changed
- Removed the dbType parameter from NewEngine, it now pulls the dbType directly from GORM's Dialector

## [0.5.0] - 2026-09-25
### Added
- ctx.Scheduler.RunAfter() for scheduling mutation calls in the future
- ctx.Scheduler.Cancel() for canceling said scheduled calls
- engine.RegisterCron() for registering recurring cron jobs

## [0.4.0] - 2026-09-20
### Added
- Guard functions for improved auth performance and safety

## [0.3.0] - 2026-09-09
### Added
- Native in-engine automatic performance profiling for all queries, mutations, authentication, etc.
- Automatic batching of like-queries into a single query execution
### Changed
- Queries now execute concurrently within goroutines

## [0.2.0] - 2026-09-02
### Added
- Unit tests for all applicable functionality
### Changed
- Updated AutoTrack to check for a primary key, not a hardcoded ID value
- Removed IsAuthenticated from the Auth props
### Fixed
- SubscribeToQuery/Untrack panics when the client is not tracked
- Race condition when multiple goroutines track/subscribe/untrack a shared client
- Updating records don't invalidate old tags
- Update does not properly produce tags, causing a missed re-run
- Mutations don't invalidate queries with TrackTable
- ExecuteQuery and ExecuteMutation panic when given an unknown query/mutation
- Malformed requests cause panics
- User IDs containing quotes create malformed JSON in auth
- Auth expiry after a client disconnect causes a panic
- Executing query on untracked client causes a panic
- Three race conditions caught by testing

## [0.1.2] - 2026-08-30
### Fixed
- Package can now be pulled by `go get`

## [0.1.0] - 2026-08-30

Initial release.
