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
