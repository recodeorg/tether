## [Unreleased]

- Postgres support
- Easy file uploads
- Guards API for query and mutation auth
- Performance profiler
- Cron Jobs & Scheduling

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
