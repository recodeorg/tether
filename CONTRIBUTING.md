# Contributing to Tether

Tether operates under a BDFL governance model. To ensure your time is respected and your efforts align with the project's trajectory, please open an issue to discuss any significant architectural changes or large features before drafting a pull request.

If a proposed PR conflicts with the core design philosophies below, it will be closed, regardless of the code quality.

## 1. Radical Simplicity
Public APIs must remain dirt simple. Do not overengineer solutions, introduce complex abstractions, or sacrifice framework readability just to shave off a few CPU cycles or bytes of memory. If a feature requires massive mental overhead for the end-user to understand, it does not belong in Tether.

## 2. Maintainability by Design
Building and scaling a Tether project should be effortless for the developer. Infrastructure transitions must be seamless. Just as moving from local disk storage to S3, or swapping SQLite for PostgreSQL, requires changing exactly one line of code, any new systems should strive for this exact plug-and-play developer experience.

## 3. Agnostic Portability ("Hostability")
Tether will never enforce vendor lock-in. Regardless of how the framework grows or what external enterprise integrations are supported, Tether must fundamentally remain capable of running as a single, self-contained Go binary on a standard, self-hosted VPS.

## 4. Pragmatic Extensibility
Do not hard-code assumptions into the core engine. Features that rely on specific external infrastructure should be introduced via modular interfaces. For example, if you wish to introduce Redis for pub/sub, it must be proposed via an adapter pattern that gracefully sits alongside the existing PostgreSQL listener without strictly coupling the rest of the codebase to your chosen tech stack.

## Development Setup

Tether requires Go 1.26 or newer.

    git clone https://github.com/recodeorg/tether.git
    cd tether
    go test -race ./...

By default, the test suite runs against in-memory SQLite. To run it against Postgres, start a local instance matching the DSN in `engine_test.go` and pass the `-postgres` flag:

    docker run --rm -p 5432:5432 -e POSTGRES_PASSWORD=secret -e POSTGRES_DB=mydb postgres
    go test -race ./... -args -postgres

Changes that touch the engine, transactions, or reactivity should pass under both databases.

## Submitting a Pull Request

Before opening a PR, please make sure:

- [ ] `go test -race ./...` passes (CI runs exactly this).
- [ ] Code is formatted with `gofmt` and passes `go vet ./...`.
- [ ] New behavior is covered by tests.
- [ ] Public API changes include updated doc comments.

Keep PRs focused. One feature or fix per PR is much easier to review than a bundle.

## Reporting Bugs

Please open an issue that includes:

- Your Go version and Tether version
- Which database you're using (SQLite or Postgres) and which storage adapter, if relevant
- A minimal code sample that reproduces the problem
- What you expected to happen versus what actually happened

## Security Vulnerabilities

Please **do not** open a public issue for security problems. Instead, use GitHub's private vulnerability reporting. You'll get a response within 5 days.

## Licensing

By submitting a contribution, you agree that it is licensed under the [Apache License 2.0](LICENSE), the same license as the rest of the project.