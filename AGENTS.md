# AGENTS.md

This is a personal, single-maintainer project (Christopher Kujawa / `Zelldon`). There
is no central org-level AGENTS.md to defer to — everything below is local to this repo.

For the human contribution workflow (issue guidelines, PR process, review emoji
code), see [CONTRIBUTING.md](CONTRIBUTING.md). This file only covers what that one
doesn't: build automation quirks, testing conventions, and things worth confirming
before doing.

## What this is

ZDB (Zeebe Debugger) is a CLI + JavaFX GUI for inspecting a Zeebe partition's on-disk
state (RocksDB snapshot/runtime) and log stream when a cluster is broken and exporters
haven't captured what happened. Java 21, multi-module Maven, `backend` mixes Kotlin
and Java. Supports Zeebe versions 8.1 through 8.8 (see `README.md` for full usage).

## Modules

| Module     | Contents                                                                |
|------------|--------------------------------------------------------------------------|
| `backend`  | Core logic: state/log/raft readers, `ZeebeDbReader`, per-version tests   |
| `cli`      | picocli-based CLI facade (`ZeebeDebugger`, `IncidentCommand`, ...)       |
| `frontend` | JavaFX GUI (currently untested — no test sources)                       |

`backend`'s Maven `sourceDirectory`/`testSourceDirectory` are `src/main/kotlin` and
`src/test/kotlin` **even though most files under `src/test/kotlin` are `.java`, not
`.kt`** — don't be misled by the directory name into looking for a `src/test/java`
that doesn't exist for this module. `cli`'s tests live in the conventional
`src/test/java`.

## Build & verify

No Maven wrapper — use the system `mvn` (Java 21 required). CI runs `mvn -B verify`
against the whole reactor; this project is small enough that module-scoped builds are
a convenience for a fast inner loop, not a hard requirement the way they are in large
monorepos.

```bash
mvn clean install -DskipTests          # full build, all modules
mvn -pl backend test -Dtest=Version88Test   # single test class
mvn verify                              # full pipeline: format, checkstyle, tests
```

Formatting and license headers are applied automatically, not just checked:
- `fmt-maven-plugin` (Google Java Format) runs in `cli` and `frontend`'s `validate`
  phase — it is **not** bound in `backend`, so `backend` sources are not
  auto-formatted by Maven; match the surrounding style by hand there.
- The root `license-maven-plugin` inserts an Apache-2.0 header into any file that's
  missing one, during the `compile` phase, for every module in the reactor
  (`skipExistingHeaders=true`, so it never touches a file that already has one).
- Checkstyle (`cli` module only) **fails** the build on violation
  (`failOnViolation=true`, bound to `validate`) — these must be fixed by hand, they
  are not auto-fixed.

## Tests

- JUnit 5 + AssertJ; structure test bodies with `// given` / `// when` / `// then`
  comments (see any `Version88*Test` class for the established shape).
- The `snapshot-generator` JUnit tag (`SnapshotGenerator*Test` classes) is excluded
  from the default `mvn test` run (`backend/pom.xml` surefire `excludedGroups`)
  because those tests need Docker and **overwrite the committed
  `src/test/resources/zeebe-states/<version>.zip`** fixture with fresh keys/timestamps
  every run. Only run them intentionally, and diff the resulting zip before deciding
  to commit it — never as part of routine verification.
- `SnapshotFixture` (`backend/src/test/kotlin/io/zell/zdb/SnapshotFixture.java`) is
  the shared helper for both directions of that zip: `unzip()` / `cleanup()` for
  tests that consume a pre-committed snapshot, `pack()` / `copyDirectory()` /
  `deleteRecursively()` for the `SnapshotGenerator*Test` that produces one. Extend
  this class instead of re-implementing zip/copy/delete logic in a new version's
  generator test.
- `GoldenFileAssert` (`backend/src/test/kotlin/io/zell/zdb/GoldenFileAssert.java`)
  compares rendered output against a committed file under
  `backend/src/test/resources/golden/<version>/` and throws with a unified diff on
  mismatch. Regenerate goldens intentionally with
  `-Dtest=<GoldenTestClass> -DupdateGoldens=true` and review the diff — never
  regenerate just to make a failing test pass without first understanding why it
  failed.
- `v81`-`v87` test classes (`backend/src/test/kotlin/io/zell/zdb/v8N/Version8NTest.java`)
  still use the older Testcontainers-per-test pattern that `v88` was migrated away
  from — see `Version88Test.java` for the target shape (`SnapshotFixture` against a
  pre-committed zip instead of a live Zeebe container per run). Migrating each of
  these the same way is planned follow-up work.

## Commit & PR conventions

Follow [CONTRIBUTING.md](CONTRIBUTING.md) for the full commit/PR workflow. One delta
worth flagging: this repo's Conventional Commits format **allows an optional scope**
(`type(scope): description`, e.g. `fix(cli): ...`) — don't assume the no-scope rule
some other repos enforce.

## Ask first

- Regenerating and committing a `zeebe-states/<version>.zip` fixture — the diff is
  effectively unreviewable binary noise, so confirm the generator ran cleanly and the
  change is intentional first.
- Changing shared BOM/dependency versions in the root `pom.xml`.
- Touching `release.sh` or the release/deploy GitHub Actions workflows.
