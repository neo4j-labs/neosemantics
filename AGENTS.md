# n10s Agent Development Instructions

This file records all conventions, workflows, and gotchas for agent sessions working on neosemantics (n10s).

## Repository Layout

- Main development branch: `2026.01` (Neo4j 2026.01.x target)
- Maintenance branch: `5.26` (Neo4j 5.26.x target)
- Java 21 required; Maven build
- Tests live in `src/test/java/n10s/`; resources in `src/test/resources/`
- Docs are AsciiDoc in `docs/modules/ROOT/pages/`; auto-generated CSVs in `docs/modules/ROOT/examples/`

## Java / Maven Environment

Java 25 is the current default. The project targets Java 21 (`--release 21` in compiler config) but builds and tests cleanly on Java 25.

If your default Java is older than 21, prefix Maven commands with:

```bash
JAVA_HOME=~/.sdkman/candidates/java/21.0.7-tem mvn <goal>
```

## Before Every Commit

Run compile to verify no syntax errors:

```bash
mvn clean compile -q
```

## Running Tests

The test suite has 38 independent classes (each starts its own embedded Neo4j). The default `forkCount` is 24; override it to match your CPU count for fastest runs. On a well-provisioned machine, setting it to ≥38 runs all classes in a single parallel wave (~1:41 wall time):

```bash
mvn clean test -Dsurefire.forkCount=40
```

Normal run with default forks (slower, ~2:49):

```bash
mvn clean test
```

**Why it's slow per class:** every test class starts an embedded Neo4j instance (~40-70 s overhead). Increasing `forkCount` to run more classes in parallel is the main lever. Splitting large test classes further is counterproductive once the overhead dominates.

**Note:** Neo4j 2026.04+ emits `WARNING: sun.misc.Unsafe::invokeCleaner` on Java 25. This is a cosmetic upstream warning from Neo4j internals and does not affect test results. No JVM flag suppresses it in Java 25.

## Git Conventions

- **One issue per branch**, one branch per PR
- Branch naming: `fix/<issue>-<short-slug>` or `feat/<issue>-<short-slug>`
- Target branch: `2026.01` for PRs (use `--base 2026.01`)
- **Never** `git add docs/` — running `mvn test` regenerates ~150 CSVs in `docs/modules/ROOT/examples/` automatically; staging them pollutes commits
- Only stage files that were explicitly created, modified, or deleted as part of the current task — never stage unrelated files
- Only stage `src/main/java/` and `src/test/java/` files (and docs `pages/` if you edited docs)
- **No** `Co-Authored-By` entry in commit messages
- Never auto-execute `git commit` — show the diff and message to the user to confirm

## Commit Message Format

```
<Short verb phrase describing the fix> (#<issue-number>)
```

Example: `Fixes #166: parse ISO 8601 datetimes with bare UTC offsets (e.g. +00:00)`

## PR Creation

Use `gh pr create` with explicit `--base 2026.01`:

```bash
gh pr create \
  --repo neo4j-labs/neosemantics \
  --base 2026.01 \
  --head <branch-name> \
  --title "..." \
  --body "..."
```

Always check `git merge-base <branch> 2026.01` before pushing to confirm the branch forks from the right point.

## Docs Updates

When fixing a bug or adding a feature, check `docs/modules/ROOT/pages/` for existing coverage:
- `import.adoc` — all import behavior, data type handling, language tags, RDF-star, etc.
- `export.adoc` — export behavior
- `config.adoc` — `_GraphConfig` parameters
- `inference.adoc` — inference procedures
- `install.adoc` — installation

If the feature has no documentation section, add one to the relevant `.adoc` file and commit it on the same branch as the fix (as a separate commit is fine).

The `docs/modules/ROOT/examples/` CSVs are auto-generated — **do not edit them manually**.

## Key Source Files

| File | Purpose |
|------|---------|
| `src/main/java/n10s/RDFToLPGStatementProcessor.java` | Core triple processing; all RDF→LPG mapping logic |
| `src/main/java/n10s/rdf/load/DirectStatementLoader.java` | Direct import with periodic commit support |
| `src/main/java/n10s/utils/NsPrefixMap.java` | Namespace prefix storage; `_NsPrefDef` node management |
| `src/main/java/n10s/utils/DateUtils.java` | Date parsing fallback (do not modify; fix upstream in processor) |
| `src/main/java/n10s/graphconfig/GraphConfig.java` | `_GraphConfig` node and constants |
| `src/test/java/n10s/RDFProceduresTest.java` | Main integration test suite |
| `pom.xml` | Dependencies; shade plugin config |

## pom.xml Key Points

- Jackson: version `2.19.0`, all Jackson deps are `provided` scope
- Shade plugin **must exclude all Jackson** (`com.fasterxml.jackson.*`) to avoid runtime conflicts with Neo4j's bundled Jackson
- JUnit: `4.13.2`
- Neo4j version property: `neo4j.version` (currently `2026.01.4`)
- RDF4J version property: `sesame.version` (currently `4.3.12`)

## Schema Nodes

| Node label | Purpose | Key properties |
|-----------|---------|---------------|
| `_GraphConfig` | Import configuration | `handleVocabUris`, `handleRDFTypes`, `applyNeo4jNaming`, `handleMultival`, `keepLangTag`, `langFilter` |
| `_NsPrefDef` | Namespace prefix store | One node; properties are `prefix → namespace` pairs |
| `_n10sValidatorConfig` | SHACL validation config | Various shape properties |

## Issue Workflow

1. Read the issue with `gh issue view <N> --repo neo4j-labs/neosemantics`
2. Label it: `gh issue edit <N> --add-label "bug"` (labels: bug, question, enhancement, duplicate, dependencies)
3. Create a fix branch from `2026.01`
4. Write a test first (in `RDFProceduresTest.java` or the relevant `*Test.java`)
5. Implement the fix
6. Compile and verify
7. Add/update docs if the behavior is user-visible
8. Show diff and commit message to user; commit on confirmation
9. Push and create PR with `--base 2026.01`

## Dependabot PRs

- Maven dependency bumps: check `pom.xml` first; if already at a newer version, close as "fixed manually"
- `/docs` npm bumps: `package.json` uses broad `^`/`~` ranges; close as "superseded by version ranges"
- Close with: `gh pr close <N> --repo neo4j-labs/neosemantics --comment "Closing as fixed manually / superseded. ..."`

## Test Patterns

Integration tests use the Neo4j test harness (`neo4j-harness`). Standard pattern:

```java
@Test
public void testMyFix() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), driverConfig)) {
        Session session = driver.session();
        // init graph config
        session.run("CALL n10s.graphconfig.init({...})");
        // import inline RDF
        Result result = session.run("CALL n10s.rdf.import.inline('" + turtle + "', 'Turtle')");
        // assert
        Result query = session.run("MATCH (n:Resource) RETURN n.someProperty");
        assertTrue(query.hasNext());
        assertEquals(expected, query.next().get("n.someProperty").asObject());
    }
}
```

## Known Gotchas

- `ZonedDateTime.parse()` requires zone region ID like `[UTC]`; use `OffsetDateTime.parse()` for bare offsets like `+00:00`
- `applyNeo4jNaming: true` uppercases relationship types and capitalizes label names; it is separate from `handleVocabUris`
- `initialiseRelProps()` in `RDFToLPGStatementProcessor` is only called from the RDF-star code path
- The `_NsPrefDef` node gets a write lock in `NsPrefixMap.reloadFromDB()` — concurrent imports can deadlock on it
- `mvn test` regenerates all `docs/modules/ROOT/examples/*.csv` — never stage those files
- Two active branches: PRs for `2026.01` (main) and `5.26` (maintenance); major fixes may need backporting to both

## Pending Work (as of 2026-05-08)

### PRs ready to merge (awaiting review)
- #346 — Jackson shade exclusion fix (2026.01 branch)
- #347 — Datetime UTC offset fix (#166) + docs
- #348 — RDF-star predicate IRI fix (#265) + docs
- #349 — JUnit 4.13.2 bump
- #350 (or similar) — Docs typo fix (#218)

### Phase 4 bugs (needs branch + fix)
- #297 — `DeadlockDetectedException` during periodic commits; fix: retry loop + move buffer clearing to success path
- #192 — Multi-annotation RDF-star: duplicate relationship instead of merged properties
- #324 — ChEBI OWL import: missing labels and relationships
- #180 — Wikidata invalid URIs: `#` and `{` in URIs cause RDF4J parse failures

### Phase 5 enhancements (needs branch + implementation)
See n10s-roadmap.md in `/Users/mh/d/python/rdflib-neo4j/findings/` for full list.
