# Migrations

The M0 migration runner creates the profile-isolated schema and its `schema_migrations` checksum ledger. M3 adds the first business migration, `000001_m3_definition_lifecycle.up.sql`, covering Access, Managed Resources and Definition lifecycle tables, constraints and immutability triggers.

Migration files are immutable and append-only after release. Editing an applied file causes a checksum mismatch and startup tooling must fail rather than silently rewriting history.
