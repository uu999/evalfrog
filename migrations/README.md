# Migrations

M0 deliberately contains no business tables. The migration runner creates the profile-isolated schema and its `schema_migrations` checksum ledger; later milestones add immutable, ordered `NNNNNN_name.up.sql` files here.

Migration files are append-only after release. Editing an applied file causes a checksum mismatch and startup tooling must fail rather than silently rewriting history.
