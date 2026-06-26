# Guides

In-depth guides for every crdb-dump feature.

<div class="grid cards" markdown>

-   :material-table-cog:{ .lg .middle } **Export Schema**

    ---

    Full-database DDL, formats, per-object files, and object selection.

    [:octicons-arrow-right-24: Export Schema](export-schema.md)

-   :material-database-arrow-down:{ .lg .middle } **Export Data**

    ---

    CSV/SQL, chunking, gzip, ordering, parallelism, and manifests.

    [:octicons-arrow-right-24: Export Data](export-data.md)

-   :material-backup-restore:{ .lg .middle } **Import & Restore**

    ---

    Schema apply, `COPY` loads, validation, parallelism, and resume.

    [:octicons-arrow-right-24: Import & Restore](import-restore.md)

-   :material-file-tree:{ .lg .middle } **Multi-Schema Objects**

    ---

    Three-part naming and non-`public` schema support.

    [:octicons-arrow-right-24: Multi-Schema Objects](multi-schema.md)

-   :material-shape:{ .lg .middle } **Type Handling**

    ---

    BYTES, UUID, arrays, enums, and VECTOR round-trips.

    [:octicons-arrow-right-24: Type Handling](type-handling.md)

-   :material-account-key:{ .lg .middle } **Permissions**

    ---

    Export roles, grants, and role memberships.

    [:octicons-arrow-right-24: Permissions](permissions.md)

-   :material-earth:{ .lg .middle } **Region-Aware**

    ---

    Filter export and import by table locality.

    [:octicons-arrow-right-24: Region-Aware](region-aware.md)

-   :material-cloud-upload:{ .lg .middle } **S3-Compatible Storage**

    ---

    AWS S3, MinIO, and Cohesity with custom endpoints.

    [:octicons-arrow-right-24: S3-Compatible Storage](s3-storage.md)

-   :material-swap-horizontal:{ .lg .middle } **Migration & Limitations**

    ---

    What crdb-dump is, what it isn't, and the consistency caveat.

    [:octicons-arrow-right-24: Migration & Limitations](migration-limitations.md)

</div>
