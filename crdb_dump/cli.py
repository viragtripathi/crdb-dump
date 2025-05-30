import importlib
import json
import os
import platform
import click
from subprocess import check_output

from crdb_dump.export.data import export_data
from crdb_dump.export.schema import export_schema
from crdb_dump.loader.loader import load_schema, load_chunks_from_manifest
from crdb_dump.utils.db_connection import get_sqlalchemy_engine
from crdb_dump.utils.io import archive_output
from crdb_dump.verify.checksum import verify_checksums
from crdb_dump.utils.logging import init_logger


@click.group()
@click.option('--verbose', is_flag=True, help='Enable debug logging')
@click.pass_context
@click.version_option()
def main(ctx, verbose):
    """crdb-dump: Export and Import CockroachDB schemas and data."""
    ctx.ensure_object(dict)
    logger = init_logger(verbose)
    ctx.obj["logger"] = logger
    ctx.obj["verbose"] = verbose


@main.command()
@click.pass_context
@click.option('--db', required=True, help='Database name')
@click.option('--tables', default=None, help='Comma-separated list of db.table names')
@click.option('--exclude-tables', default=None, help='Comma-separated list of db.table names to exclude from schema/data export')
@click.option('--per-table', is_flag=True, help='Output individual files per object')
@click.option('--format', 'out_format', type=click.Choice(['sql', 'json', 'yaml']), default='sql', help='Schema output format')
@click.option('--include-permissions', is_flag=True, help='Export CREATE ROLE, GRANT, and membership statements')
@click.option('--archive', is_flag=True, help='Compress output directory')
@click.option('--diff', help='Compare output with existing SQL file')
@click.option('--parallel', is_flag=True, help='Enable parallel exports')
@click.option('--data', is_flag=True, help='Export table data')
@click.option('--data-format', type=click.Choice(['sql', 'csv']), default='sql', help='Data export format')
@click.option('--data-split', is_flag=True, help='Split each table into a separate file')
@click.option('--data-limit', type=int, default=None, help='Limit rows per table')
@click.option('--data-compress', is_flag=True, help='Compress CSV output')
@click.option('--data-order', default=None, help='Order data by column(s)')
@click.option('--data-order-desc', is_flag=True, help='Order data descending')
@click.option('--data-parallel', is_flag=True, help='Parallel data export')
@click.option('--data-order-strict', is_flag=True, help='Fail if ordered column(s) not found')
@click.option('--chunk-size', type=int, default=None, help='Rows per CSV chunk')
@click.option('--verify', is_flag=True, help='Verify exported chunk checksums')
@click.option('--verify-strict', is_flag=True, help='Stop if any checksum fails')
@click.option('--out-dir', default='crdb_dump_output', help='Output directory for all exports')
@click.option('--print-connection', is_flag=True, help='Print resolved database connection URL and exit')
@click.option('--retry-count', type=int, default=3, help='Number of retry attempts')
@click.option('--retry-delay', type=int, default=1000, help='Initial retry delay in milliseconds')
def export(ctx, **kwargs):
    logger = ctx.obj["logger"]
    kwargs["verbose"] = ctx.obj["verbose"]

    kwargs["retry_count"] = kwargs.get("retry_count", 3)
    kwargs["retry_delay"] = kwargs.get("retry_delay", 1000)

    engine = get_sqlalchemy_engine(kwargs)

    if kwargs.get("print_connection"):
        redacted_url = engine.url.set(username=None, password=None)
        logger.info(f"🔗 Using CockroachDB URL: {redacted_url}")
        return

    out_dir = os.path.join(kwargs['out_dir'], kwargs['db'])
    export_schema(kwargs, out_dir, logger)

    if kwargs['data']:
        export_data(kwargs, out_dir, logger)

    if kwargs['verify']:
        verify_checksums(kwargs, out_dir, logger)

    if kwargs['archive']:
        archive_output(out_dir)

@main.command()
@click.option('--db', required=True, help='Target database name')
@click.option('--schema', type=click.Path(exists=True), help='Schema SQL file to load')
@click.option('--data-dir', type=click.Path(exists=True), help='Directory containing manifest and data files')
@click.option('--resume-log', default='resume.json', help='Path to JSON file tracking loaded chunks')
@click.option('--dry-run', is_flag=True, help='Show what would be imported without executing')
@click.option('--include-tables', default=None, help='Comma-separated list of fully-qualified tables to include (e.g., movr.users)')
@click.option('--exclude-tables', default=None, help='Comma-separated list of fully-qualified tables to exclude')
@click.option('--print-connection', is_flag=True, help='Print resolved database connection URL and exit')
@click.option('--parallel-load', is_flag=True, help='Use parallel loading of chunks')
@click.option('--validate-csv', is_flag=True, help='Validate row/column match before COPY')
@click.option('--retry-count', type=int, default=3, help='Number of retry attempts')
@click.option('--retry-delay', type=int, default=1000, help='Initial retry delay in milliseconds')
@click.pass_context
def load(ctx, db, schema, data_dir, resume_log, dry_run,
         include_tables, exclude_tables, print_connection,
         parallel_load, validate_csv, retry_count, retry_delay):
    logger = ctx.obj.get("logger")
    opts = {"db": db}
    engine = get_sqlalchemy_engine(opts)

    if print_connection:
        redacted_url = engine.url.set(username=None, password=None)
        logger.info(f"🔗 Using CockroachDB URL: {redacted_url}")
        click.echo(f"🔗 Using CockroachDB URL: {redacted_url}")
        if not dry_run:
            return

    if schema and not dry_run:
        load_schema(schema, engine, logger)

    include = set(include_tables.split(',')) if include_tables else None
    exclude = set(exclude_tables.split(',')) if exclude_tables else None

    for fname in os.listdir(data_dir):
        if fname.endswith(".manifest.json"):
            manifest_path = os.path.join(data_dir, fname)

            try:
                with open(manifest_path) as f:
                    table_fullname = json.load(f)["table"]
            except Exception as e:
                logger.warning(f"⚠️ Skipping malformed manifest {fname}: {e}")
                continue

            if include and table_fullname not in include:
                logger.info(f"⏩ Skipping {table_fullname} (not in include list)")
                continue
            if exclude and table_fullname in exclude:
                logger.info(f"⏩ Skipping {table_fullname} (in exclude list)")
                continue

            if dry_run:
                logger.info(f"[Dry Run] Would load: {manifest_path}")
            else:
                load_chunks_from_manifest(
                    manifest_path,
                    data_dir,
                    engine,
                    logger,
                    resume_file=resume_log,
                    parallel=parallel_load,
                    validate=validate_csv,
                    retry_count=retry_count,
                    retry_delay=retry_delay
                )

@main.command()
@click.pass_context
@click.option('--json', 'as_json', is_flag=True, help='Output version info as JSON')
def version(ctx, as_json):
    """Show detailed version info."""
    logger = ctx.obj["logger"]

    try:
        pkg_name = "crdb-dump"
        pkg_version = importlib.metadata.version(pkg_name)
        dist_info = importlib.metadata.distribution(pkg_name)
        pkg_location = dist_info.locate_file("")
    except importlib.metadata.PackageNotFoundError:
        pkg_version = "unknown"
        pkg_location = "not installed"

    try:
        repo_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        commit = check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=repo_dir,
            stderr=open(os.devnull, 'w')
        ).decode().strip()
    except Exception:
        commit = "unknown"

    build_date = check_output(["date", "+%Y-%m-%d"]).decode().strip()
    python_version = platform.python_version()

    metadata = {
        "name": "crdb-dump",
        "version": pkg_version,
        "git_commit": commit,
        "build_date": build_date,
        "python_version": python_version,
        "install_path": str(pkg_location),
        "source_repo": "https://github.com/viragtripathi/crdb-dump"
    }

    if as_json:
        click.echo(json.dumps(metadata, indent=2))
    else:
        click.echo(f"📦 crdb-dump version : {metadata['version']}")
        click.echo(f"🔀 Git commit        : {metadata['git_commit']}")
        click.echo(f"📅 Build date        : {metadata['build_date']}")
        click.echo(f"🐍 Python version    : {metadata['python_version']}")
        click.echo(f"📂 Install path      : {metadata['install_path']}")
        click.echo(f"🔗 Source repo       : {metadata['source_repo']}")


if __name__ == '__main__':
    main()
