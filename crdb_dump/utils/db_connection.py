import os
from sqlalchemy import create_engine
import psycopg2


def get_sqlalchemy_engine(opts=None):
    url = os.getenv("CRDB_URL")
    if url:
        if url.startswith("postgresql://"):
            url = url.replace("postgresql://", "cockroachdb://", 1)
        return create_engine(url)

    if opts is None:
        # fallback default to local instance
        return create_engine("cockroachdb://root@localhost:26257/defaultdb?sslmode=disable")

    base = f"cockroachdb://root@{opts.get('host', 'localhost')}:{opts.get('port', 26257)}/{opts['db']}"
    if opts.get("certs_dir"):
        base += (
            f"?sslmode=verify-full"
            f"&sslrootcert={opts['certs_dir']}/ca.crt"
            f"&sslcert={opts['certs_dir']}/client.root.crt"
            f"&sslkey={opts['certs_dir']}/client.root.key"
        )
    else:
        base += "?sslmode=disable"

    return create_engine(base)


def get_psycopg_connection(opts=None):
    url = os.getenv("CRDB_URL")
    if url:
        pg_url = url.replace("cockroachdb://", "postgresql://", 1)
        return psycopg2.connect(pg_url)

    opts = opts or {}
    host = opts.get("host", "localhost")
    port = opts.get("port", 26257)
    db = opts.get("db", "defaultdb")
    base = f"postgresql://root@{host}:{port}/{db}"
    if opts.get("certs_dir"):
        base += (
            f"?sslmode=verify-full"
            f"&sslrootcert={opts['certs_dir']}/ca.crt"
            f"&sslcert={opts['certs_dir']}/client.root.crt"
            f"&sslkey={opts['certs_dir']}/client.root.key"
        )
    else:
        base += "?sslmode=disable"
    return psycopg2.connect(base)
