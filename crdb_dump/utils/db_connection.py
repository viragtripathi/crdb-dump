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


def get_psycopg_connection():
    url = os.getenv("CRDB_URL")
    if not url:
        url = "cockroachdb://root@localhost:26257/defaultdb?sslmode=disable"
    pg_url = url.replace("cockroachdb://", "postgresql://")
    return psycopg2.connect(pg_url)
