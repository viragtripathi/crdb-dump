import random
import re
import time
import random
import functools
import psycopg2
from sqlalchemy import exc
from crdb_dump.utils.type_constants import NOT_NULL_MIN, NOT_NULL_MAX, DEFAULT_ARRAY_COUNT


RETRYABLE_EXCEPTIONS = (psycopg2.OperationalError, exc.OperationalError)

def retry(retries=3, delay=1.0, backoff=2.0, exceptions=RETRYABLE_EXCEPTIONS):
    def decorator_retry(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            for attempt in range(retries):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == retries - 1:
                        raise
                    sleep = current_delay + random.uniform(0, 0.3)
                    print(f"[Retry] Attempt {attempt + 1} failed: {e}. Retrying in {sleep:.2f}s...")
                    time.sleep(sleep)
                    current_delay *= backoff
        return wrapper
    return decorator_retry

def to_sql_literal(val):
    if val is None:
        return 'NULL'

    if isinstance(val, memoryview):
        return f"decode('{val.tobytes().hex()}', 'hex')"

    if isinstance(val, (bytes, bytearray)):
        return f"decode('{val.hex()}', 'hex')"

    if isinstance(val, list):
        def serialize_item(v):
            if v is None:
                return 'NULL'
            if isinstance(v, str):
                escaped = v.replace("'", "''")
                # Only quote if special characters
                if re.search(r'[,\s{}"]', escaped):
                    return f'"{escaped}"'
                return escaped
            return str(v)

        items = [serialize_item(item) for item in val]
        return f"'{{{','.join(items)}}}'"

    if isinstance(val, str):
        escaped = val.replace("'", "''")
        return f"'{escaped}'"

    return str(val)

def to_csv_literal(val):
    if isinstance(val, memoryview):
        return val.tobytes().hex()
    if isinstance(val, (bytes, bytearray)):
        return val.hex()
    if isinstance(val, list):
        def escape_csv_array_item(item):
            if item is None:
                return ''
            if isinstance(item, str):
                escaped = item.replace('"', '""')
                if re.search(r'[\s,{}"]', escaped):
                    return f'"{escaped}"'
                return escaped
            return str(item)

        return '{' + ','.join(escape_csv_array_item(v) for v in val) + '}'
    return val

def to_json_literal(val):
    if isinstance(val, memoryview):
        return val.tobytes().hex()
    if isinstance(val, (bytes, bytearray)):
        return val.hex()
    if isinstance(val, set):
        return list(val)
    if isinstance(val, dict):
        return {k: to_json_literal(v) for k, v in val.items()}
    if isinstance(val, list):
        return [to_json_literal(v) for v in val]
    return val

def get_type_and_args(col_type_and_args: list):
    col_type_and_args = [x.lower() for x in col_type_and_args]  # Normalize early

    is_not_null = "not" in col_type_and_args and "null" in col_type_and_args
    is_array = "array" in col_type_and_args or any(x.endswith("[]") for x in col_type_and_args)

    datatype = col_type_and_args[0].replace("[]", "")
    arg = col_type_and_args[1:] if len(col_type_and_args) > 1 else None

    null_pct = 0.0 if is_not_null else round(random.randint(NOT_NULL_MIN, NOT_NULL_MAX) / 100, 2)
    array_count = DEFAULT_ARRAY_COUNT if is_array else 0

    if datatype in ["bool", "boolean"]:
        return {"type": "bool", "args": {"seed": random.randint(0, 100), "null_pct": null_pct, "array": array_count}}

    if datatype in ["int2", "smallint", "int4", "int8", "int64", "bigint", "int", "integer"]:
        limits = {
            "int2": (-32767, 32767),
            "int4": (-(2**31) + 1, (2**31) - 1),
        }
        int_min, int_max = limits.get(datatype, (-(2**63) + 1, (2**63) - 1))
        return {"type": "integer", "args": {"min": int_min, "max": int_max, "seed": random.randint(0, 100), "null_pct": null_pct, "array": array_count}}

    if datatype in ["string", "char", "character", "varchar", "text", "clob"]:
        _min, _max = 10, 30
        if arg and arg[0].isdigit():
            _min = int(arg[0]) // 3 + 1
            _max = int(arg[0])
        return {"type": "string", "args": {"min": _min, "max": _max, "prefix": "", "seed": random.randint(0, 100), "null_pct": null_pct, "array": array_count}}

    if datatype in ["decimal", "float", "float4", "float8", "dec", "numeric", "real", "double"]:
        _min, _max, _round = 0, 10000000, 2
        if arg:
            if ":" in arg[0]:
                prec, scale = arg[0].split(":")
                if prec: _max = 10 ** (int(prec) - int(scale))
                if scale: _round = int(scale)
            elif arg[0].isdigit():
                _max = 10 ** int(arg[0])
                _round = 0
        return {"type": "float", "args": {"min": _min, "max": _max, "round": _round, "seed": random.randint(0, 100), "null_pct": null_pct, "array": array_count}}

    if datatype in ["time", "timetz"]:
        return {"type": "time", "args": {"start": "07:30:00", "end": "15:30:00", "micros": False, "seed": random.randint(0, 100), "null_pct": null_pct, "array": array_count}}

    if datatype in ["json", "jsonb"]:
        return {"type": "json", "args": {"min": 10, "max": 50, "seed": random.randint(0, 100), "null_pct": null_pct}}

    if datatype == "date":
        return {"type": "date", "args": {"start": "2000-01-01", "end": "2024-12-31", "format": "%Y-%m-%d", "seed": random.randint(0, 100), "null_pct": null_pct, "array": array_count}}

    if datatype in ["timestamp", "timestamptz"]:
        return {"type": "timestamp", "args": {"start": "2000-01-01", "end": "2024-12-31", "format": "%Y-%m-%d %H:%M:%S.%f", "seed": random.randint(0, 100), "null_pct": null_pct, "array": array_count}}

    if datatype == "uuid":
        return {"type": "uuid", "args": {"seed": random.randint(0, 100), "null_pct": null_pct, "array": array_count}}

    if datatype in ["bit", "varbit"]:
        _size = 1
        if arg and arg[0].isdigit():
            _size = int(arg[0])
        return {"type": "bit", "args": {"size": _size, "seed": random.randint(0, 100), "null_pct": null_pct, "array": array_count}}

    if datatype in ["bytes", "blob", "bytea"]:
        return {"type": "bytes", "args": {"size": 20, "seed": random.randint(0, 100), "null_pct": null_pct, "array": array_count}}

    raise ValueError(f"Unsupported type: {datatype}")

