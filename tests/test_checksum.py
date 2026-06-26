import hashlib
import json
import logging
from crdb_dump.verify.checksum import verify_checksums


def _logger_capturing(records):
    class ListHandler(logging.Handler):
        def emit(self, record):
            records.append(record.getMessage())

    logger = logging.getLogger("checksum_test")
    logger.handlers = []
    logger.setLevel(logging.INFO)
    logger.addHandler(ListHandler())
    return logger


def test_verify_uses_three_part_manifest(tmp_path):
    base = "cp.cpkit.tasks"
    data_file = tmp_path / f"{base}_001.sql"
    data_file.write_text("INSERT INTO x VALUES (1);\n")
    h = hashlib.sha256(data_file.read_bytes()).hexdigest()
    (tmp_path / f"{base}.manifest.json").write_text(json.dumps(
        {"table": base, "region": "N/A",
         "chunks": [{"file": f"{base}_001.sql", "rows": 1, "sha256": h}]}))

    records = []
    opts = {"tables": "cp.cpkit.tasks", "db": "cp", "retry_count": 1, "retry_delay": 0}
    verify_checksums(opts, str(tmp_path), _logger_capturing(records))

    joined = "\n".join(records)
    assert "No manifest found" not in joined
    assert "Verified" in joined
