import os
import tarfile


def write_file(path, content):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        f.write(content)
    print(f"Wrote: {path}")


def archive_output(directory):
    archive_name = f"{directory}.tar.gz"
    with tarfile.open(archive_name, "w:gz") as tar:
        tar.add(directory, arcname=os.path.basename(directory))
    print(f"📦 Archived output to {archive_name}")
