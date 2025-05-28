from difflib import unified_diff


def diff_schemas(file1, file2):
    with open(file1) as f1, open(file2) as f2:
        lines1 = f1.readlines()
        lines2 = f2.readlines()
        return ''.join(unified_diff(lines1, lines2, fromfile=file1, tofile=file2))
