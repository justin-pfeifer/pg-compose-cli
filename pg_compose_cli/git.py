
import tempfile
import subprocess
import os
from pg_compose_cli.merge import reorder_sql_file
from pg_compose_cli.extract import extract_build_queries

def extract_from_git_repo(repo_url: str) -> list[dict]:
    with tempfile.TemporaryDirectory() as tmp_dir:
        subprocess.run(["git", "clone", "--depth", "1", repo_url, tmp_dir], check=True)
        sorted_path = os.path.join(tmp_dir, "sorted.sql")
        reorder_sql_file(tmp_dir, sorted_path)
        with open(sorted_path, "r", encoding="utf-8") as f:
            return extract_build_queries(f.read())
