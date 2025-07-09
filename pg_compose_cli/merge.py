from pg_compose_cli.extract import extract_build_queries
from pg_compose_cli.sorter import sort_queries
import os
from typing import Literal

def reorder_sql_file(
    input_path: str,
    output_path: str = None,
    mode: Literal['write', 'append'] = 'write',
    verbose: bool = False
):
    # Read SQL input
    with open(input_path, "r", encoding="utf-8") as f:
        sql = f.read()

    # Extract and sort
    sorted_queries = sort_queries(extract_build_queries(sql))

    # Determine output path
    output_path = output_path or input_path.replace(".sql", "_sorted.sql")

    # Open file in specified mode
    file_mode = 'w' if mode == 'write' else 'a'

    with open(output_path, file_mode, encoding="utf-8") as f:
        for action in sorted_queries:
            query_text = sql[action["query_start_pos"]:action["query_end_pos"]]
            f.write(query_text.strip() + ";\n\n")

            if verbose:
                print(f"→ {action['query_type']}")
                if action.get("object_name"):
                    print(f"   object: {action['object_name']}")
                if action.get("dependencies"):
                    print("   depends on:")
                    for d in action["dependencies"]:
                        print(f"     - {d}")
                print("-" * 40)

    if verbose:
        print(f"\n✅ Reordered SQL written to: {output_path}")


def merge_sql(
    base_dir: str,
    output_dir: str='',
    *sub_dirs: str
):
    first = True
    output_path = os.path.join(output_dir, "sorted.sql")

    for root, dirs, files in os.walk(base_dir):
        # Filter to specified subdirectories if any
        if sub_dirs and not any(sub in root for sub in sub_dirs):
            continue

        for file in sorted(files):
            if file.endswith(".sql"):
                input_path = os.path.join(root, file)

                reorder_sql_file(
                    input_path=input_path,
                    output_path=output_path,  # unified target file
                    mode='write' if first else 'append',
                )
                first = False


