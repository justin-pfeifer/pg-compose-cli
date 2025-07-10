from pg_compose_cli.extract import extract_build_queries
from pg_compose_cli.ast_objects import ASTList
import os
from typing import Literal

def reorder_sql_file(
    input_path: str,
    output_path: str = None,
    mode: Literal['write', 'append'] = 'write',
    verbose: bool = False
) -> ASTList:
    """Reorder SQL file and return ASTList of sorted objects."""
    # Read SQL input
    with open(input_path, "r", encoding="utf-8") as f:
        sql = f.read()

    # Extract and create ASTList
    ast_objects = extract_build_queries(sql, use_ast_objects=True)
    ast_list = ASTList(ast_objects)
    
    # Sort the ASTList
    sorted_ast_list = ast_list.sort()

    # Determine output path
    output_path = output_path or input_path.replace(".sql", "_sorted.sql")

    # Open file in specified mode
    file_mode = 'w' if mode == 'write' else 'a'

    with open(output_path, file_mode, encoding="utf-8") as f:
        f.write(sorted_ast_list.to_sql())

        if verbose:
            for obj in sorted_ast_list:
                print(f"→ {obj.query_type.value}")
                if obj.object_name:
                    print(f"   object: {obj.object_name}")
                if obj.dependencies:
                    print("   depends on:")
                    for d in obj.dependencies:
                        print(f"     - {d}")
                print("-" * 40)

    if verbose:
        print(f"\n✅ Reordered SQL written to: {output_path}")
    
    return sorted_ast_list


def merge_sql(
    base_dir: str,
    output_dir: str='',
    *sub_dirs: str
) -> ASTList:
    """Merge SQL files from directory structure and return combined ASTList."""
    merged_ast_list = ASTList()
    output_path = os.path.join(output_dir, "sorted.sql") if output_dir else None

    for root, dirs, files in os.walk(base_dir):
        # Filter to specified subdirectories if any
        if sub_dirs and not any(sub in root for sub in sub_dirs):
            continue

        for file in sorted(files):
            if file.endswith(".sql"):
                input_path = os.path.join(root, file)
                
                # Extract and sort this file
                ast_objects = extract_build_queries(
                    open(input_path, "r", encoding="utf-8").read(),
                    use_ast_objects=True
                )
                file_ast_list = ASTList(ast_objects).sort()
                
                # Merge with accumulated results
                merged_ast_list = merged_ast_list.merge(file_ast_list)

    # Final sort of merged results
    final_ast_list = merged_ast_list.sort()
    
    # Write to output file if specified
    if output_path:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(final_ast_list.to_sql())
        print(f"✅ Merged SQL written to: {output_path}")
    
    return final_ast_list


