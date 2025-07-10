
import argparse
from pg_compose_core.lib.compare import compare_sources
from pg_compose_core.lib.ast_objects import ASTList

# Global verbosity setting
VERBOSE = False

def main():
    parser = argparse.ArgumentParser(
        description="pg-compose: compare PostgreSQL schemas from files or connections"
    )
    parser.add_argument("source_a", help="First source (.sql, .json, raw SQL, git@repo.git, or postgres:// URI)")
    parser.add_argument("source_b", help="Second source to compare against")
    parser.add_argument(
        "--schemas",
        nargs="+",
        help="Restrict comparison to specific schemas (only applies to postgres:// sources)"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output (show detailed logs)"
    )

    parser.add_argument(
        "--deploy",
        help="Deploy schema changes to specified file (future: database connection)"
    )
    parser.add_argument(
        "--prod",
        action="store_true",
        help="Deploy to production (default is preview mode)"
    )
    parser.add_argument(
        "--grants",
        action="store_true",
        help="Include GRANT/REVOKE statements in comparison and migration"
    )
    parser.add_argument(
        "--no-grants",
        action="store_true",
        help="Exclude GRANT/REVOKE statements from comparison and migration"
    )
    parser.add_argument(
        "--use-ast-objects",
        action="store_true",
        help="Use ASTObject instances instead of dictionaries (experimental)"
    )
    parser.add_argument(
        "--output-format",
        choices=["sql", "json", "ast"],
        default="sql",
        help="Output format for deployment (default: sql)"
    )

    args = parser.parse_args()

    # Set global verbosity
    global VERBOSE
    VERBOSE = args.verbose

    # Handle grants flag logic
    grants = False  # Default to excluding grants
    if args.grants:
        grants = True  # Explicitly include grants
    elif args.no_grants:
        grants = False  # Explicitly exclude grants

    if args.deploy and args.use_ast_objects:
        # Use new ASTList-based alter command generation
        from pg_compose_core.lib.deploy import diff_sort
        alter_ast_list = diff_sort(
            source_a=args.source_a,
            source_b=args.source_b,
            schemas=args.schemas,
            grants=grants
        )
        
        # Always write to file when --deploy is specified
        with open(args.deploy, 'w') as f:
            if args.output_format == "sql":
                f.write(alter_ast_list.to_sql())
            elif args.output_format == "json":
                import json
                json.dump(alter_ast_list.to_dict_list(), f, indent=2)
            elif args.output_format == "ast":
                # Output ASTObject representations
                for obj in alter_ast_list:
                    f.write(f"{obj}\n")
        
        if not args.prod:
            if VERBOSE:
                print("DRY RUN - Preview of commands that would be deployed:")
                print("=" * 50)
                # Show first 5 commands and last 5 commands if there are more than 10
                if len(alter_ast_list) <= 10:
                    for i, cmd in enumerate(alter_ast_list, 1):
                        # Truncate long commands
                        preview_cmd = cmd.command[:100] + "..." if len(cmd.command) > 100 else cmd.command
                        print(f"{i}. {preview_cmd}")
                else:
                    for i, cmd in enumerate(alter_ast_list[:5], 1):
                        # Truncate long commands
                        preview_cmd = cmd.command[:100] + "..." if len(cmd.command) > 100 else cmd.command
                        print(f"{i}. {preview_cmd}")
                    print(f"... ({len(alter_ast_list) - 10} more commands) ...")
                    for i, cmd in enumerate(alter_ast_list[-5:], len(alter_ast_list) - 4):
                        # Truncate long commands
                        preview_cmd = cmd.command[:100] + "..." if len(cmd.command) > 100 else cmd.command
                        print(f"{i}. {preview_cmd}")
                print("=" * 50)
            print(f"Total: {len(alter_ast_list)} commands")
            print(f"Commands written to: {args.deploy} (preview mode)")
        else:
            print(f"Deployment commands written to: {args.deploy} (production mode)")
    else:
        # Use traditional dict-based approach
        result = compare_sources(
            args.source_a,
            args.source_b,
            schemas=args.schemas,
            grants=grants,
            use_ast_objects=args.use_ast_objects
        )

        if args.deploy:
            if args.use_ast_objects:
                # result is an ASTList, convert to SQL
                # Always write to file when --deploy is specified
                with open(args.deploy, 'w') as f:
                    if args.output_format == "sql":
                        f.write(result.to_sql())
                    elif args.output_format == "json":
                        import json
                        json.dump(result.to_dict_list(), f, indent=2)
                    elif args.output_format == "ast":
                        for obj in result:
                            f.write(f"{obj}\n")
                
                if not args.prod:
                    if VERBOSE:
                        print("DRY RUN - Preview of commands that would be deployed:")
                        print("=" * 50)
                        # Show first 5 commands and last 5 commands if there are more than 10
                        if len(result) <= 10:
                            for i, obj in enumerate(result, 1):
                                # Truncate long commands
                                preview_cmd = obj.command[:100] + "..." if len(obj.command) > 100 else obj.command
                                print(f"{i}. {preview_cmd}")
                        else:
                            for i, obj in enumerate(result[:5], 1):
                                # Truncate long commands
                                preview_cmd = obj.command[:100] + "..." if len(obj.command) > 100 else obj.command
                                print(f"{i}. {preview_cmd}")
                            print(f"... ({len(result) - 10} more commands) ...")
                            for i, obj in enumerate(result[-5:], len(result) - 4):
                                # Truncate long commands
                                preview_cmd = obj.command[:100] + "..." if len(obj.command) > 100 else obj.command
                                print(f"{i}. {preview_cmd}")
                        print("=" * 50)
                    print(f"Total: {len(result)} commands")
                    print(f"Commands written to: {args.deploy} (preview mode)")
                else:
                    print(f"Deployment commands written to: {args.deploy} (production mode)")
            else:
                # Traditional dict-based approach
                from pg_compose_core.lib.alter_generator import generate_alter_commands
                alter_commands = generate_alter_commands(result)
                
                # Always write to file when --deploy is specified
                with open(args.deploy, 'w') as f:
                    f.write('\n'.join(alter_commands))
                
                if not args.prod:
                    if VERBOSE:
                        print("DRY RUN - Preview of commands that would be deployed:")
                        print("=" * 50)
                        # Show first 5 commands and last 5 commands if there are more than 10
                        if len(alter_commands) <= 10:
                            for i, cmd in enumerate(alter_commands, 1):
                                # Truncate long commands
                                preview_cmd = cmd[:100] + "..." if len(cmd) > 100 else cmd
                                print(f"{i}. {preview_cmd}")
                        else:
                            for i, cmd in enumerate(alter_commands[:5], 1):
                                # Truncate long commands
                                preview_cmd = cmd[:100] + "..." if len(cmd) > 100 else cmd
                                print(f"{i}. {preview_cmd}")
                            print(f"... ({len(alter_commands) - 10} more commands) ...")
                            for i, cmd in enumerate(alter_commands[-5:], len(alter_commands) - 4):
                                # Truncate long commands
                                preview_cmd = cmd[:100] + "..." if len(cmd) > 100 else cmd
                                print(f"{i}. {preview_cmd}")
                        print("=" * 50)
                    print(f"Total: {len(alter_commands)} commands")
                    print(f"Commands written to: {args.deploy} (preview mode)")
                else:
                    print(f"Deployment commands written to: {args.deploy} (production mode)")
        else:
            # Regular diff output (no --deploy specified)
            if args.use_ast_objects:
                # result is an ASTList
                if VERBOSE:
                    print("Schema differences found:")
                    print("=" * 50)
                    # Show first 5 commands and last 5 commands if there are more than 10
                    if len(result) <= 10:
                        for i, obj in enumerate(result, 1):
                            # Truncate long commands
                            preview_cmd = obj.command[:100] + "..." if len(obj.command) > 100 else obj.command
                            print(f"{i}. {preview_cmd}")
                    else:
                        for i, obj in enumerate(result[:5], 1):
                            # Truncate long commands
                            preview_cmd = obj.command[:100] + "..." if len(obj.command) > 100 else obj.command
                            print(f"{i}. {preview_cmd}")
                        print(f"... ({len(result) - 10} more commands) ...")
                        for i, obj in enumerate(result[-5:], len(result) - 4):
                            # Truncate long commands
                            preview_cmd = obj.command[:100] + "..." if len(obj.command) > 100 else obj.command
                            print(f"{i}. {preview_cmd}")
                    print("=" * 50)
                print(f"Total: {len(result)} differences found")
            else:
                # Traditional dict-based approach
                from pg_compose_core.lib.alter_generator import generate_alter_commands
                alter_commands = generate_alter_commands(result)
                
                if VERBOSE:
                    print("Schema differences found:")
                    print("=" * 50)
                    # Show first 5 commands and last 5 commands if there are more than 10
                    if len(alter_commands) <= 10:
                        for i, cmd in enumerate(alter_commands, 1):
                            # Truncate long commands
                            preview_cmd = cmd[:100] + "..." if len(cmd) > 100 else cmd
                            print(f"{i}. {preview_cmd}")
                    else:
                        for i, cmd in enumerate(alter_commands[:5], 1):
                            # Truncate long commands
                            preview_cmd = cmd[:100] + "..." if len(cmd) > 100 else cmd
                            print(f"{i}. {preview_cmd}")
                        print(f"... ({len(alter_commands) - 10} more commands) ...")
                        for i, cmd in enumerate(alter_commands[-5:], len(alter_commands) - 4):
                            # Truncate long commands
                            preview_cmd = cmd[:100] + "..." if len(cmd) > 100 else cmd
                            print(f"{i}. {preview_cmd}")
                    print("=" * 50)
                print(f"Total: {len(alter_commands)} differences found")

if __name__ == "__main__":
    main()
