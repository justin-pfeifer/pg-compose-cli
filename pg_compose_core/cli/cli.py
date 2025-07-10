
import argparse
from pg_compose_core.lib.compare import compare_sources
from pg_compose_core.lib.ast_objects import ASTList

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
        "--quiet",
        action="store_true",
        help="Suppress stdout output (useful for programmatic use)"
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

    # Handle grants flag logic
    grants = False  # Default to excluding grants
    if args.grants:
        grants = True  # Explicitly include grants
    elif args.no_grants:
        grants = False  # Explicitly exclude grants

    if args.deploy and args.use_ast_objects:
        # Use new ASTList-based alter command generation
        alter_ast_list = compare_sources(
            args.source_a,
            args.source_b,
            schemas=args.schemas,
            grants=grants,
            use_ast_objects=True,
            verbose=not args.quiet
        )
        
        if not args.prod:
            print("DRY RUN - Would deploy the following commands:")
            print("=" * 50)
            for i, cmd in enumerate(alter_ast_list, 1):
                print(f"{i}. {cmd.command}")
            print("=" * 50)
            print(f"Total: {len(alter_ast_list)} commands")
        else:
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
            print(f"Deployment commands written to: {args.deploy}")
    else:
        # Use traditional dict-based approach
        result = compare_sources(
            args.source_a,
            args.source_b,
            schemas=args.schemas,
            verbose=not args.quiet,
            grants=grants,
            use_ast_objects=args.use_ast_objects
        )

        if args.deploy:
            if args.use_ast_objects:
                # result is an ASTList, convert to SQL
                if not args.prod:
                    print("DRY RUN - Would deploy the following commands:")
                    print("=" * 50)
                    for i, obj in enumerate(result, 1):
                        print(f"{i}. {obj.command}")
                    print("=" * 50)
                    print(f"Total: {len(result)} commands")
                else:
                    with open(args.deploy, 'w') as f:
                        if args.output_format == "sql":
                            f.write(result.to_sql())
                        elif args.output_format == "json":
                            import json
                            json.dump(result.to_dict_list(), f, indent=2)
                        elif args.output_format == "ast":
                            for obj in result:
                                f.write(f"{obj}\n")
                    print(f"Deployment commands written to: {args.deploy}")
            else:
                # Traditional dict-based approach
                from pg_compose_core.lib.alter_generator import generate_alter_commands
                alter_commands = generate_alter_commands(result)
                
                if not args.prod:
                    print("DRY RUN - Would deploy the following commands:")
                    print("=" * 50)
                    for i, cmd in enumerate(alter_commands, 1):
                        print(f"{i}. {cmd}")
                    print("=" * 50)
                    print(f"Total: {len(alter_commands)} commands")
                else:
                    with open(args.deploy, 'w') as f:
                        f.write('\n'.join(alter_commands))
                    print(f"Deployment commands written to: {args.deploy}")

if __name__ == "__main__":
    main()
