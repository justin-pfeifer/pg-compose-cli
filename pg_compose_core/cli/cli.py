
import argparse
import logging
from pg_compose_core.lib.parser import load_source
from pg_compose_core.lib.diff import diff_schemas
from pg_compose_core.lib.ast import ASTList


def write_to_file(commands, filename, output_format):
    """Write commands to file in the specified format."""
    with open(filename, 'w') as f:
        if output_format == "sql":
            if hasattr(commands, 'to_sql'):
                f.write(commands.to_sql())
            else:
                f.write('\n'.join(commands))
        elif output_format == "json":
            import json
            if hasattr(commands, 'to_dict_list'):
                json.dump(commands.to_dict_list(), f, indent=2)
            else:
                json.dump(commands, f, indent=2)
        elif output_format == "ast":
            for obj in commands:
                f.write(f"{obj}\n")


def preview_commands(commands, title="Commands"):
    """Show a preview of commands with truncation for long ones."""
    logging.info(f"{title}:")
    logging.info("=" * 50)
    
    # Convert to list if it's an ASTList
    if hasattr(commands, '__iter__') and not isinstance(commands, (list, tuple)):
        commands = list(commands)
    
    # Show first 5 commands and last 5 commands if there are more than 10
    if len(commands) <= 10:
        for i, cmd in enumerate(commands, 1):
            # Get command text
            if hasattr(cmd, 'command'):
                cmd_text = cmd.command
            else:
                cmd_text = str(cmd)
            # Truncate long commands
            preview_cmd = cmd_text[:100] + "..." if len(cmd_text) > 100 else cmd_text
            logging.info(f"{i}. {preview_cmd}")
    else:
        for i, cmd in enumerate(commands[:5], 1):
            # Get command text
            if hasattr(cmd, 'command'):
                cmd_text = cmd.command
            else:
                cmd_text = str(cmd)
            # Truncate long commands
            preview_cmd = cmd_text[:100] + "..." if len(cmd_text) > 100 else cmd_text
            logging.info(f"{i}. {preview_cmd}")
        logging.info(f"... ({len(commands) - 10} more commands) ...")
        for i, cmd in enumerate(commands[-5:], len(commands) - 4):
            # Get command text
            if hasattr(cmd, 'command'):
                cmd_text = cmd.command
            else:
                cmd_text = str(cmd)
            # Truncate long commands
            preview_cmd = cmd_text[:100] + "..." if len(cmd_text) > 100 else cmd_text
            logging.info(f"{i}. {preview_cmd}")
    logging.info("=" * 50)


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
        action="count",
        default=0,
        help="Increase verbosity (-v for INFO, -vv for DEBUG, -vvv for TRACE)"
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
        "--output-format",
        choices=["sql", "json", "ast"],
        default="sql",
        help="Output format for deployment (default: sql)"
    )

    args = parser.parse_args()

    # Set log level based on verbosity count
    if args.verbose == 0:
        log_level = logging.WARNING
    elif args.verbose == 1:
        log_level = logging.INFO
    elif args.verbose == 2:
        log_level = logging.DEBUG
    else:
        log_level = logging.DEBUG  # Max verbosity
    
    # Configure logging
    logging.basicConfig(
        level=log_level,
        format='%(levelname)s: %(message)s',
        datefmt='%H:%M:%S'
    )

    # Handle grants flag logic
    grants = False  # Default to excluding grants
    if args.grants:
        grants = True  # Explicitly include grants
    elif args.no_grants:
        grants = False  # Explicitly exclude grants

    # Get the diff result
    if args.deploy:
        # Use new ASTList-based alter command generation
        from pg_compose_core.lib.deploy import diff_sort
        result = diff_sort(
            source_a=args.source_a,
            source_b=args.source_b,
            schemas=args.schemas,
            grants=grants
        )
    else:
        # Use simplified comparison approach
        schema_a = load_source(args.source_a, schemas=args.schemas, grants=grants)
        schema_b = load_source(args.source_b, schemas=args.schemas, grants=grants)
        result = diff_schemas(schema_a, schema_b)

    # Handle deployment or diff output
    if args.deploy:
        # Write to file
        write_to_file(result, args.deploy, args.output_format)
        
        # Show preview if not production
        if not args.prod:
            preview_commands(result, "DRY RUN - Preview of commands that would be deployed")
            logging.info(f"Total: {len(result)} commands")
            print(f"Commands written to: {args.deploy} (preview mode)")
        else:
            print(f"Deployment commands written to: {args.deploy} (production mode)")
    else:
        # Show diff output
        preview_commands(result, "Schema differences found")
        logging.info(f"Total: {len(result)} differences found")


if __name__ == "__main__":
    main()
