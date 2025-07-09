
import argparse
from pg_compose_cli.compare import compare_sources
from pg_compose_cli.alter_generator import generate_alter_commands

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
        "--dry-run",
        action="store_true",
        help="Show what would be deployed without actually applying changes"
    )
    parser.add_argument(
        "--grants",
        action="store_true",
        help="Include GRANT/REVOKE statements in comparison and migration"
    )

    args = parser.parse_args()

    result = compare_sources(
        args.source_a,
        args.source_b,
        schemas=args.schemas,
        verbose=not args.quiet,
        grants=args.grants
    )

    if args.deploy:
        alter_commands = generate_alter_commands(result)
        
        if args.dry_run:
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
