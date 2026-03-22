import argparse

from domain_contract_generator import contract
from domain_schema_generator import domain
from simplemdm.generators.python_library import generate_library
from simplemdm.generators.spark_script import build_spark_from_schema
from simplemdm.generators.sqlite_db import build_db_from_schema
from simplemdm.services.generation import generate_all_from_schema


def _schema_command(args: argparse.Namespace) -> int:
    d = domain(args.name, destination=args.destination)
    d.create(version=args.version)

    if args.key:
        d.add_key(*args.key)
    if args.add_status:
        d.add_status(key_type=args.status_key_type)
    if args.add_type:
        d.add_type(key_type=args.type_key_type)
    if args.add_attribute:
        d.add_attribute()
    if args.add_hierarchy:
        d.add_hierarchy()
    for other in args.relationship:
        d.add_relationship(other)

    schema_path = d.save()
    print(schema_path)
    return 0


def _contract_command(args: argparse.Namespace) -> int:
    c = contract(args.name)
    c.create()

    if args.owner_name or args.owner_team or args.owner_email:
        if not (args.owner_name and args.owner_team and args.owner_email):
            raise ValueError("owner_name, owner_team, and owner_email must all be supplied together")
        c.set_owner(args.owner_name, args.owner_team, args.owner_email)

    if args.sla_freshness or args.sla_availability:
        if not (args.sla_freshness and args.sla_availability):
            raise ValueError("sla_freshness and sla_availability must both be supplied together")
        c.set_sla(args.sla_freshness, args.sla_availability)

    for consumer in args.consumer:
        name, team = consumer.split(":", 1)
        c.add_consumer(name, team)

    if args.status:
        c.set_status(args.status)

    contract_path = c.save()
    print(contract_path)
    return 0


def _sqlite_command(args: argparse.Namespace) -> int:
    print(build_db_from_schema(args.schema_path))
    return 0


def _spark_command(args: argparse.Namespace) -> int:
    print(build_spark_from_schema(args.schema_path, catalog=args.catalog, schema=args.schema))
    return 0


def _library_command(args: argparse.Namespace) -> int:
    print(generate_library(args.schema_path))
    return 0


def _all_command(args: argparse.Namespace) -> int:
    outputs = generate_all_from_schema(
        args.schema_path,
        catalog=args.catalog,
        spark_schema=args.schema,
    )
    for key, value in outputs.items():
        print(f"{key}: {value}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="simplemdm", description="simpleMDM command line tools")
    subparsers = parser.add_subparsers(dest="command", required=True)

    schema_parser = subparsers.add_parser("schema", help="Create and extend a domain schema")
    schema_parser.add_argument("name", help="Domain name in PascalCase")
    schema_parser.add_argument("--destination", default="domains", help="Output root path")
    schema_parser.add_argument("--version", default="1.0.0", help="Schema version")
    schema_parser.add_argument("--key", nargs="+", help="Natural key field(s)")
    schema_parser.add_argument("--add-status", action="store_true", help="Add status reference table")
    schema_parser.add_argument("--status-key-type", default="uuid", choices=["uuid", "integer", "string"])
    schema_parser.add_argument("--add-type", action="store_true", help="Add type reference table")
    schema_parser.add_argument("--type-key-type", default="uuid", choices=["uuid", "integer", "string"])
    schema_parser.add_argument("--add-attribute", action="store_true", help="Add attribute + value bridge tables")
    schema_parser.add_argument("--add-hierarchy", action="store_true", help="Add hierarchy + hierarchy value tables")
    schema_parser.add_argument(
        "--relationship",
        action="append",
        default=[],
        help="Add relationship table to another domain (repeatable)",
    )
    schema_parser.set_defaults(func=_schema_command)

    contract_parser = subparsers.add_parser("contract", help="Create and extend a data contract")
    contract_parser.add_argument("name", help="Domain name in PascalCase")
    contract_parser.add_argument("--owner-name")
    contract_parser.add_argument("--owner-team")
    contract_parser.add_argument("--owner-email")
    contract_parser.add_argument("--sla-freshness")
    contract_parser.add_argument("--sla-availability")
    contract_parser.add_argument(
        "--consumer",
        action="append",
        default=[],
        help="Consumer in NAME:TEAM format (repeatable)",
    )
    contract_parser.add_argument("--status", choices=["draft", "active", "deprecated"])
    contract_parser.set_defaults(func=_contract_command)

    sqlite_parser = subparsers.add_parser("sqlite", help="Generate SQLite DB from schema")
    sqlite_parser.add_argument("schema_path", help="Path to schema JSON")
    sqlite_parser.set_defaults(func=_sqlite_command)

    spark_parser = subparsers.add_parser("spark", help="Generate Spark script from schema")
    spark_parser.add_argument("schema_path", help="Path to schema JSON")
    spark_parser.add_argument("--catalog", default="main")
    spark_parser.add_argument("--schema", dest="schema", default=None)
    spark_parser.set_defaults(func=_spark_command)

    library_parser = subparsers.add_parser("library", help="Generate Python library from schema")
    library_parser.add_argument("schema_path", help="Path to schema JSON")
    library_parser.set_defaults(func=_library_command)

    all_parser = subparsers.add_parser("all", help="Generate sqlite, spark, and library artifacts")
    all_parser.add_argument("schema_path", help="Path to schema JSON")
    all_parser.add_argument("--catalog", default="main")
    all_parser.add_argument("--schema", dest="schema", default=None)
    all_parser.set_defaults(func=_all_command)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
