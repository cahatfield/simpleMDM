import pathlib
import sqlite3

from simplemdm.core.schema_core import foreign_keys, identity_field, sqlite_type
from simplemdm.parsers.schema_parser import TableSpec, parse_schema_file


def build_table_sql(table: TableSpec) -> str:
    properties = table.properties
    identity = identity_field({"x-key": table.x_key, "properties": properties})
    composite_key = table.x_key.get("composite_key", [])

    columns = []
    fk_constraints = []

    for field, prop in properties.items():
        col_type = sqlite_type(prop)
        not_null = "NOT NULL" if field in table.required else ""

        if field == identity:
            columns.append(f"    {field} {col_type} PRIMARY KEY")
        else:
            columns.append(f"    {field} {col_type} {not_null}".rstrip())

    for field, ref_table, ref_field in foreign_keys({"properties": properties}):
        fk_constraints.append(f"    FOREIGN KEY ({field}) REFERENCES {ref_table}({ref_field})")

    if composite_key:
        columns.append(f"    PRIMARY KEY ({', '.join(composite_key)})")

    all_parts = columns + fk_constraints
    return f"CREATE TABLE {table.name} (\n" + ",\n".join(all_parts) + "\n);"


def build_db_from_schema(schema_path: str) -> str:
    schema_path = pathlib.Path(schema_path)
    spec = parse_schema_file(schema_path)

    domain_name = spec.domain_name
    db_path = schema_path.parent / f"{domain_name}.db"

    if db_path.exists():
        db_path.unlink()

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()

    statements = []

    main_sql = build_table_sql(spec.main_table)
    statements.append((domain_name, main_sql))

    for table in spec.definition_tables:
        sql = build_table_sql(table)
        statements.append((table.name, sql))

    for table_name, sql in statements:
        cursor.execute(sql)
        print(f"  Created table: {table_name}")

    conn.commit()
    conn.close()

    return str(db_path)
