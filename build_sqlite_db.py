import json
import pathlib
import sqlite3


def json_type_to_sqlite(prop: dict) -> str:
    fmt = prop.get("format", "")
    typ = prop.get("type", "string")
    if fmt == "uuid":
        return "TEXT"
    if typ == "integer":
        return "INTEGER"
    if typ == "number":
        return "REAL"
    if typ == "boolean":
        return "INTEGER"
    return "TEXT"


def _identity_field(table_def: dict) -> str | None:
    """Return the identity/PK field name from x-key or readOnly fallback."""
    x_key = table_def.get("x-key", {})
    if x_key.get("identity_field"):
        return x_key["identity_field"]
    # Fallback: first readOnly field is the PK
    for field, prop in table_def.get("properties", {}).items():
        if prop.get("readOnly"):
            return field
    return None


def build_table_sql(table_name: str, table_def: dict, required_fields: list) -> str:
    properties = table_def.get("properties", {})
    identity = _identity_field(table_def)
    composite_key = table_def.get("x-key", {}).get("composite_key", [])

    columns = []
    foreign_keys = []

    for field, prop in properties.items():
        col_type = json_type_to_sqlite(prop)
        not_null = "NOT NULL" if field in required_fields else ""

        if field == identity:
            columns.append(f"    {field} {col_type} PRIMARY KEY")
        else:
            columns.append(f"    {field} {col_type} {not_null}".rstrip())

        # Field-level x-key means it's a foreign key
        if "x-key" in prop and "references" in prop["x-key"]:
            ref_table = prop["x-key"]["references"]
            ref_field = prop["x-key"]["field"]
            foreign_keys.append(f"    FOREIGN KEY ({field}) REFERENCES {ref_table}({ref_field})")

    if composite_key:
        columns.append(f"    PRIMARY KEY ({', '.join(composite_key)})")

    all_parts = columns + foreign_keys
    return f"CREATE TABLE {table_name} (\n" + ",\n".join(all_parts) + "\n);"


def build_db_from_schema(schema_path: str) -> str:
    schema_path = pathlib.Path(schema_path)
    schema = json.loads(schema_path.read_text())

    domain_name = schema["title"]
    db_path = schema_path.parent / f"{domain_name}.db"

    # Always rebuild from scratch
    if db_path.exists():
        db_path.unlink()

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()

    statements = []

    # Main domain table
    main_sql = build_table_sql(domain_name, schema, schema.get("required", []))
    statements.append((domain_name, main_sql))

    # Definition tables — reference tables before bridge tables (FK order)
    definitions = schema.get("definitions", {})
    ref_tables = {}
    bridge_tables = {}

    for table_name, table_def in definitions.items():
        props = table_def.get("properties", {})
        has_fk = any("x-key" in p and "references" in p["x-key"] for p in props.values())
        if has_fk:
            bridge_tables[table_name] = table_def
        else:
            ref_tables[table_name] = table_def

    for table_name, table_def in ref_tables.items():
        sql = build_table_sql(table_name, table_def, table_def.get("required", []))
        statements.append((table_name, sql))

    for table_name, table_def in bridge_tables.items():
        sql = build_table_sql(table_name, table_def, table_def.get("required", []))
        statements.append((table_name, sql))

    for table_name, sql in statements:
        cursor.execute(sql)
        print(f"  Created table: {table_name}")

    conn.commit()
    conn.close()

    return str(db_path)


if __name__ == "__main__":
    db = build_db_from_schema("domains/Customer/Customer.schema.json")
    print(f"\nDatabase built: {db}")
