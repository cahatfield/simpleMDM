from typing import Iterable


def json_type_kind(prop: dict) -> str:
    """Return a normalized logical type for schema field metadata."""
    fmt = prop.get("format", "")
    typ = prop.get("type", "string")
    if fmt == "uuid":
        return "uuid"
    if typ == "integer":
        return "integer"
    if typ == "number":
        return "number"
    if typ == "boolean":
        return "boolean"
    return "string"


def sqlite_type(prop: dict) -> str:
    kind = json_type_kind(prop)
    if kind == "integer":
        return "INTEGER"
    if kind == "number":
        return "REAL"
    if kind == "boolean":
        return "INTEGER"
    return "TEXT"


def pyspark_type(prop: dict) -> str:
    kind = json_type_kind(prop)
    if kind == "integer":
        return "LongType()"
    if kind == "number":
        return "DoubleType()"
    if kind == "boolean":
        return "BooleanType()"
    return "StringType()"


def python_type(prop: dict) -> str:
    kind = json_type_kind(prop)
    if kind == "integer":
        return "int"
    if kind == "number":
        return "float"
    if kind == "boolean":
        return "bool"
    return "str"


def identity_field(table_def: dict) -> str | None:
    """Return the identity/PK field name from x-key or readOnly fallback."""
    x_key = table_def.get("x-key", {})
    if x_key.get("identity_field"):
        return x_key["identity_field"]
    for field, prop in table_def.get("properties", {}).items():
        if prop.get("readOnly"):
            return field
    return None


def foreign_keys(table_def: dict) -> list[tuple[str, str, str]]:
    """Extract (field, referenced_table, referenced_field) triples."""
    result: list[tuple[str, str, str]] = []
    for field, prop in table_def.get("properties", {}).items():
        x_key = prop.get("x-key", {})
        if "references" in x_key and "field" in x_key:
            result.append((field, x_key["references"], x_key["field"]))
    return result


def ordered_definition_items(definitions: dict) -> list[tuple[str, dict]]:
    """Return definitions with FK-free tables before FK-dependent tables."""
    ref_tables: list[tuple[str, dict]] = []
    bridge_tables: list[tuple[str, dict]] = []
    for table_name, table_def in definitions.items():
        if foreign_keys(table_def):
            bridge_tables.append((table_name, table_def))
        else:
            ref_tables.append((table_name, table_def))
    return [*ref_tables, *bridge_tables]


def required_not_null_fields(properties: dict, required_fields: Iterable[str]) -> list[str]:
    """Return fields that should be rendered as NOT NULL in generated targets."""
    required_set = set(required_fields)
    output: list[str] = []
    for field, prop in properties.items():
        is_identity_uuid = prop.get("readOnly") and prop.get("format") == "uuid"
        if field in required_set or prop.get("nullable") is False or is_identity_uuid:
            output.append(field)
    return output
