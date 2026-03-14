import json
import pathlib


def json_type_to_python(prop: dict) -> str:
    typ = prop.get("type", "string")
    if typ == "integer":
        return "int"
    if typ == "number":
        return "float"
    if typ == "boolean":
        return "bool"
    return "str"


def _identity_field(table_def: dict) -> str | None:
    x_key = table_def.get("x-key", {})
    if x_key.get("identity_field"):
        return x_key["identity_field"]
    for field, prop in table_def.get("properties", {}).items():
        if prop.get("readOnly"):
            return field
    return None


def generate_class(table_name: str, table_def: dict) -> str:
    properties = table_def.get("properties", {})
    required = table_def.get("required", [])
    identity = _identity_field(table_def)

    # Split fields: identity (auto-generated), required params, optional params
    required_params = [f for f in required if f != identity]
    optional_params = [f for f in properties if f not in required and f != identity]

    # --- dataclass ---
    lines = []
    lines.append(f"@dataclass")
    lines.append(f"class {table_name}:")
    lines.append(f'    """{table_def.get("description", table_name)}"""')
    if identity:
        py_type = json_type_to_python(properties[identity])
        lines.append(f"    {identity}: {py_type}")
    for field in required_params:
        py_type = json_type_to_python(properties[field])
        lines.append(f"    {field}: {py_type}")
    for field in optional_params:
        py_type = json_type_to_python(properties[field])
        lines.append(f"    {field}: Optional[{py_type}] = None")
    lines.append("")

    # --- repository class ---
    col_names = list(properties.keys())
    placeholders = ", ".join("?" * len(col_names))
    col_list = ", ".join(col_names)

    lines.append(f"class {table_name}Repository:")
    lines.append(f'    """CRUD operations for {table_name}."""')
    lines.append("")
    lines.append("    def __init__(self, db_path: str):")
    lines.append("        self.db_path = db_path")
    lines.append("")

    # add()
    add_params = []
    for field in required_params:
        py_type = json_type_to_python(properties[field])
        add_params.append(f"{field}: {py_type}")
    for field in optional_params:
        py_type = json_type_to_python(properties[field])
        add_params.append(f"{field}: Optional[{py_type}] = None")

    param_str = ", ".join(add_params)
    lines.append(f"    def add(self, {param_str}) -> {table_name}:")
    if identity:
        lines.append(f"        {identity} = str(uuid.uuid4())")

    # Build the values tuple in column order
    values_list = []
    for field in col_names:
        values_list.append(field)
    values_str = ", ".join(values_list)

    lines.append(f"        with sqlite3.connect(self.db_path) as conn:")
    lines.append(f"            conn.execute(")
    lines.append(f'                "INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})",')
    lines.append(f"                ({values_str},),")
    lines.append(f"            )")

    # Build the return
    init_args = ", ".join(f"{f}={f}" for f in col_names)
    lines.append(f"        return {table_name}({init_args})")
    lines.append("")

    # update()
    if identity:
        lines.append(f"    def update(self, {identity}: str, **kwargs) -> None:")
        lines.append(f"        allowed = {set(f for f in col_names if f != identity)!r}")
        lines.append(f"        fields = {{k: v for k, v in kwargs.items() if k in allowed}}")
        lines.append(f"        if not fields:")
        lines.append(f"            return")
        lines.append(f"        set_clause = ', '.join(f'{{k}} = ?' for k in fields)")
        lines.append(f"        with sqlite3.connect(self.db_path) as conn:")
        lines.append(f"            conn.execute(")
        lines.append(f'                f"UPDATE {table_name} SET {{set_clause}} WHERE {identity} = ?",')
        lines.append(f"                (*fields.values(), {identity}),")
        lines.append(f"            )")
        lines.append("")

        # delete()
        lines.append(f"    def delete(self, {identity}: str) -> None:")
        lines.append(f"        with sqlite3.connect(self.db_path) as conn:")
        lines.append(f"            conn.execute(")
        lines.append(f'                "DELETE FROM {table_name} WHERE {identity} = ?",')
        lines.append(f"                ({identity},),")
        lines.append(f"            )")
        lines.append("")

    return "\n".join(lines)


def generate_library(schema_path: str) -> str:
    schema_path = pathlib.Path(schema_path)
    schema = json.loads(schema_path.read_text())

    domain_name = schema["title"]
    output_path = schema_path.parent / f"{domain_name}_library.py"

    db_path = f"domains/{domain_name}/{domain_name}.db"

    sections = []

    # File header
    sections.append(
        "import sqlite3\n"
        "import uuid\n"
        "from dataclasses import dataclass\n"
        "from typing import Optional\n"
    )

    # Main domain table
    sections.append(f"# {'='*60}")
    sections.append(f"# {domain_name}")
    sections.append(f"# {'='*60}\n")
    sections.append(generate_class(domain_name, schema))

    # Definition tables
    for table_name, table_def in schema.get("definitions", {}).items():
        sections.append(f"# {'='*60}")
        sections.append(f"# {table_name}")
        sections.append(f"# {'='*60}\n")
        sections.append(generate_class(table_name, table_def))

    output = "\n".join(sections)
    output_path.write_text(output)
    return str(output_path)


if __name__ == "__main__":
    out = generate_library("domains/Customer/Customer.schema.json")
    print(f"Generated: {out}")
