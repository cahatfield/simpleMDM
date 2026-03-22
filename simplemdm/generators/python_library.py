import pathlib

from simplemdm.core.schema_core import identity_field, python_type
from simplemdm.parsers.schema_parser import TableSpec, parse_schema_file


def generate_class(table: TableSpec) -> str:
    properties = table.properties
    required = table.required
    identity = identity_field({"x-key": table.x_key, "properties": properties})

    required_params = [f for f in required if f != identity]
    optional_params = [f for f in properties if f not in required and f != identity]

    lines = []
    lines.append("@dataclass")
    lines.append(f"class {table.name}:")
    lines.append(f'    """{table.description}"""')
    if identity:
        py_type = python_type(properties[identity])
        lines.append(f"    {identity}: {py_type}")
    for field in required_params:
        py_type = python_type(properties[field])
        lines.append(f"    {field}: {py_type}")
    for field in optional_params:
        py_type = python_type(properties[field])
        lines.append(f"    {field}: Optional[{py_type}] = None")
    lines.append("")

    col_names = list(properties.keys())
    placeholders = ", ".join("?" * len(col_names))
    col_list = ", ".join(col_names)

    lines.append(f"class {table.name}Repository:")
    lines.append(f'    """CRUD operations for {table.name}."""')
    lines.append("")
    lines.append("    def __init__(self, db_path: str, catalog: Optional[str] = None, schema: Optional[str] = None):")
    lines.append("        self.db_path = db_path")
    lines.append("        prefix = '.'.join(p for p in [catalog, schema] if p)")
    lines.append(f'        self._table = f"{{prefix}}.{table.name}" if prefix else "{table.name}"')
    lines.append("")

    add_params = []
    for field in required_params:
        py_type = python_type(properties[field])
        add_params.append(f"{field}: {py_type}")
    for field in optional_params:
        py_type = python_type(properties[field])
        add_params.append(f"{field}: Optional[{py_type}] = None")

    param_str = ", ".join(add_params)
    lines.append(f"    def add(self, {param_str}) -> {table.name}:")
    if identity:
        lines.append(f"        {identity} = str(uuid.uuid4())")

    values_list = []
    for field in col_names:
        values_list.append(field)
    values_str = ", ".join(values_list)

    lines.append("        with sqlite3.connect(self.db_path) as conn:")
    lines.append("            conn.execute(")
    lines.append(f'                f"INSERT INTO {{self._table}} ({col_list}) VALUES ({placeholders})",')
    lines.append(f"                ({values_str},),")
    lines.append("            )")

    init_args = ", ".join(f"{f}={f}" for f in col_names)
    lines.append(f"        return {table.name}({init_args})")
    lines.append("")

    if identity:
        lines.append(f"    def update(self, {identity}: str, **kwargs) -> None:")
        allowed_fields = tuple(sorted(f for f in col_names if f != identity))
        lines.append(f"        allowed = {allowed_fields!r}")
        lines.append("        fields = {k: v for k, v in kwargs.items() if k in allowed}")
        lines.append("        if not fields:")
        lines.append("            return")
        lines.append("        set_clause = ', '.join(f'{k} = ?' for k in fields)")
        lines.append("        with sqlite3.connect(self.db_path) as conn:")
        lines.append("            conn.execute(")
        lines.append(f'                f"UPDATE {{self._table}} SET {{set_clause}} WHERE {identity} = ?",')
        lines.append(f"                (*fields.values(), {identity}),")
        lines.append("            )")
        lines.append("")

        lines.append(f"    def delete(self, {identity}: str) -> None:")
        lines.append("        with sqlite3.connect(self.db_path) as conn:")
        lines.append("            conn.execute(")
        lines.append(f'                f"DELETE FROM {{self._table}} WHERE {identity} = ?",')
        lines.append(f"                ({identity},),")
        lines.append("            )")
        lines.append("")

    return "\n".join(lines)


def generate_library(schema_path: str) -> str:
    schema_path = pathlib.Path(schema_path)
    spec = parse_schema_file(schema_path)

    domain_name = spec.domain_name
    output_path = schema_path.parent / f"{domain_name}_library.py"

    sections = []

    sections.append(
        "import sqlite3\n"
        "import uuid\n"
        "from dataclasses import dataclass\n"
        "from typing import Optional\n"
    )

    sections.append(f"# {'='*60}")
    sections.append(f"# {domain_name}")
    sections.append(f"# {'='*60}\n")
    sections.append(generate_class(spec.main_table))

    for table in spec.definition_tables:
        sections.append(f"# {'='*60}")
        sections.append(f"# {table.name}")
        sections.append(f"# {'='*60}\n")
        sections.append(generate_class(table))

    output = "\n".join(sections)
    output_path.write_text(output)
    return str(output_path)
