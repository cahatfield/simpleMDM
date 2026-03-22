import json
import pathlib
from dataclasses import dataclass

from simplemdm.core.schema_core import ordered_definition_items


@dataclass
class TableSpec:
    name: str
    description: str
    properties: dict
    required: list[str]
    x_key: dict


@dataclass
class SchemaSpec:
    domain_name: str
    main_table: TableSpec
    definition_tables: list[TableSpec]

    def all_tables(self) -> list[TableSpec]:
        return [self.main_table, *self.definition_tables]


def _table_spec(name: str, table_def: dict) -> TableSpec:
    return TableSpec(
        name=name,
        description=table_def.get("description", name),
        properties=table_def.get("properties", {}),
        required=table_def.get("required", []),
        x_key=table_def.get("x-key", {}),
    )


def parse_schema(schema: dict) -> SchemaSpec:
    domain_name = schema["title"]
    main_table = _table_spec(domain_name, schema)

    definitions = schema.get("definitions", {})
    definition_tables = [
        _table_spec(table_name, table_def)
        for table_name, table_def in ordered_definition_items(definitions)
    ]

    return SchemaSpec(
        domain_name=domain_name,
        main_table=main_table,
        definition_tables=definition_tables,
    )


def parse_schema_file(schema_path: str | pathlib.Path) -> SchemaSpec:
    path = pathlib.Path(schema_path)
    schema = json.loads(path.read_text())
    return parse_schema(schema)
