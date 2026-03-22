import pathlib

from simplemdm.core.schema_core import (
    foreign_keys,
    identity_field,
    pyspark_type,
    required_not_null_fields,
)
from simplemdm.parsers.schema_parser import TableSpec, parse_schema_file


def build_table_block(table: TableSpec, catalog: str, schema: str) -> list[str]:
    properties = table.properties
    identity = identity_field({"x-key": table.x_key, "properties": properties})
    x_key = table.x_key
    composite_key = x_key.get("composite_key", [])
    full_table = f"{catalog}.{schema}.{table.name}"
    description = table.description

    lines = []

    schema_var = f"{table.name.lower()}_schema"
    lines.append(f"{schema_var} = StructType([")
    for field, prop in properties.items():
        py_type = pyspark_type(prop)
        is_identity = prop.get("readOnly") and prop.get("format") == "uuid"
        nullable = "False" if field in table.required or prop.get("nullable") is False or is_identity else "True"
        comment = prop.get("description", "").replace("'", "\\'")
        lines.append(f"    StructField('{field}', {py_type}, nullable={nullable}),  # {comment}")
    lines.append("])" )
    lines.append("")

    lines.append("(")
    lines.append(f"    spark.createDataFrame([], {schema_var})")
    lines.append("    .write")
    lines.append("    .format('delta')")
    lines.append("    .mode('ignore')")
    lines.append(f"    .option('comment', '{description}')")
    lines.append(f"    .saveAsTable('{full_table}')")
    lines.append(")")
    lines.append("")

    not_null_fields = required_not_null_fields(properties, table.required)
    for field in not_null_fields:
        lines.append(f"spark.sql(\"ALTER TABLE {full_table} ALTER COLUMN {field} SET NOT NULL\")")
    if not_null_fields:
        lines.append("")

    if identity and not composite_key:
        lines.append(f"spark.sql(\"ALTER TABLE {full_table} ADD CONSTRAINT pk_{table.name} PRIMARY KEY ({identity})\")")
    elif composite_key:
        key_cols = ", ".join(composite_key)
        lines.append(f"spark.sql(\"ALTER TABLE {full_table} ADD CONSTRAINT pk_{table.name} PRIMARY KEY ({key_cols})\")")

    natural_keys = x_key.get("natural_keys", [])
    if natural_keys:
        key_cols = ", ".join(natural_keys)
        lines.append(f"spark.sql(\"ALTER TABLE {full_table} ADD CONSTRAINT uq_{table.name} UNIQUE ({key_cols})\")")

    for field, ref_table, ref_field in foreign_keys({"properties": properties}):
        fk_name = f"fk_{table.name}_{field}"
        lines.append(
            f"spark.sql(\"ALTER TABLE {full_table} ADD CONSTRAINT {fk_name} "
            f"FOREIGN KEY ({field}) REFERENCES {catalog}.{schema}.{ref_table}({ref_field})\")"
        )

    lines.append(f"print('  Created table: {table.name}')")
    lines.append("")

    return lines


def build_spark_from_schema(schema_path: str, catalog: str = "main", schema: str = None) -> str:
    schema_path = pathlib.Path(schema_path)
    spec = parse_schema_file(schema_path)

    domain_name = spec.domain_name
    if schema is None:
        schema = domain_name.lower()
    output_path = schema_path.parent / f"{domain_name}_spark.py"

    lines = [
        f"# Generated PySpark/Delta library for domain: {domain_name}",
        f"# Catalog: {catalog}  |  Schema: {schema}",
        "#",
        "# Run this file in a Databricks notebook or spark-submit job.",
        "",
        "from pyspark.sql import SparkSession",
        "from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType",
        "",
        "spark = SparkSession.builder.getOrCreate()",
        "",
        f"spark.sql('CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}')",
        "",
    ]

    for table in spec.all_tables():
        lines.append(f"# {'='*60}")
        lines.append(f"# {table.name}")
        lines.append(f"# {'='*60}")
        lines.append("")
        lines.extend(build_table_block(table, catalog, schema))

    output_path.write_text("\n".join(lines))
    return str(output_path)
