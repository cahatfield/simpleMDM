import json
import pathlib


def json_type_to_pyspark(prop: dict) -> str:
    fmt = prop.get("format", "")
    typ = prop.get("type", "string")
    if fmt == "uuid":
        return "StringType()"
    if typ == "integer":
        return "LongType()"
    if typ == "number":
        return "DoubleType()"
    if typ == "boolean":
        return "BooleanType()"
    return "StringType()"


def _identity_field(table_def: dict) -> str | None:
    x_key = table_def.get("x-key", {})
    if x_key.get("identity_field"):
        return x_key["identity_field"]
    for field, prop in table_def.get("properties", {}).items():
        if prop.get("readOnly"):
            return field
    return None


def build_table_block(table_name: str, table_def: dict, required_fields: list,
                      catalog: str, schema: str) -> list[str]:
    properties = table_def.get("properties", {})
    identity = _identity_field(table_def)
    x_key = table_def.get("x-key", {})
    composite_key = x_key.get("composite_key", [])
    full_table = f"{catalog}.{schema}.{table_name}"
    description = table_def.get("description", table_name)

    lines = []

    # StructType schema
    schema_var = f"{table_name.lower()}_schema"
    lines.append(f"{schema_var} = StructType([")
    for field, prop in properties.items():
        py_type = json_type_to_pyspark(prop)
        is_identity = prop.get("readOnly") and prop.get("format") == "uuid"
        nullable = "False" if field in required_fields or prop.get("nullable") is False or is_identity else "True"
        comment = prop.get("description", "").replace("'", "\\'")
        lines.append(f"    StructField('{field}', {py_type}, nullable={nullable}),  # {comment}")
    lines.append("])")
    lines.append("")

    # Create table
    lines.append(f"(")
    lines.append(f"    spark.createDataFrame([], {schema_var})")
    lines.append(f"    .write")
    lines.append(f"    .format('delta')")
    lines.append(f"    .mode('ignore')")
    lines.append(f"    .option('comment', '{description}')")
    lines.append(f"    .saveAsTable('{full_table}')")
    lines.append(f")")
    lines.append("")

    # Primary key constraint
    if identity and not composite_key:
        lines.append(f"spark.sql(\"ALTER TABLE {full_table} ADD CONSTRAINT pk_{table_name} PRIMARY KEY ({identity})\")")
    elif composite_key:
        key_cols = ", ".join(composite_key)
        lines.append(f"spark.sql(\"ALTER TABLE {full_table} ADD CONSTRAINT pk_{table_name} PRIMARY KEY ({key_cols})\")")

    # Foreign key constraints
    for field, prop in properties.items():
        if "x-key" in prop and "references" in prop["x-key"]:
            ref_table = prop["x-key"]["references"]
            ref_field = prop["x-key"]["field"]
            fk_name = f"fk_{table_name}_{field}"
            lines.append(
                f"spark.sql(\"ALTER TABLE {full_table} ADD CONSTRAINT {fk_name} "
                f"FOREIGN KEY ({field}) REFERENCES {catalog}.{schema}.{ref_table}({ref_field})\")"
            )

    lines.append(f"print('  Created table: {table_name}')")
    lines.append("")

    return lines


def build_spark_from_schema(schema_path: str, catalog: str = "main",
                             schema: str = None) -> str:
    schema_path = pathlib.Path(schema_path)
    domain_schema = json.loads(schema_path.read_text())

    domain_name = domain_schema["title"]
    if schema is None:
        schema = domain_name.lower()
    output_path = schema_path.parent / f"{domain_name}_spark.py"

    # Order: reference tables before bridge tables
    definitions = domain_schema.get("definitions", {})
    ref_tables = {}
    bridge_tables = {}
    for table_name, table_def in definitions.items():
        props = table_def.get("properties", {})
        has_fk = any("x-key" in p and "references" in p["x-key"] for p in props.values())
        if has_fk:
            bridge_tables[table_name] = table_def
        else:
            ref_tables[table_name] = table_def

    ordered = [(domain_name, domain_schema, domain_schema.get("required", []))]
    for t, d in ref_tables.items():
        ordered.append((t, d, d.get("required", [])))
    for t, d in bridge_tables.items():
        ordered.append((t, d, d.get("required", [])))

    # File header
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

    for table_name, table_def, required in ordered:
        lines.append(f"# {'='*60}")
        lines.append(f"# {table_name}")
        lines.append(f"# {'='*60}")
        lines.append("")
        lines.extend(build_table_block(table_name, table_def, required, catalog, schema))

    output_path.write_text("\n".join(lines))
    return str(output_path)


if __name__ == "__main__":
    out = build_spark_from_schema(
        "domains/Customer/Customer.schema.json",
        catalog="main",
    )
    print(f"Generated: {out}")
