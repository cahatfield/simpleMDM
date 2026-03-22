from simplemdm.generators.python_library import generate_library
from simplemdm.generators.spark_script import build_spark_from_schema
from simplemdm.generators.sqlite_db import build_db_from_schema


def generate_all_from_schema(
    schema_path: str,
    catalog: str = "main",
    spark_schema: str | None = None,
) -> dict[str, str]:
    """Generate all artifacts for a domain schema and return output paths."""
    return {
        "sqlite_db": build_db_from_schema(schema_path),
        "spark_script": build_spark_from_schema(
            schema_path,
            catalog=catalog,
            schema=spark_schema,
        ),
        "python_library": generate_library(schema_path),
    }
