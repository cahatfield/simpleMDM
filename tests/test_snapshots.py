import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from simplemdm.generators.python_library import generate_library
from simplemdm.generators.spark_script import build_spark_from_schema
from simplemdm.generators.sqlite_db import build_db_from_schema


FIXTURE_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Tiny",
    "version": "1.0.0",
    "type": "object",
    "properties": {
        "tiny_id": {
            "type": "string",
            "format": "uuid",
            "readOnly": True,
            "nullable": False,
            "description": "id",
        },
        "name": {"type": "string", "description": "name"},
        "active": {"type": "boolean", "description": "active"},
    },
    "required": ["tiny_id", "name"],
    "x-key": {"identity_field": "tiny_id", "natural_keys": ["name"]},
}

EXPECTED_SQLITE_DDL = """CREATE TABLE Tiny (
    tiny_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    active INTEGER
)"""

EXPECTED_SPARK = """# Generated PySpark/Delta library for domain: Tiny
# Catalog: main  |  Schema: tiny
#
# Run this file in a Databricks notebook or spark-submit job.

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType

spark = SparkSession.builder.getOrCreate()

spark.sql('CREATE SCHEMA IF NOT EXISTS main.tiny')

# ============================================================
# Tiny
# ============================================================

tiny_schema = StructType([
    StructField('tiny_id', StringType(), nullable=False),  # id
    StructField('name', StringType(), nullable=False),  # name
    StructField('active', BooleanType(), nullable=True),  # active
])

(
    spark.createDataFrame([], tiny_schema)
    .write
    .format('delta')
    .mode('ignore')
    .option('comment', 'Tiny')
    .saveAsTable('main.tiny.Tiny')
)

spark.sql("ALTER TABLE main.tiny.Tiny ALTER COLUMN tiny_id SET NOT NULL")
spark.sql("ALTER TABLE main.tiny.Tiny ALTER COLUMN name SET NOT NULL")

spark.sql("ALTER TABLE main.tiny.Tiny ADD CONSTRAINT pk_Tiny PRIMARY KEY (tiny_id)")
spark.sql("ALTER TABLE main.tiny.Tiny ADD CONSTRAINT uq_Tiny UNIQUE (name)")
print('  Created table: Tiny')
"""

EXPECTED_LIBRARY = """import sqlite3
import uuid
from dataclasses import dataclass
from typing import Optional

# ============================================================
# Tiny
# ============================================================

@dataclass
class Tiny:
    \"\"\"Tiny\"\"\"
    tiny_id: str
    name: str
    active: Optional[bool] = None

class TinyRepository:
    \"\"\"CRUD operations for Tiny.\"\"\"

    def __init__(self, db_path: str, catalog: Optional[str] = None, schema: Optional[str] = None):
        self.db_path = db_path
        prefix = '.'.join(p for p in [catalog, schema] if p)
        self._table = f\"{prefix}.Tiny\" if prefix else \"Tiny\"

    def add(self, name: str, active: Optional[bool] = None) -> Tiny:
        tiny_id = str(uuid.uuid4())
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                f\"INSERT INTO {self._table} (tiny_id, name, active) VALUES (?, ?, ?)\",
                (tiny_id, name, active,),
            )
        return Tiny(tiny_id=tiny_id, name=name, active=active)

    def update(self, tiny_id: str, **kwargs) -> None:
        allowed = ('active', 'name')
        fields = {k: v for k, v in kwargs.items() if k in allowed}
        if not fields:
            return
        set_clause = ', '.join(f'{k} = ?' for k in fields)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                f\"UPDATE {self._table} SET {set_clause} WHERE tiny_id = ?\",
                (*fields.values(), tiny_id),
            )

    def delete(self, tiny_id: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                f\"DELETE FROM {self._table} WHERE tiny_id = ?\",
                (tiny_id,),
            )
"""


class SnapshotGenerationTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.schema_path = self.temp_dir / "Tiny.schema.json"
        self.schema_path.write_text(json.dumps(FIXTURE_SCHEMA, indent=2))

    def test_sqlite_snapshot(self):
        db_path = Path(build_db_from_schema(str(self.schema_path)))
        with sqlite3.connect(db_path) as conn:
            ddl = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='Tiny'"
            ).fetchone()[0]
        self.assertEqual(ddl, EXPECTED_SQLITE_DDL)

    def test_spark_snapshot(self):
        spark_path = Path(
            build_spark_from_schema(str(self.schema_path), catalog="main", schema="tiny")
        )
        self.assertEqual(spark_path.read_text(), EXPECTED_SPARK)

    def test_library_snapshot(self):
        library_path = Path(generate_library(str(self.schema_path)))
        self.assertEqual(library_path.read_text(), EXPECTED_LIBRARY)


if __name__ == "__main__":
    unittest.main()
