# simpleMDM

A lightweight Python toolkit for defining, generating, and managing master data domains. Build structured domain schemas from code, then generate SQLite databases, Spark/Delta Lake tables, Python libraries, and data contracts — all from a single schema definition.

No external dependencies. Pure Python standard library.

---

## Package Layout

The codebase now uses a package-first structure for modularity while preserving the original top-level scripts as compatibility wrappers.

```
simplemdm/
  core/
    schema_core.py            # Shared schema/type/key helpers
  parsers/
    schema_parser.py          # SchemaSpec/TableSpec parser layer
  generators/
    sqlite_db.py              # SQLite generator
    spark_script.py           # Spark/Delta script generator
    python_library.py         # Dataclass/repository generator
  services/
    generation.py             # Orchestrates all outputs from one schema
```

## Toolkit Overview

| Module | Purpose |
|--------|---------|
| `domain_schema_generator.py` | Define domain structure and generate JSON Schema files |
| `simplemdm.generators.sqlite_db` | Create a local SQLite database from a domain schema |
| `simplemdm.generators.spark_script` | Generate PySpark/Delta Lake setup scripts for Databricks |
| `simplemdm.generators.python_library` | Generate Python dataclass and repository classes |
| `domain_contract_generator.py` | Create and manage data contracts for a domain |

---

## Requirements

- Python 3.10+
- No external dependencies for schema generation and contract management
- PySpark for `simplemdm.generators.spark_script`

---

## Quick Start

```python
from domain_schema_generator import domain

# Build a full domain
d = domain('Customer')
d.create() \
 .add_key('email', 'first_name', 'last_name') \
 .add_status() \
 .add_type() \
 .add_attribute() \
 .add_hierarchy() \
 .save()
```

This creates `domains/Customer/Customer.schema.json` — the foundation for all other toolkit outputs.

---

## Domain Schema Generator

### `domain(name, destination=None)`

| Parameter | Description |
|-----------|-------------|
| `name` | Domain name in PascalCase (e.g. `'Customer'`, `'Product'`) |
| `destination` | Output folder path. Defaults to `domains/` |

### Methods

| Method | Description |
|--------|-------------|
| `create()` | Initialise the domain folder and schema file |
| `add_key(*fields)` | Define the surrogate identity key and natural/business key fields |
| `add_status(key_type)` | Add a status reference table and FK on the domain table |
| `add_type(key_type)` | Add a type reference table and FK on the domain table |
| `add_attribute()` | Add a flexible attribute system (attribute + bridge tables) |
| `add_hierarchy()` | Add a hierarchy system (hierarchy type + parent/child value tables) |
| `add_relationship(other_domain)` | Add a many-to-many bridge table to another domain |
| `load()` | Load an existing schema for in-memory edits |
| `save()` / `commit()` | Persist in-memory changes to disk |

All methods return `self` so they can be chained:

```python
domain('Product').create().add_key('product_brand_name', 'product_catalog_number').add_status().add_type()
```

---

## Data Contract Generator

```python
from domain_contract_generator import contract

contract('Customer') \
    .create() \
    .set_owner('Jane Smith', 'Customer Data', 'jane.smith@example.com') \
    .set_sla('daily', '99.9%') \
    .add_consumer('CRM System', 'Sales') \
    .add_quality_rule('Customer', 'email', not_null=True, unique=True) \
  .set_status('active') \
  .save()
```

### Methods

| Method | Description |
|--------|-------------|
| `create()` | Initialise a new contract file |
| `set_owner(name, team, email)` | Set the data owner |
| `set_sla(freshness, availability)` | Define freshness and availability targets |
| `add_consumer(name, team)` | Register a data consumer |
| `add_quality_rule(table, field, **rules)` | Add field-level quality constraints |
| `set_status(status)` | Set lifecycle status: `draft`, `active`, or `deprecated` |
| `add_changelog(version, description)` | Append a changelog entry |
| `load()` | Load an existing contract for in-memory edits |
| `save()` / `commit()` | Persist in-memory changes to disk |

---

## Schema Conventions

### Keys
- Every domain table gets a system-generated UUID surrogate key: `{domain_name}_id`
- Natural/business key fields are marked `x-natural-key: true`

### Foreign Keys
- Expressed with the custom `x-key` extension at the field level
- Reference table and field are explicit in the schema

### Audit Fields
All tables end with these four columns, always last:

| Field | Description |
|-------|-------------|
| `create_date` | Timestamp the record was inserted |
| `created_by` | User who inserted the record |
| `update_date` | Timestamp the record was last updated |
| `updated_by` | User who last updated the record |

---

## Output Structure

```
domains/
  Customer/
    Customer.schema.json        # Domain schema
    Customer.contract.json      # Data contract
    Customer_library.py         # Generated Python dataclasses + repository
    Customer_spark.py           # Generated PySpark/Delta Lake setup
  Product/
    Product.schema.json
    ...
```

---

## Detailed Documentation

See [domain_schema_generator_guide.md](domain_schema_generator_guide.md) for full method reference and field-level schema documentation.

---

## Service API (Recommended)

For one-call generation of all artifacts:

```python
from simplemdm.services.generation import generate_all_from_schema

outputs = generate_all_from_schema("domains/Customer/Customer.schema.json")
print(outputs)
```

---

## CLI Usage

Run via module:

```powershell
python -m simplemdm.cli --help
```

Common examples:

```powershell
# Create schema + common structures
python -m simplemdm.cli schema Customer --key email first_name last_name --add-status --add-type --add-attribute --add-hierarchy

# Generate individual artifacts
python -m simplemdm.cli sqlite domains/Customer/Customer.schema.json
python -m simplemdm.cli spark domains/Customer/Customer.schema.json --catalog main --schema customer
python -m simplemdm.cli library domains/Customer/Customer.schema.json

# Generate all artifacts
python -m simplemdm.cli all domains/Customer/Customer.schema.json

# Create contract
python -m simplemdm.cli contract Customer --owner-name "Jane Smith" --owner-team "Customer Data" --owner-email jane.smith@example.com --status active
```
