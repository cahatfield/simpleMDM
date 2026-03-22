# simpleMDM — Domain Schema Generator User Guide

## Overview

`domain_schema_generator.py` generates JSON Schema files that define the structure of a data domain. Each domain gets its own folder under `domains/` containing a `.schema.json` file that describes all tables, keys, foreign keys, and audit fields for that domain.

These schema files are the foundation for the rest of the simpleMDM toolkit — they drive database creation, library generation, Spark/Databricks table setup, and data contracts.

---

## Prerequisites

- Python 3.10+
- No external dependencies — uses only the standard library (`json`, `pathlib`)
- Run scripts from the project root (the folder containing `domain_schema_generator.py`)

---

## Quick Start

```python
from domain_schema_generator import domain

domain('Customer').create()
domain('Customer').add_key('email', 'first_name', 'last_name')
domain('Customer').add_status()
domain('Customer').add_type()
domain('Customer').add_attribute()
domain('Customer').add_hierarchy()
```

This creates `domains/Customer/Customer.schema.json` with a fully structured domain.

---

## Output Structure

Every domain creates a folder and schema file:

```
domains/
  Customer/
    Customer.schema.json
  Product/
    Product.schema.json
```

The schema file follows JSON Schema draft-07 with custom `x-key` extensions for identity and foreign key metadata.

---

## Class: `domain`

### `domain(name)`

Instantiate the domain builder by passing the domain name. Use PascalCase (e.g. `'Customer'`, `'Product'`, `'SalesOrder'`).

```python
d = domain('Customer')
```

---

### `create(version="1.0.0")`

Initialises the domain — creates the folder and an empty schema file with audit fields.

```python
domain('Customer').create()           # default version 1.0.0
domain('Customer').create('2.0.0')    # explicit version
```

**Output:** `domains/Customer/Customer.schema.json`

The `version` string is stored at the root of the schema JSON and can be used to track breaking changes to the domain structure.

> Must be called before any other method.

---

### `add_key(*fields)`

Defines the key structure for the domain table.

- Automatically creates a system-generated surrogate identity field: `{domain_name_lowercase}_id` (UUID)
- Accepts one or more field names that together form the natural/business key
- All key fields are added to the `required` array
- Stores key metadata in `x-key` at the schema root

```python
# Single natural key
domain('Vendor').add_key('vendor_name')

# Composite natural key
domain('Customer').add_key('email', 'first_name', 'last_name')

# Three-part composite key
domain('Product').add_key('product_brand_name', 'product_catalog_number', 'product_description')
```

**Generated fields on the domain table:**

| Field | Type | Notes |
|-------|------|-------|
| `{name}_id` | `string (uuid)` | System-generated surrogate PK, `readOnly: true` |
| `{field1}` | `string` | Natural key field, `x-natural-key: true` |
| `{field2}` | `string` | Natural key field, `x-natural-key: true` |

---

### `add_status(key_type="uuid")`

Adds a status reference table and links it to the domain table via a foreign key.

```python
domain('Customer').add_status()            # default: uuid key
domain('Customer').add_status('integer')   # integer primary key
domain('Customer').add_status('string')    # string primary key
```

**Created definition:** `{Domain}_Status`

| Field | Type | Notes |
|-------|------|-------|
| `status_id` | `string (uuid)` | Primary key, `readOnly: true` |
| `status_name` | `string` | Required |
| `status_description` | `string` | Optional |
| *(audit fields)* | | Last 4 columns |

**Added to domain table:**

| Field | Type | Notes |
|-------|------|-------|
| `status_id` | `string (uuid)` | FK to `{Domain}_Status.status_id` |

---

### `add_type(key_type="uuid")`

Adds a type reference table and links it to the domain table via a foreign key. Identical pattern to `add_status`.

```python
domain('Customer').add_type()
```

**Created definition:** `{Domain}_Type`

| Field | Type | Notes |
|-------|------|-------|
| `type_id` | `string (uuid)` | Primary key, `readOnly: true` |
| `type_name` | `string` | Required |
| `type_description` | `string` | Optional |
| *(audit fields)* | | Last 4 columns |

**Added to domain table:**

| Field | Type | Notes |
|-------|------|-------|
| `type_id` | `string (uuid)` | FK to `{Domain}_Type.type_id` |

---

### `add_attribute()`

Adds a flexible attribute system — an attribute reference table and a bridge table linking domain records to attribute values. Nothing is added to the domain table itself.

```python
domain('Customer').add_attribute()
```

**Created definitions:**

**`{Domain}_Attribute`**

| Field | Type | Notes |
|-------|------|-------|
| `attribute_id` | `string (uuid)` | PK, `readOnly: true` |
| `attribute_name` | `string` | Natural key, `x-natural-key: true` |
| *(audit fields)* | | Last 4 columns |

**`{Domain}_Attribute_Value`** (bridge table)

| Field | Type | Notes |
|-------|------|-------|
| `{name}_attribute_value_id` | `string (uuid)` | PK, `readOnly: true` |
| `{name}_id` | `string (uuid)` | FK to `{Domain}.{name}_id` |
| `attribute_id` | `string (uuid)` | FK to `{Domain}_Attribute.attribute_id` |
| `attribute_value` | `string` | The attribute's value for this record |
| *(audit fields)* | | Last 4 columns |

---

### `add_hierarchy()`

Adds a hierarchy system — a hierarchy type table and a hierarchy value table for storing parent/child relationships between domain records.

```python
domain('Customer').add_hierarchy()
```

**Created definitions:**

**`{Domain}_Hierarchy`**

| Field | Type | Notes |
|-------|------|-------|
| `{name}_hierarchy_id` | `string (uuid)` | PK, `readOnly: true` |
| `{name}_hierarchy_name` | `string` | Natural key, `x-natural-key: true` |
| *(audit fields)* | | Last 4 columns |

**`{Domain}_Hierarchy_Value`**

| Field | Type | Notes |
|-------|------|-------|
| `{name}_hierarchy_value_id` | `string (uuid)` | PK, `readOnly: true` |
| `{name}_hierarchy_id` | `string (uuid)` | FK to `{Domain}_Hierarchy` |
| `parent_{name}_id` | `string (uuid)` | FK to `{Domain}` — the parent record |
| `child_{name}_id` | `string (uuid)` | FK to `{Domain}` — the child record |
| *(audit fields)* | | Last 4 columns |

---

### `add_relationship(other_domain)`

Adds a bridge table capturing a many-to-many relationship between this domain and another domain.

```python
domain('Product').add_relationship('Vendor')
```

**Created definition:** `{Domain}_{Other}_Relationship`

| Field | Type | Notes |
|-------|------|-------|
| `{name}_{other}_relationship_id` | `string (uuid)` | PK, `readOnly: true` |
| `{name}_id` | `string (uuid)` | FK to `{Domain}` |
| `{other}_id` | `string (uuid)` | FK to `{other_domain}` |
| *(audit fields)* | | Last 4 columns |

> The relationship table is stored in the schema of the domain you call `add_relationship` on. The other domain's schema is not modified.

---

## Audit Fields

Every table — the domain table and all definition tables — always ends with these four columns:

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `create_date` | `string (date-time)` | No | When the record was inserted, `readOnly: true` |
| `created_by` | `string` | No | Who inserted the record, `readOnly: true` |
| `update_date` | `string (date-time)` | No | When the record was last updated |
| `updated_by` | `string` | No | Who last updated the record |

These fields are automatically normalised to always appear as the final four columns regardless of insertion order.

---

## Key Conventions

### Identity Keys

Every table with an `x-key.identity_field` has a system-generated UUID surrogate key. These fields are marked `readOnly: true` in the schema, signalling that the application layer generates the value (typically `uuid.uuid4()`).

### Natural Keys

Fields marked `x-natural-key: true` represent the business/natural key — the combination of fields that uniquely identifies a record in the real world (e.g. email + first_name + last_name for a customer).

### Foreign Keys

Foreign key relationships are expressed at the field level using the custom `x-key` extension:

```json
"status_id": {
  "type": "string",
  "format": "uuid",
  "description": "Foreign key to Customer_Status",
  "x-key": {
    "references": "Customer_Status",
    "field": "status_id"
  }
}
```

---

## Complete Example

Build the full Customer domain from scratch:

```python
from domain_schema_generator import domain

d = domain('Customer')
d.create()
d.add_key('email', 'first_name', 'last_name')
d.add_status()
d.add_type()
d.add_attribute()
d.add_hierarchy()
```

This produces `domains/Customer/Customer.schema.json` with the following tables:

| Table | Purpose |
|-------|---------|
| `Customer` | Core domain table |
| `Customer_Status` | Status reference values |
| `Customer_Type` | Type reference values |
| `Customer_Attribute` | Attribute definitions |
| `Customer_Attribute_Value` | Bridge: customer ↔ attribute values |
| `Customer_Hierarchy` | Hierarchy type definitions |
| `Customer_Hierarchy_Value` | Parent/child customer relationships |

---

## What to Do Next

Once the schema is generated, use the other tools in the simpleMDM toolkit:

| Tool | Purpose |
|------|---------|
| `simplemdm.generators.sqlite_db` | Create a local SQLite database from the schema |
| `simplemdm.generators.spark_script` | Generate PySpark/Delta Lake setup script for Databricks |
| `simplemdm.generators.python_library` | Generate Python dataclass + repository classes |
| `domain_contract_generator.py` | Create and manage the data contract for the domain |
