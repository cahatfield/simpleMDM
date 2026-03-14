
import json
import pathlib

_AUDIT_KEYS = ("create_date", "created_by", "update_date", "updated_by")


def _move_audit_last(properties: dict) -> dict:
    non_audit = {k: v for k, v in properties.items() if k not in _AUDIT_KEYS}
    audit = {k: properties[k] for k in _AUDIT_KEYS if k in properties}
    return {**non_audit, **audit}


def _normalize_schema(schema: dict) -> dict:
    """Ensure audit fields are the last 4 columns in every table's properties."""
    schema["properties"] = _move_audit_last(schema["properties"])
    for defn in schema.get("definitions", {}).values():
        defn["properties"] = _move_audit_last(defn["properties"])
    return schema


def _audit_fields() -> dict:
    return {
        "create_date": {
            "type": "string",
            "format": "date-time",
            "description": "Date and time the record was inserted into the table",
            "readOnly": True,
        },
        "created_by": {
            "type": "string",
            "description": "User who inserted the record into the table",
            "readOnly": True,
        },
        "update_date": {
            "type": "string",
            "format": "date-time",
            "description": "Date and time the record was last updated",
        },
        "updated_by": {
            "type": "string",
            "description": "User who last updated the record",
        },
    }


class domain:
    def __init__(self, name, destination=None):
        self.name = name
        self.destination = pathlib.Path(destination) if destination else pathlib.Path("domains")

    def create(self):
        folder = self.destination / self.name
        folder.mkdir(parents=True, exist_ok=True)

        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": self.name,
            "type": "object",
            "properties": {**_audit_fields()},
            "required": [],
        }

        schema_file = folder / f"{self.name}.schema.json"
        schema_file.write_text(json.dumps(_normalize_schema(schema), indent=2))
        return schema_file

    def add_key(self, *fields: str):
        """Define the key for the domain.

        Accepts one or more fields that together represent the natural/business key.
        A system-generated identity field ({domain_name}_id) is automatically created.

        Args:
            *fields: One or more field names that form the natural key.
        """
        if not fields:
            raise ValueError("At least one field name must be provided")

        schema_file = self.destination / self.name / f"{self.name}.schema.json"
        schema = json.loads(schema_file.read_text())

        identity_field = f"{self.name.lower()}_id"

        schema["properties"][identity_field] = {
            "type": "string",
            "format": "uuid",
            "description": f"System-generated identity field for the {self.name} domain",
            "readOnly": True,
        }
        if identity_field not in schema["required"]:
            schema["required"].append(identity_field)

        for field in fields:
            schema["properties"][field] = {
                "type": "string",
                "description": f"Natural key field for the {self.name} domain",
                "x-natural-key": True,
            }
            if field not in schema["required"]:
                schema["required"].append(field)

        schema["x-key"] = {
            "identity_field": identity_field,
            "natural_keys": list(fields),
        }

        schema_file.write_text(json.dumps(_normalize_schema(schema), indent=2))
        return self

    def add_status(self, key_type: str = "uuid"):
        """Add a status reference table and link it via foreign key on the domain table.

        Args:
            key_type: Data type for the status primary key — 'uuid', 'integer', or 'string'.
        """
        type_map = {
            "uuid":    {"type": "string", "format": "uuid"},
            "integer": {"type": "integer"},
            "string":  {"type": "string"},
        }
        if key_type not in type_map:
            raise ValueError(f"key_type must be one of {list(type_map)}, got {key_type!r}")

        schema_file = self.destination / self.name / f"{self.name}.schema.json"
        schema = json.loads(schema_file.read_text())

        status_table = f"{self.name}_Status"

        schema.setdefault("definitions", {})[status_table] = {
            "title": status_table,
            "description": f"Status reference table for the {self.name} domain",
            "type": "object",
            "properties": {
                "status_id": {
                    **type_map[key_type],
                    "description": f"Primary key for {status_table}",
                    "readOnly": True,
                },
                "status_name": {
                    "type": "string",
                    "description": "Name of the status",
                },
                "status_description": {
                    "type": "string",
                    "description": "Description of what this status means",
                },
                **_audit_fields(),
            },
            "required": ["status_id", "status_name"],
        }

        schema["properties"]["status_id"] = {
            **type_map[key_type],
            "description": f"Foreign key to {status_table}",
            "x-key": {"references": status_table, "field": "status_id"},
        }

        if "status_id" not in schema["required"]:
            schema["required"].append("status_id")

        schema_file.write_text(json.dumps(_normalize_schema(schema), indent=2))
        return self

    def add_type(self, key_type: str = "uuid"):
        """Add a type reference table and link it via foreign key on the domain table.

        Args:
            key_type: Data type for the type primary key — 'uuid', 'integer', or 'string'.
        """
        type_map = {
            "uuid":    {"type": "string", "format": "uuid"},
            "integer": {"type": "integer"},
            "string":  {"type": "string"},
        }
        if key_type not in type_map:
            raise ValueError(f"key_type must be one of {list(type_map)}, got {key_type!r}")

        schema_file = self.destination / self.name / f"{self.name}.schema.json"
        schema = json.loads(schema_file.read_text())

        type_table = f"{self.name}_Type"

        schema.setdefault("definitions", {})[type_table] = {
            "title": type_table,
            "description": f"Type reference table for the {self.name} domain",
            "type": "object",
            "properties": {
                "type_id": {
                    **type_map[key_type],
                    "description": f"Primary key for {type_table}",
                    "readOnly": True,
                },
                "type_name": {
                    "type": "string",
                    "description": "Name of the type",
                },
                "type_description": {
                    "type": "string",
                    "description": "Description of what this type means",
                },
                **_audit_fields(),
            },
            "required": ["type_id", "type_name"],
        }

        schema["properties"]["type_id"] = {
            **type_map[key_type],
            "description": f"Foreign key to {type_table}",
            "x-key": {"references": type_table, "field": "type_id"},
        }

        if "type_id" not in schema["required"]:
            schema["required"].append("type_id")

        schema_file.write_text(json.dumps(_normalize_schema(schema), indent=2))
        return self

    def add_attribute(self):
        """Add an attribute reference table and bridge table for the domain."""
        schema_file = self.destination / self.name / f"{self.name}.schema.json"
        schema = json.loads(schema_file.read_text())

        identity_field = f"{self.name.lower()}_id"
        attribute_table = f"{self.name}_Attribute"
        bridge_table = f"{self.name}_Attribute_Value"
        bridge_value_id = f"{self.name.lower()}_attribute_value_id"

        definitions = schema.setdefault("definitions", {})

        definitions[attribute_table] = {
            "title": attribute_table,
            "description": f"Attribute reference table for the {self.name} domain",
            "type": "object",
            "properties": {
                "attribute_id": {
                    "type": "string",
                    "format": "uuid",
                    "description": f"System-generated identity key for {attribute_table}",
                    "readOnly": True,
                },
                "attribute_name": {
                    "type": "string",
                    "description": "Name of the attribute",
                    "x-natural-key": True,
                },
                **_audit_fields(),
            },
            "required": ["attribute_id", "attribute_name"],
            "x-key": {
                "identity_field": "attribute_id",
                "natural_keys": ["attribute_name"],
            },
        }

        definitions[bridge_table] = {
            "title": bridge_table,
            "description": f"Bridge table linking {self.name} records to their attribute values",
            "type": "object",
            "properties": {
                bridge_value_id: {
                    "type": "string",
                    "format": "uuid",
                    "description": f"System-generated identity key for {bridge_table}",
                    "readOnly": True,
                },
                identity_field: {
                    "type": "string",
                    "format": "uuid",
                    "description": f"Foreign key to {self.name}",
                    "x-key": {"references": self.name, "field": identity_field},
                },
                "attribute_id": {
                    "type": "string",
                    "format": "uuid",
                    "description": f"Foreign key to {attribute_table}",
                    "x-key": {"references": attribute_table, "field": "attribute_id"},
                },
                "attribute_value": {
                    "type": "string",
                    "description": "Value of the attribute for this domain record",
                },
                **_audit_fields(),
            },
            "required": [bridge_value_id, identity_field, "attribute_id", "attribute_value"],
            "x-key": {
                "identity_field": bridge_value_id,
                "natural_keys": [identity_field, "attribute_id"],
            },
        }

        schema_file.write_text(json.dumps(_normalize_schema(schema), indent=2))
        return self

    def add_hierarchy(self):
        """Add a hierarchy type table and hierarchy value table for the domain."""
        schema_file = self.destination / self.name / f"{self.name}.schema.json"
        schema = json.loads(schema_file.read_text())

        n = self.name.lower()
        hierarchy_table = f"{self.name}_Hierarchy"
        value_table = f"{self.name}_Hierarchy_Value"
        hierarchy_id = f"{n}_hierarchy_id"
        hierarchy_name = f"{n}_hierarchy_name"
        hierarchy_value_id = f"{n}_hierarchy_value_id"
        identity_field = f"{n}_id"
        parent_field = f"parent_{identity_field}"
        child_field = f"child_{identity_field}"

        definitions = schema.setdefault("definitions", {})

        definitions[hierarchy_table] = {
            "title": hierarchy_table,
            "description": f"Hierarchy type table for the {self.name} domain",
            "type": "object",
            "properties": {
                hierarchy_id: {
                    "type": "string",
                    "format": "uuid",
                    "description": f"System-generated identity key for {hierarchy_table}",
                    "readOnly": True,
                },
                hierarchy_name: {
                    "type": "string",
                    "description": "Name of the hierarchy",
                    "x-natural-key": True,
                },
                **_audit_fields(),
            },
            "required": [hierarchy_id, hierarchy_name],
            "x-key": {
                "identity_field": hierarchy_id,
                "natural_keys": [hierarchy_name],
            },
        }

        definitions[value_table] = {
            "title": value_table,
            "description": f"Hierarchy value table storing parent/child relationships for the {self.name} domain",
            "type": "object",
            "properties": {
                hierarchy_value_id: {
                    "type": "string",
                    "format": "uuid",
                    "description": f"System-generated identity key for {value_table}",
                    "readOnly": True,
                },
                hierarchy_id: {
                    "type": "string",
                    "format": "uuid",
                    "description": f"Foreign key to {hierarchy_table}",
                    "x-key": {"references": hierarchy_table, "field": hierarchy_id},
                },
                parent_field: {
                    "type": "string",
                    "format": "uuid",
                    "description": f"Foreign key to {self.name} — the parent record",
                    "x-key": {"references": self.name, "field": identity_field},
                },
                child_field: {
                    "type": "string",
                    "format": "uuid",
                    "description": f"Foreign key to {self.name} — the child record",
                    "x-key": {"references": self.name, "field": identity_field},
                },
                **_audit_fields(),
            },
            "required": [hierarchy_value_id, hierarchy_id, parent_field, child_field],
            "x-key": {
                "identity_field": hierarchy_value_id,
                "natural_keys": [hierarchy_id, parent_field, child_field],
            },
        }

        schema_file.write_text(json.dumps(_normalize_schema(schema), indent=2))
        return self

    def add_relationship(self, other_domain: str):
        """Add a relationship bridge table between this domain and another domain.

        Args:
            other_domain: Name of the other domain to relate to (e.g. 'Product').
        """
        schema_file = self.destination / self.name / f"{self.name}.schema.json"
        schema = json.loads(schema_file.read_text())

        n1 = self.name.lower()
        n2 = other_domain.lower()
        rel_table = f"{self.name}_{other_domain}_Relationship"
        rel_id = f"{n1}_{n2}_relationship_id"
        fk1 = f"{n1}_id"
        fk2 = f"{n2}_id"

        schema.setdefault("definitions", {})[rel_table] = {
            "title": rel_table,
            "description": f"Relationship bridge table between {self.name} and {other_domain}",
            "type": "object",
            "properties": {
                rel_id: {
                    "type": "string",
                    "format": "uuid",
                    "description": f"System-generated identity key for {rel_table}",
                    "readOnly": True,
                },
                fk1: {
                    "type": "string",
                    "format": "uuid",
                    "description": f"Foreign key to {self.name}",
                    "x-key": {"references": self.name, "field": fk1},
                },
                fk2: {
                    "type": "string",
                    "format": "uuid",
                    "description": f"Foreign key to {other_domain}",
                    "x-key": {"references": other_domain, "field": fk2},
                },
                **_audit_fields(),
            },
            "required": [rel_id, fk1, fk2],
            "x-key": {
                "identity_field": rel_id,
                "natural_keys": [fk1, fk2],
            },
        }

        schema_file.write_text(json.dumps(_normalize_schema(schema), indent=2))
        return self
