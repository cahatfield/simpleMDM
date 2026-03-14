import sqlite3
import uuid
from dataclasses import dataclass
from typing import Optional

# ============================================================
# Product
# ============================================================

@dataclass
class Product:
    """Product"""
    product_id: str
    product_brand_name: str
    product_catalog_number: str
    product_description: str
    status_id: str
    type_id: str
    create_date: Optional[str] = None
    created_by: Optional[str] = None
    update_date: Optional[str] = None
    updated_by: Optional[str] = None

class ProductRepository:
    """CRUD operations for Product."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def add(self, product_brand_name: str, product_catalog_number: str, product_description: str, status_id: str, type_id: str, create_date: Optional[str] = None, created_by: Optional[str] = None, update_date: Optional[str] = None, updated_by: Optional[str] = None) -> Product:
        product_id = str(uuid.uuid4())
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO Product (product_id, product_brand_name, product_catalog_number, product_description, status_id, type_id, create_date, created_by, update_date, updated_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (product_id, product_brand_name, product_catalog_number, product_description, status_id, type_id, create_date, created_by, update_date, updated_by,),
            )
        return Product(product_id=product_id, product_brand_name=product_brand_name, product_catalog_number=product_catalog_number, product_description=product_description, status_id=status_id, type_id=type_id, create_date=create_date, created_by=created_by, update_date=update_date, updated_by=updated_by)

    def update(self, product_id: str, **kwargs) -> None:
        allowed = {'product_description', 'update_date', 'product_catalog_number', 'type_id', 'status_id', 'updated_by', 'created_by', 'create_date', 'product_brand_name'}
        fields = {k: v for k, v in kwargs.items() if k in allowed}
        if not fields:
            return
        set_clause = ', '.join(f'{k} = ?' for k in fields)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                f"UPDATE Product SET {set_clause} WHERE product_id = ?",
                (*fields.values(), product_id),
            )

    def delete(self, product_id: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "DELETE FROM Product WHERE product_id = ?",
                (product_id,),
            )

# ============================================================
# Product_Status
# ============================================================

@dataclass
class Product_Status:
    """Status reference table for the Product domain"""
    status_id: str
    status_name: str
    status_description: Optional[str] = None
    create_date: Optional[str] = None
    created_by: Optional[str] = None
    update_date: Optional[str] = None
    updated_by: Optional[str] = None

class Product_StatusRepository:
    """CRUD operations for Product_Status."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def add(self, status_name: str, status_description: Optional[str] = None, create_date: Optional[str] = None, created_by: Optional[str] = None, update_date: Optional[str] = None, updated_by: Optional[str] = None) -> Product_Status:
        status_id = str(uuid.uuid4())
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO Product_Status (status_id, status_name, status_description, create_date, created_by, update_date, updated_by) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (status_id, status_name, status_description, create_date, created_by, update_date, updated_by,),
            )
        return Product_Status(status_id=status_id, status_name=status_name, status_description=status_description, create_date=create_date, created_by=created_by, update_date=update_date, updated_by=updated_by)

    def update(self, status_id: str, **kwargs) -> None:
        allowed = {'update_date', 'status_description', 'status_name', 'updated_by', 'created_by', 'create_date'}
        fields = {k: v for k, v in kwargs.items() if k in allowed}
        if not fields:
            return
        set_clause = ', '.join(f'{k} = ?' for k in fields)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                f"UPDATE Product_Status SET {set_clause} WHERE status_id = ?",
                (*fields.values(), status_id),
            )

    def delete(self, status_id: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "DELETE FROM Product_Status WHERE status_id = ?",
                (status_id,),
            )

# ============================================================
# Product_Type
# ============================================================

@dataclass
class Product_Type:
    """Type reference table for the Product domain"""
    type_id: str
    type_name: str
    type_description: Optional[str] = None
    create_date: Optional[str] = None
    created_by: Optional[str] = None
    update_date: Optional[str] = None
    updated_by: Optional[str] = None

class Product_TypeRepository:
    """CRUD operations for Product_Type."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def add(self, type_name: str, type_description: Optional[str] = None, create_date: Optional[str] = None, created_by: Optional[str] = None, update_date: Optional[str] = None, updated_by: Optional[str] = None) -> Product_Type:
        type_id = str(uuid.uuid4())
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO Product_Type (type_id, type_name, type_description, create_date, created_by, update_date, updated_by) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (type_id, type_name, type_description, create_date, created_by, update_date, updated_by,),
            )
        return Product_Type(type_id=type_id, type_name=type_name, type_description=type_description, create_date=create_date, created_by=created_by, update_date=update_date, updated_by=updated_by)

    def update(self, type_id: str, **kwargs) -> None:
        allowed = {'update_date', 'type_name', 'updated_by', 'created_by', 'type_description', 'create_date'}
        fields = {k: v for k, v in kwargs.items() if k in allowed}
        if not fields:
            return
        set_clause = ', '.join(f'{k} = ?' for k in fields)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                f"UPDATE Product_Type SET {set_clause} WHERE type_id = ?",
                (*fields.values(), type_id),
            )

    def delete(self, type_id: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "DELETE FROM Product_Type WHERE type_id = ?",
                (type_id,),
            )

# ============================================================
# Product_Attribute
# ============================================================

@dataclass
class Product_Attribute:
    """Attribute reference table for the Product domain"""
    attribute_id: str
    attribute_name: str
    create_date: Optional[str] = None
    created_by: Optional[str] = None
    update_date: Optional[str] = None
    updated_by: Optional[str] = None

class Product_AttributeRepository:
    """CRUD operations for Product_Attribute."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def add(self, attribute_name: str, create_date: Optional[str] = None, created_by: Optional[str] = None, update_date: Optional[str] = None, updated_by: Optional[str] = None) -> Product_Attribute:
        attribute_id = str(uuid.uuid4())
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO Product_Attribute (attribute_id, attribute_name, create_date, created_by, update_date, updated_by) VALUES (?, ?, ?, ?, ?, ?)",
                (attribute_id, attribute_name, create_date, created_by, update_date, updated_by,),
            )
        return Product_Attribute(attribute_id=attribute_id, attribute_name=attribute_name, create_date=create_date, created_by=created_by, update_date=update_date, updated_by=updated_by)

    def update(self, attribute_id: str, **kwargs) -> None:
        allowed = {'update_date', 'attribute_name', 'updated_by', 'created_by', 'create_date'}
        fields = {k: v for k, v in kwargs.items() if k in allowed}
        if not fields:
            return
        set_clause = ', '.join(f'{k} = ?' for k in fields)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                f"UPDATE Product_Attribute SET {set_clause} WHERE attribute_id = ?",
                (*fields.values(), attribute_id),
            )

    def delete(self, attribute_id: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "DELETE FROM Product_Attribute WHERE attribute_id = ?",
                (attribute_id,),
            )

# ============================================================
# Product_Attribute_Value
# ============================================================

@dataclass
class Product_Attribute_Value:
    """Bridge table linking Product records to their attribute values"""
    product_attribute_value_id: str
    product_id: str
    attribute_id: str
    attribute_value: str
    create_date: Optional[str] = None
    created_by: Optional[str] = None
    update_date: Optional[str] = None
    updated_by: Optional[str] = None

class Product_Attribute_ValueRepository:
    """CRUD operations for Product_Attribute_Value."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def add(self, product_id: str, attribute_id: str, attribute_value: str, create_date: Optional[str] = None, created_by: Optional[str] = None, update_date: Optional[str] = None, updated_by: Optional[str] = None) -> Product_Attribute_Value:
        product_attribute_value_id = str(uuid.uuid4())
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO Product_Attribute_Value (product_attribute_value_id, product_id, attribute_id, attribute_value, create_date, created_by, update_date, updated_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (product_attribute_value_id, product_id, attribute_id, attribute_value, create_date, created_by, update_date, updated_by,),
            )
        return Product_Attribute_Value(product_attribute_value_id=product_attribute_value_id, product_id=product_id, attribute_id=attribute_id, attribute_value=attribute_value, create_date=create_date, created_by=created_by, update_date=update_date, updated_by=updated_by)

    def update(self, product_attribute_value_id: str, **kwargs) -> None:
        allowed = {'update_date', 'attribute_id', 'product_id', 'attribute_value', 'updated_by', 'created_by', 'create_date'}
        fields = {k: v for k, v in kwargs.items() if k in allowed}
        if not fields:
            return
        set_clause = ', '.join(f'{k} = ?' for k in fields)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                f"UPDATE Product_Attribute_Value SET {set_clause} WHERE product_attribute_value_id = ?",
                (*fields.values(), product_attribute_value_id),
            )

    def delete(self, product_attribute_value_id: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "DELETE FROM Product_Attribute_Value WHERE product_attribute_value_id = ?",
                (product_attribute_value_id,),
            )

# ============================================================
# Product_Hierarchy
# ============================================================

@dataclass
class Product_Hierarchy:
    """Hierarchy type table for the Product domain"""
    product_hierarchy_id: str
    product_hierarchy_name: str
    create_date: Optional[str] = None
    created_by: Optional[str] = None
    update_date: Optional[str] = None
    updated_by: Optional[str] = None

class Product_HierarchyRepository:
    """CRUD operations for Product_Hierarchy."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def add(self, product_hierarchy_name: str, create_date: Optional[str] = None, created_by: Optional[str] = None, update_date: Optional[str] = None, updated_by: Optional[str] = None) -> Product_Hierarchy:
        product_hierarchy_id = str(uuid.uuid4())
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO Product_Hierarchy (product_hierarchy_id, product_hierarchy_name, create_date, created_by, update_date, updated_by) VALUES (?, ?, ?, ?, ?, ?)",
                (product_hierarchy_id, product_hierarchy_name, create_date, created_by, update_date, updated_by,),
            )
        return Product_Hierarchy(product_hierarchy_id=product_hierarchy_id, product_hierarchy_name=product_hierarchy_name, create_date=create_date, created_by=created_by, update_date=update_date, updated_by=updated_by)

    def update(self, product_hierarchy_id: str, **kwargs) -> None:
        allowed = {'update_date', 'updated_by', 'created_by', 'create_date', 'product_hierarchy_name'}
        fields = {k: v for k, v in kwargs.items() if k in allowed}
        if not fields:
            return
        set_clause = ', '.join(f'{k} = ?' for k in fields)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                f"UPDATE Product_Hierarchy SET {set_clause} WHERE product_hierarchy_id = ?",
                (*fields.values(), product_hierarchy_id),
            )

    def delete(self, product_hierarchy_id: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "DELETE FROM Product_Hierarchy WHERE product_hierarchy_id = ?",
                (product_hierarchy_id,),
            )

# ============================================================
# Product_Hierarchy_Value
# ============================================================

@dataclass
class Product_Hierarchy_Value:
    """Hierarchy value table storing parent/child relationships for the Product domain"""
    product_hierarchy_value_id: str
    product_hierarchy_id: str
    parent_product_id: str
    child_product_id: str
    create_date: Optional[str] = None
    created_by: Optional[str] = None
    update_date: Optional[str] = None
    updated_by: Optional[str] = None

class Product_Hierarchy_ValueRepository:
    """CRUD operations for Product_Hierarchy_Value."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def add(self, product_hierarchy_id: str, parent_product_id: str, child_product_id: str, create_date: Optional[str] = None, created_by: Optional[str] = None, update_date: Optional[str] = None, updated_by: Optional[str] = None) -> Product_Hierarchy_Value:
        product_hierarchy_value_id = str(uuid.uuid4())
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO Product_Hierarchy_Value (product_hierarchy_value_id, product_hierarchy_id, parent_product_id, child_product_id, create_date, created_by, update_date, updated_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (product_hierarchy_value_id, product_hierarchy_id, parent_product_id, child_product_id, create_date, created_by, update_date, updated_by,),
            )
        return Product_Hierarchy_Value(product_hierarchy_value_id=product_hierarchy_value_id, product_hierarchy_id=product_hierarchy_id, parent_product_id=parent_product_id, child_product_id=child_product_id, create_date=create_date, created_by=created_by, update_date=update_date, updated_by=updated_by)

    def update(self, product_hierarchy_value_id: str, **kwargs) -> None:
        allowed = {'update_date', 'parent_product_id', 'updated_by', 'created_by', 'product_hierarchy_id', 'create_date', 'child_product_id'}
        fields = {k: v for k, v in kwargs.items() if k in allowed}
        if not fields:
            return
        set_clause = ', '.join(f'{k} = ?' for k in fields)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                f"UPDATE Product_Hierarchy_Value SET {set_clause} WHERE product_hierarchy_value_id = ?",
                (*fields.values(), product_hierarchy_value_id),
            )

    def delete(self, product_hierarchy_value_id: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "DELETE FROM Product_Hierarchy_Value WHERE product_hierarchy_value_id = ?",
                (product_hierarchy_value_id,),
            )

# ============================================================
# Product_Vendor_Relationship
# ============================================================

@dataclass
class Product_Vendor_Relationship:
    """Relationship bridge table between Product and Vendor"""
    product_vendor_relationship_id: str
    product_id: str
    vendor_id: str
    create_date: Optional[str] = None
    created_by: Optional[str] = None
    update_date: Optional[str] = None
    updated_by: Optional[str] = None

class Product_Vendor_RelationshipRepository:
    """CRUD operations for Product_Vendor_Relationship."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def add(self, product_id: str, vendor_id: str, create_date: Optional[str] = None, created_by: Optional[str] = None, update_date: Optional[str] = None, updated_by: Optional[str] = None) -> Product_Vendor_Relationship:
        product_vendor_relationship_id = str(uuid.uuid4())
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO Product_Vendor_Relationship (product_vendor_relationship_id, product_id, vendor_id, create_date, created_by, update_date, updated_by) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (product_vendor_relationship_id, product_id, vendor_id, create_date, created_by, update_date, updated_by,),
            )
        return Product_Vendor_Relationship(product_vendor_relationship_id=product_vendor_relationship_id, product_id=product_id, vendor_id=vendor_id, create_date=create_date, created_by=created_by, update_date=update_date, updated_by=updated_by)

    def update(self, product_vendor_relationship_id: str, **kwargs) -> None:
        allowed = {'update_date', 'product_id', 'updated_by', 'created_by', 'vendor_id', 'create_date'}
        fields = {k: v for k, v in kwargs.items() if k in allowed}
        if not fields:
            return
        set_clause = ', '.join(f'{k} = ?' for k in fields)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                f"UPDATE Product_Vendor_Relationship SET {set_clause} WHERE product_vendor_relationship_id = ?",
                (*fields.values(), product_vendor_relationship_id),
            )

    def delete(self, product_vendor_relationship_id: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "DELETE FROM Product_Vendor_Relationship WHERE product_vendor_relationship_id = ?",
                (product_vendor_relationship_id,),
            )
