import sqlite3
import uuid
from dataclasses import dataclass
from typing import Optional

# ============================================================
# Customer
# ============================================================

@dataclass
class Customer:
    """Customer"""
    customer_id: str
    email: str
    first_name: str
    last_name: str
    status_id: str
    type_id: str

class CustomerRepository:
    """CRUD operations for Customer."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def add(self, email: str, first_name: str, last_name: str, status_id: str, type_id: str) -> Customer:
        customer_id = str(uuid.uuid4())
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO Customer (customer_id, email, first_name, last_name, status_id, type_id) VALUES (?, ?, ?, ?, ?, ?)",
                (customer_id, email, first_name, last_name, status_id, type_id,),
            )
        return Customer(customer_id=customer_id, email=email, first_name=first_name, last_name=last_name, status_id=status_id, type_id=type_id)

    def update(self, customer_id: str, **kwargs) -> None:
        allowed = {'last_name', 'first_name', 'type_id', 'email', 'status_id'}
        fields = {k: v for k, v in kwargs.items() if k in allowed}
        if not fields:
            return
        set_clause = ', '.join(f'{k} = ?' for k in fields)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                f"UPDATE Customer SET {set_clause} WHERE customer_id = ?",
                (*fields.values(), customer_id),
            )

    def delete(self, customer_id: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "DELETE FROM Customer WHERE customer_id = ?",
                (customer_id,),
            )

# ============================================================
# Customer_Status
# ============================================================

@dataclass
class Customer_Status:
    """Status reference table for the Customer domain"""
    status_id: str
    status_name: str
    status_description: Optional[str] = None

class Customer_StatusRepository:
    """CRUD operations for Customer_Status."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def add(self, status_name: str, status_description: Optional[str] = None) -> Customer_Status:
        status_id = str(uuid.uuid4())
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO Customer_Status (status_id, status_name, status_description) VALUES (?, ?, ?)",
                (status_id, status_name, status_description,),
            )
        return Customer_Status(status_id=status_id, status_name=status_name, status_description=status_description)

    def update(self, status_id: str, **kwargs) -> None:
        allowed = {'status_description', 'status_name'}
        fields = {k: v for k, v in kwargs.items() if k in allowed}
        if not fields:
            return
        set_clause = ', '.join(f'{k} = ?' for k in fields)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                f"UPDATE Customer_Status SET {set_clause} WHERE status_id = ?",
                (*fields.values(), status_id),
            )

    def delete(self, status_id: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "DELETE FROM Customer_Status WHERE status_id = ?",
                (status_id,),
            )

# ============================================================
# Customer_Type
# ============================================================

@dataclass
class Customer_Type:
    """Type reference table for the Customer domain"""
    type_id: str
    type_name: str
    type_description: Optional[str] = None

class Customer_TypeRepository:
    """CRUD operations for Customer_Type."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def add(self, type_name: str, type_description: Optional[str] = None) -> Customer_Type:
        type_id = str(uuid.uuid4())
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO Customer_Type (type_id, type_name, type_description) VALUES (?, ?, ?)",
                (type_id, type_name, type_description,),
            )
        return Customer_Type(type_id=type_id, type_name=type_name, type_description=type_description)

    def update(self, type_id: str, **kwargs) -> None:
        allowed = {'type_description', 'type_name'}
        fields = {k: v for k, v in kwargs.items() if k in allowed}
        if not fields:
            return
        set_clause = ', '.join(f'{k} = ?' for k in fields)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                f"UPDATE Customer_Type SET {set_clause} WHERE type_id = ?",
                (*fields.values(), type_id),
            )

    def delete(self, type_id: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "DELETE FROM Customer_Type WHERE type_id = ?",
                (type_id,),
            )

# ============================================================
# Customer_Attribute
# ============================================================

@dataclass
class Customer_Attribute:
    """Attribute reference table for the Customer domain"""
    attribute_id: str
    attribute_name: str

class Customer_AttributeRepository:
    """CRUD operations for Customer_Attribute."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def add(self, attribute_name: str) -> Customer_Attribute:
        attribute_id = str(uuid.uuid4())
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO Customer_Attribute (attribute_id, attribute_name) VALUES (?, ?)",
                (attribute_id, attribute_name,),
            )
        return Customer_Attribute(attribute_id=attribute_id, attribute_name=attribute_name)

    def update(self, attribute_id: str, **kwargs) -> None:
        allowed = {'attribute_name'}
        fields = {k: v for k, v in kwargs.items() if k in allowed}
        if not fields:
            return
        set_clause = ', '.join(f'{k} = ?' for k in fields)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                f"UPDATE Customer_Attribute SET {set_clause} WHERE attribute_id = ?",
                (*fields.values(), attribute_id),
            )

    def delete(self, attribute_id: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "DELETE FROM Customer_Attribute WHERE attribute_id = ?",
                (attribute_id,),
            )

# ============================================================
# Customer_Attribute_Value
# ============================================================

@dataclass
class Customer_Attribute_Value:
    """Bridge table linking Customer records to their attribute values"""
    customer_attribute_value_id: str
    customer_id: str
    attribute_id: str
    attribute_value: str

class Customer_Attribute_ValueRepository:
    """CRUD operations for Customer_Attribute_Value."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def add(self, customer_id: str, attribute_id: str, attribute_value: str) -> Customer_Attribute_Value:
        customer_attribute_value_id = str(uuid.uuid4())
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO Customer_Attribute_Value (customer_attribute_value_id, customer_id, attribute_id, attribute_value) VALUES (?, ?, ?, ?)",
                (customer_attribute_value_id, customer_id, attribute_id, attribute_value,),
            )
        return Customer_Attribute_Value(customer_attribute_value_id=customer_attribute_value_id, customer_id=customer_id, attribute_id=attribute_id, attribute_value=attribute_value)

    def update(self, customer_attribute_value_id: str, **kwargs) -> None:
        allowed = {'customer_id', 'attribute_id', 'attribute_value'}
        fields = {k: v for k, v in kwargs.items() if k in allowed}
        if not fields:
            return
        set_clause = ', '.join(f'{k} = ?' for k in fields)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                f"UPDATE Customer_Attribute_Value SET {set_clause} WHERE customer_attribute_value_id = ?",
                (*fields.values(), customer_attribute_value_id),
            )

    def delete(self, customer_attribute_value_id: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "DELETE FROM Customer_Attribute_Value WHERE customer_attribute_value_id = ?",
                (customer_attribute_value_id,),
            )

# ============================================================
# Customer_Hierarchy
# ============================================================

@dataclass
class Customer_Hierarchy:
    """Hierarchy type table for the Customer domain"""
    customer_hierarchy_id: str
    customer_hierarchy_name: str

class Customer_HierarchyRepository:
    """CRUD operations for Customer_Hierarchy."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def add(self, customer_hierarchy_name: str) -> Customer_Hierarchy:
        customer_hierarchy_id = str(uuid.uuid4())
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO Customer_Hierarchy (customer_hierarchy_id, customer_hierarchy_name) VALUES (?, ?)",
                (customer_hierarchy_id, customer_hierarchy_name,),
            )
        return Customer_Hierarchy(customer_hierarchy_id=customer_hierarchy_id, customer_hierarchy_name=customer_hierarchy_name)

    def update(self, customer_hierarchy_id: str, **kwargs) -> None:
        allowed = {'customer_hierarchy_name'}
        fields = {k: v for k, v in kwargs.items() if k in allowed}
        if not fields:
            return
        set_clause = ', '.join(f'{k} = ?' for k in fields)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                f"UPDATE Customer_Hierarchy SET {set_clause} WHERE customer_hierarchy_id = ?",
                (*fields.values(), customer_hierarchy_id),
            )

    def delete(self, customer_hierarchy_id: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "DELETE FROM Customer_Hierarchy WHERE customer_hierarchy_id = ?",
                (customer_hierarchy_id,),
            )

# ============================================================
# Customer_Hierarchy_Value
# ============================================================

@dataclass
class Customer_Hierarchy_Value:
    """Hierarchy value table storing parent/child relationships for the Customer domain"""
    customer_hierarchy_value_id: str
    customer_hierarchy_id: str
    parent_customer_id: str
    child_customer_id: str

class Customer_Hierarchy_ValueRepository:
    """CRUD operations for Customer_Hierarchy_Value."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def add(self, customer_hierarchy_id: str, parent_customer_id: str, child_customer_id: str) -> Customer_Hierarchy_Value:
        customer_hierarchy_value_id = str(uuid.uuid4())
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO Customer_Hierarchy_Value (customer_hierarchy_value_id, customer_hierarchy_id, parent_customer_id, child_customer_id) VALUES (?, ?, ?, ?)",
                (customer_hierarchy_value_id, customer_hierarchy_id, parent_customer_id, child_customer_id,),
            )
        return Customer_Hierarchy_Value(customer_hierarchy_value_id=customer_hierarchy_value_id, customer_hierarchy_id=customer_hierarchy_id, parent_customer_id=parent_customer_id, child_customer_id=child_customer_id)

    def update(self, customer_hierarchy_value_id: str, **kwargs) -> None:
        allowed = {'child_customer_id', 'parent_customer_id', 'customer_hierarchy_id'}
        fields = {k: v for k, v in kwargs.items() if k in allowed}
        if not fields:
            return
        set_clause = ', '.join(f'{k} = ?' for k in fields)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                f"UPDATE Customer_Hierarchy_Value SET {set_clause} WHERE customer_hierarchy_value_id = ?",
                (*fields.values(), customer_hierarchy_value_id),
            )

    def delete(self, customer_hierarchy_value_id: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "DELETE FROM Customer_Hierarchy_Value WHERE customer_hierarchy_value_id = ?",
                (customer_hierarchy_value_id,),
            )
