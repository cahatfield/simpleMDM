# Generated PySpark/Delta library for domain: Product
# Catalog: main  |  Database: adm
#
# Run this file in a Databricks notebook or spark-submit job.

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType

spark = SparkSession.builder.getOrCreate()

spark.sql('CREATE DATABASE IF NOT EXISTS main.adm')

# ============================================================
# Product
# ============================================================

product_schema = StructType([
    StructField('product_id', StringType(), nullable=False),  # System-generated identity field for the Product domain
    StructField('product_brand_name', StringType(), nullable=False),  # Natural key field for the Product domain
    StructField('product_catalog_number', StringType(), nullable=False),  # Natural key field for the Product domain
    StructField('product_description', StringType(), nullable=False),  # Natural key field for the Product domain
    StructField('status_id', StringType(), nullable=False),  # Foreign key to Product_Status
    StructField('type_id', StringType(), nullable=False),  # Foreign key to Product_Type
    StructField('create_date', StringType(), nullable=True),  # Date and time the record was inserted into the table
    StructField('created_by', StringType(), nullable=True),  # User who inserted the record into the table
    StructField('update_date', StringType(), nullable=True),  # Date and time the record was last updated
    StructField('updated_by', StringType(), nullable=True),  # User who last updated the record
])

(
    spark.createDataFrame([], product_schema)
    .write
    .format('delta')
    .mode('ignore')
    .option('comment', 'Product')
    .saveAsTable('main.adm.Product')
)

spark.sql("ALTER TABLE main.adm.Product ADD CONSTRAINT pk_Product PRIMARY KEY (product_id)")
spark.sql("ALTER TABLE main.adm.Product ADD CONSTRAINT fk_Product_status_id FOREIGN KEY (status_id) REFERENCES main.adm.Product_Status(status_id)")
spark.sql("ALTER TABLE main.adm.Product ADD CONSTRAINT fk_Product_type_id FOREIGN KEY (type_id) REFERENCES main.adm.Product_Type(type_id)")
print('  Created table: Product')

# ============================================================
# Product_Status
# ============================================================

product_status_schema = StructType([
    StructField('status_id', StringType(), nullable=False),  # Primary key for Product_Status
    StructField('status_name', StringType(), nullable=False),  # Name of the status
    StructField('status_description', StringType(), nullable=True),  # Description of what this status means
    StructField('create_date', StringType(), nullable=True),  # Date and time the record was inserted into the table
    StructField('created_by', StringType(), nullable=True),  # User who inserted the record into the table
    StructField('update_date', StringType(), nullable=True),  # Date and time the record was last updated
    StructField('updated_by', StringType(), nullable=True),  # User who last updated the record
])

(
    spark.createDataFrame([], product_status_schema)
    .write
    .format('delta')
    .mode('ignore')
    .option('comment', 'Status reference table for the Product domain')
    .saveAsTable('main.adm.Product_Status')
)

spark.sql("ALTER TABLE main.adm.Product_Status ADD CONSTRAINT pk_Product_Status PRIMARY KEY (status_id)")
print('  Created table: Product_Status')

# ============================================================
# Product_Type
# ============================================================

product_type_schema = StructType([
    StructField('type_id', StringType(), nullable=False),  # Primary key for Product_Type
    StructField('type_name', StringType(), nullable=False),  # Name of the type
    StructField('type_description', StringType(), nullable=True),  # Description of what this type means
    StructField('create_date', StringType(), nullable=True),  # Date and time the record was inserted into the table
    StructField('created_by', StringType(), nullable=True),  # User who inserted the record into the table
    StructField('update_date', StringType(), nullable=True),  # Date and time the record was last updated
    StructField('updated_by', StringType(), nullable=True),  # User who last updated the record
])

(
    spark.createDataFrame([], product_type_schema)
    .write
    .format('delta')
    .mode('ignore')
    .option('comment', 'Type reference table for the Product domain')
    .saveAsTable('main.adm.Product_Type')
)

spark.sql("ALTER TABLE main.adm.Product_Type ADD CONSTRAINT pk_Product_Type PRIMARY KEY (type_id)")
print('  Created table: Product_Type')

# ============================================================
# Product_Attribute
# ============================================================

product_attribute_schema = StructType([
    StructField('attribute_id', StringType(), nullable=False),  # System-generated identity key for Product_Attribute
    StructField('attribute_name', StringType(), nullable=False),  # Name of the attribute
    StructField('create_date', StringType(), nullable=True),  # Date and time the record was inserted into the table
    StructField('created_by', StringType(), nullable=True),  # User who inserted the record into the table
    StructField('update_date', StringType(), nullable=True),  # Date and time the record was last updated
    StructField('updated_by', StringType(), nullable=True),  # User who last updated the record
])

(
    spark.createDataFrame([], product_attribute_schema)
    .write
    .format('delta')
    .mode('ignore')
    .option('comment', 'Attribute reference table for the Product domain')
    .saveAsTable('main.adm.Product_Attribute')
)

spark.sql("ALTER TABLE main.adm.Product_Attribute ADD CONSTRAINT pk_Product_Attribute PRIMARY KEY (attribute_id)")
print('  Created table: Product_Attribute')

# ============================================================
# Product_Hierarchy
# ============================================================

product_hierarchy_schema = StructType([
    StructField('product_hierarchy_id', StringType(), nullable=False),  # System-generated identity key for Product_Hierarchy
    StructField('product_hierarchy_name', StringType(), nullable=False),  # Name of the hierarchy
    StructField('create_date', StringType(), nullable=True),  # Date and time the record was inserted into the table
    StructField('created_by', StringType(), nullable=True),  # User who inserted the record into the table
    StructField('update_date', StringType(), nullable=True),  # Date and time the record was last updated
    StructField('updated_by', StringType(), nullable=True),  # User who last updated the record
])

(
    spark.createDataFrame([], product_hierarchy_schema)
    .write
    .format('delta')
    .mode('ignore')
    .option('comment', 'Hierarchy type table for the Product domain')
    .saveAsTable('main.adm.Product_Hierarchy')
)

spark.sql("ALTER TABLE main.adm.Product_Hierarchy ADD CONSTRAINT pk_Product_Hierarchy PRIMARY KEY (product_hierarchy_id)")
print('  Created table: Product_Hierarchy')

# ============================================================
# Product_Attribute_Value
# ============================================================

product_attribute_value_schema = StructType([
    StructField('product_attribute_value_id', StringType(), nullable=False),  # System-generated identity key for Product_Attribute_Value
    StructField('product_id', StringType(), nullable=False),  # Foreign key to Product
    StructField('attribute_id', StringType(), nullable=False),  # Foreign key to Product_Attribute
    StructField('attribute_value', StringType(), nullable=False),  # Value of the attribute for this domain record
    StructField('create_date', StringType(), nullable=True),  # Date and time the record was inserted into the table
    StructField('created_by', StringType(), nullable=True),  # User who inserted the record into the table
    StructField('update_date', StringType(), nullable=True),  # Date and time the record was last updated
    StructField('updated_by', StringType(), nullable=True),  # User who last updated the record
])

(
    spark.createDataFrame([], product_attribute_value_schema)
    .write
    .format('delta')
    .mode('ignore')
    .option('comment', 'Bridge table linking Product records to their attribute values')
    .saveAsTable('main.adm.Product_Attribute_Value')
)

spark.sql("ALTER TABLE main.adm.Product_Attribute_Value ADD CONSTRAINT pk_Product_Attribute_Value PRIMARY KEY (product_attribute_value_id)")
spark.sql("ALTER TABLE main.adm.Product_Attribute_Value ADD CONSTRAINT fk_Product_Attribute_Value_product_id FOREIGN KEY (product_id) REFERENCES main.adm.Product(product_id)")
spark.sql("ALTER TABLE main.adm.Product_Attribute_Value ADD CONSTRAINT fk_Product_Attribute_Value_attribute_id FOREIGN KEY (attribute_id) REFERENCES main.adm.Product_Attribute(attribute_id)")
print('  Created table: Product_Attribute_Value')

# ============================================================
# Product_Hierarchy_Value
# ============================================================

product_hierarchy_value_schema = StructType([
    StructField('product_hierarchy_value_id', StringType(), nullable=False),  # System-generated identity key for Product_Hierarchy_Value
    StructField('product_hierarchy_id', StringType(), nullable=False),  # Foreign key to Product_Hierarchy
    StructField('parent_product_id', StringType(), nullable=False),  # Foreign key to Product — the parent record
    StructField('child_product_id', StringType(), nullable=False),  # Foreign key to Product — the child record
    StructField('create_date', StringType(), nullable=True),  # Date and time the record was inserted into the table
    StructField('created_by', StringType(), nullable=True),  # User who inserted the record into the table
    StructField('update_date', StringType(), nullable=True),  # Date and time the record was last updated
    StructField('updated_by', StringType(), nullable=True),  # User who last updated the record
])

(
    spark.createDataFrame([], product_hierarchy_value_schema)
    .write
    .format('delta')
    .mode('ignore')
    .option('comment', 'Hierarchy value table storing parent/child relationships for the Product domain')
    .saveAsTable('main.adm.Product_Hierarchy_Value')
)

spark.sql("ALTER TABLE main.adm.Product_Hierarchy_Value ADD CONSTRAINT pk_Product_Hierarchy_Value PRIMARY KEY (product_hierarchy_value_id)")
spark.sql("ALTER TABLE main.adm.Product_Hierarchy_Value ADD CONSTRAINT fk_Product_Hierarchy_Value_product_hierarchy_id FOREIGN KEY (product_hierarchy_id) REFERENCES main.adm.Product_Hierarchy(product_hierarchy_id)")
spark.sql("ALTER TABLE main.adm.Product_Hierarchy_Value ADD CONSTRAINT fk_Product_Hierarchy_Value_parent_product_id FOREIGN KEY (parent_product_id) REFERENCES main.adm.Product(product_id)")
spark.sql("ALTER TABLE main.adm.Product_Hierarchy_Value ADD CONSTRAINT fk_Product_Hierarchy_Value_child_product_id FOREIGN KEY (child_product_id) REFERENCES main.adm.Product(product_id)")
print('  Created table: Product_Hierarchy_Value')

# ============================================================
# Product_Vendor_Relationship
# ============================================================

product_vendor_relationship_schema = StructType([
    StructField('product_vendor_relationship_id', StringType(), nullable=False),  # System-generated identity key for Product_Vendor_Relationship
    StructField('product_id', StringType(), nullable=False),  # Foreign key to Product
    StructField('vendor_id', StringType(), nullable=False),  # Foreign key to Vendor
    StructField('create_date', StringType(), nullable=True),  # Date and time the record was inserted into the table
    StructField('created_by', StringType(), nullable=True),  # User who inserted the record into the table
    StructField('update_date', StringType(), nullable=True),  # Date and time the record was last updated
    StructField('updated_by', StringType(), nullable=True),  # User who last updated the record
])

(
    spark.createDataFrame([], product_vendor_relationship_schema)
    .write
    .format('delta')
    .mode('ignore')
    .option('comment', 'Relationship bridge table between Product and Vendor')
    .saveAsTable('main.adm.Product_Vendor_Relationship')
)

spark.sql("ALTER TABLE main.adm.Product_Vendor_Relationship ADD CONSTRAINT pk_Product_Vendor_Relationship PRIMARY KEY (product_vendor_relationship_id)")
spark.sql("ALTER TABLE main.adm.Product_Vendor_Relationship ADD CONSTRAINT fk_Product_Vendor_Relationship_product_id FOREIGN KEY (product_id) REFERENCES main.adm.Product(product_id)")
spark.sql("ALTER TABLE main.adm.Product_Vendor_Relationship ADD CONSTRAINT fk_Product_Vendor_Relationship_vendor_id FOREIGN KEY (vendor_id) REFERENCES main.adm.Vendor(vendor_id)")
print('  Created table: Product_Vendor_Relationship')
