# Generated PySpark/Delta library for domain: Customer
# Catalog: main  |  Database: adm
#
# Run this file in a Databricks notebook or spark-submit job.

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType

spark = SparkSession.builder.getOrCreate()

spark.sql('CREATE DATABASE IF NOT EXISTS main.adm')

# ============================================================
# Customer
# ============================================================

customer_schema = StructType([
    StructField('customer_id', StringType(), nullable=False),  # System-generated identity field for the Customer domain
    StructField('email', StringType(), nullable=False),  # Natural key field for the Customer domain
    StructField('first_name', StringType(), nullable=False),  # Natural key field for the Customer domain
    StructField('last_name', StringType(), nullable=False),  # Natural key field for the Customer domain
    StructField('status_id', StringType(), nullable=False),  # Foreign key to Customer_Status
    StructField('type_id', StringType(), nullable=False),  # Foreign key to Customer_Type
])

(
    spark.createDataFrame([], customer_schema)
    .write
    .format('delta')
    .mode('ignore')
    .option('comment', 'Customer')
    .saveAsTable('main.adm.Customer')
)

spark.sql("ALTER TABLE main.adm.Customer ADD CONSTRAINT pk_Customer PRIMARY KEY (customer_id)")
spark.sql("ALTER TABLE main.adm.Customer ADD CONSTRAINT fk_Customer_status_id FOREIGN KEY (status_id) REFERENCES main.adm.Customer_Status(status_id)")
spark.sql("ALTER TABLE main.adm.Customer ADD CONSTRAINT fk_Customer_type_id FOREIGN KEY (type_id) REFERENCES main.adm.Customer_Type(type_id)")
print('  Created table: Customer')

# ============================================================
# Customer_Status
# ============================================================

customer_status_schema = StructType([
    StructField('status_id', StringType(), nullable=False),  # Primary key for Customer_Status
    StructField('status_name', StringType(), nullable=False),  # Name of the status
    StructField('status_description', StringType(), nullable=True),  # Description of what this status means
])

(
    spark.createDataFrame([], customer_status_schema)
    .write
    .format('delta')
    .mode('ignore')
    .option('comment', 'Status reference table for the Customer domain')
    .saveAsTable('main.adm.Customer_Status')
)

spark.sql("ALTER TABLE main.adm.Customer_Status ADD CONSTRAINT pk_Customer_Status PRIMARY KEY (status_id)")
print('  Created table: Customer_Status')

# ============================================================
# Customer_Type
# ============================================================

customer_type_schema = StructType([
    StructField('type_id', StringType(), nullable=False),  # Primary key for Customer_Type
    StructField('type_name', StringType(), nullable=False),  # Name of the type
    StructField('type_description', StringType(), nullable=True),  # Description of what this type means
])

(
    spark.createDataFrame([], customer_type_schema)
    .write
    .format('delta')
    .mode('ignore')
    .option('comment', 'Type reference table for the Customer domain')
    .saveAsTable('main.adm.Customer_Type')
)

spark.sql("ALTER TABLE main.adm.Customer_Type ADD CONSTRAINT pk_Customer_Type PRIMARY KEY (type_id)")
print('  Created table: Customer_Type')

# ============================================================
# Customer_Attribute
# ============================================================

customer_attribute_schema = StructType([
    StructField('attribute_id', StringType(), nullable=False),  # System-generated identity key for Customer_Attribute
    StructField('attribute_name', StringType(), nullable=False),  # Name of the attribute
])

(
    spark.createDataFrame([], customer_attribute_schema)
    .write
    .format('delta')
    .mode('ignore')
    .option('comment', 'Attribute reference table for the Customer domain')
    .saveAsTable('main.adm.Customer_Attribute')
)

spark.sql("ALTER TABLE main.adm.Customer_Attribute ADD CONSTRAINT pk_Customer_Attribute PRIMARY KEY (attribute_id)")
print('  Created table: Customer_Attribute')

# ============================================================
# Customer_Hierarchy
# ============================================================

customer_hierarchy_schema = StructType([
    StructField('customer_hierarchy_id', StringType(), nullable=False),  # System-generated identity key for Customer_Hierarchy
    StructField('customer_hierarchy_name', StringType(), nullable=False),  # Name of the hierarchy
])

(
    spark.createDataFrame([], customer_hierarchy_schema)
    .write
    .format('delta')
    .mode('ignore')
    .option('comment', 'Hierarchy type table for the Customer domain')
    .saveAsTable('main.adm.Customer_Hierarchy')
)

spark.sql("ALTER TABLE main.adm.Customer_Hierarchy ADD CONSTRAINT pk_Customer_Hierarchy PRIMARY KEY (customer_hierarchy_id)")
print('  Created table: Customer_Hierarchy')

# ============================================================
# Customer_Attribute_Value
# ============================================================

customer_attribute_value_schema = StructType([
    StructField('customer_attribute_value_id', StringType(), nullable=False),  # System-generated identity key for Customer_Attribute_Value
    StructField('customer_id', StringType(), nullable=False),  # Foreign key to Customer
    StructField('attribute_id', StringType(), nullable=False),  # Foreign key to Customer_Attribute
    StructField('attribute_value', StringType(), nullable=False),  # Value of the attribute for this domain record
])

(
    spark.createDataFrame([], customer_attribute_value_schema)
    .write
    .format('delta')
    .mode('ignore')
    .option('comment', 'Bridge table linking Customer records to their attribute values')
    .saveAsTable('main.adm.Customer_Attribute_Value')
)

spark.sql("ALTER TABLE main.adm.Customer_Attribute_Value ADD CONSTRAINT pk_Customer_Attribute_Value PRIMARY KEY (customer_attribute_value_id)")
spark.sql("ALTER TABLE main.adm.Customer_Attribute_Value ADD CONSTRAINT fk_Customer_Attribute_Value_customer_id FOREIGN KEY (customer_id) REFERENCES main.adm.Customer(customer_id)")
spark.sql("ALTER TABLE main.adm.Customer_Attribute_Value ADD CONSTRAINT fk_Customer_Attribute_Value_attribute_id FOREIGN KEY (attribute_id) REFERENCES main.adm.Customer_Attribute(attribute_id)")
print('  Created table: Customer_Attribute_Value')

# ============================================================
# Customer_Hierarchy_Value
# ============================================================

customer_hierarchy_value_schema = StructType([
    StructField('customer_hierarchy_value_id', StringType(), nullable=False),  # System-generated identity key for Customer_Hierarchy_Value
    StructField('customer_hierarchy_id', StringType(), nullable=False),  # Foreign key to Customer_Hierarchy
    StructField('parent_customer_id', StringType(), nullable=False),  # Foreign key to Customer — the parent record
    StructField('child_customer_id', StringType(), nullable=False),  # Foreign key to Customer — the child record
])

(
    spark.createDataFrame([], customer_hierarchy_value_schema)
    .write
    .format('delta')
    .mode('ignore')
    .option('comment', 'Hierarchy value table storing parent/child relationships for the Customer domain')
    .saveAsTable('main.adm.Customer_Hierarchy_Value')
)

spark.sql("ALTER TABLE main.adm.Customer_Hierarchy_Value ADD CONSTRAINT pk_Customer_Hierarchy_Value PRIMARY KEY (customer_hierarchy_value_id)")
spark.sql("ALTER TABLE main.adm.Customer_Hierarchy_Value ADD CONSTRAINT fk_Customer_Hierarchy_Value_customer_hierarchy_id FOREIGN KEY (customer_hierarchy_id) REFERENCES main.adm.Customer_Hierarchy(customer_hierarchy_id)")
spark.sql("ALTER TABLE main.adm.Customer_Hierarchy_Value ADD CONSTRAINT fk_Customer_Hierarchy_Value_parent_customer_id FOREIGN KEY (parent_customer_id) REFERENCES main.adm.Customer(customer_id)")
spark.sql("ALTER TABLE main.adm.Customer_Hierarchy_Value ADD CONSTRAINT fk_Customer_Hierarchy_Value_child_customer_id FOREIGN KEY (child_customer_id) REFERENCES main.adm.Customer(customer_id)")
print('  Created table: Customer_Hierarchy_Value')
