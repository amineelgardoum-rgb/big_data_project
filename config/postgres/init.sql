-- Hive Metastore DB
-- Drop first if you want a clean start (optional)
DROP DATABASE IF EXISTS metastore_db;
CREATE DATABASE metastore_db;

-- Create user for Hive Metastore
DO
$$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'metastore_user') THEN
      CREATE USER metastore_user WITH PASSWORD 'metastore_pass';
   END IF;
END
$$;

GRANT ALL PRIVILEGES ON DATABASE metastore_db TO metastore_user;

-- Application DB
DROP DATABASE IF EXISTS procurement_db;
CREATE DATABASE procurement_db;

DO
$$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'procurement_user') THEN
      CREATE USER procurement_user WITH PASSWORD 'procurement_pass';
   END IF;
END
$$;

GRANT ALL PRIVILEGES ON DATABASE procurement_db TO procurement_user;

-- Now create your tables inside the application DB
-- Connect to the procurement_db as postgres_admin
\c procurement_db

-- Grant all privileges on all tables to the user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO procurement_user;

-- Optional: future tables automatically
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT ALL PRIVILEGES ON TABLES TO procurement_user;


-- Suppliers table
CREATE TABLE suppliers (
    supplier_id VARCHAR(50) PRIMARY KEY,
    supplier_name VARCHAR(200) NOT NULL,
    contact_email VARCHAR(200),
    lead_time_days INTEGER NOT NULL DEFAULT 2
);

-- Products table
CREATE TABLE products (
    sku_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    category VARCHAR(100),
    supplier_id VARCHAR(50) REFERENCES suppliers(supplier_id),
    pack_size INTEGER NOT NULL DEFAULT 1,
    min_order_qty INTEGER NOT NULL DEFAULT 0,
    unit_price DECIMAL(10,2),
    safety_stock INTEGER NOT NULL DEFAULT 10
);

-- Warehouses table
CREATE TABLE warehouses (
    warehouse_id VARCHAR(50) PRIMARY KEY,
    warehouse_name VARCHAR(200) NOT NULL,
    location VARCHAR(200)
);

-- Indexes for performance
CREATE INDEX idx_products_supplier ON products(supplier_id);
CREATE INDEX idx_products_category ON products(category);
