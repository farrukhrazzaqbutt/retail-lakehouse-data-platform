-- =============================================================================
-- Retail Lakehouse Data Platform — PostgreSQL Initialization
-- Phase 1: Transactional source schema for customers, products, orders,
--          order items, and payments.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS retail;

SET search_path TO retail, public;

-- ---------------------------------------------------------------------------
-- Customers
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS retail.customers (
    customer_id         BIGINT          PRIMARY KEY,
    email               VARCHAR(255)    NOT NULL UNIQUE,
    first_name          VARCHAR(100)    NOT NULL,
    last_name           VARCHAR(100)    NOT NULL,
    phone               VARCHAR(30),
    country_code        CHAR(2)         NOT NULL,
    country_name        VARCHAR(100)    NOT NULL,
    city                VARCHAR(100)    NOT NULL,
    postal_code         VARCHAR(20),
    customer_segment    VARCHAR(50)     NOT NULL,
    signup_date         DATE            NOT NULL,
    is_active           BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    source_system       VARCHAR(100)    NOT NULL DEFAULT 'retail_postgres_generator',

    CONSTRAINT chk_customers_segment
        CHECK (customer_segment IN ('budget', 'standard', 'premium'))
);

CREATE INDEX IF NOT EXISTS idx_customers_country_code
    ON retail.customers (country_code);

CREATE INDEX IF NOT EXISTS idx_customers_signup_date
    ON retail.customers (signup_date);

-- ---------------------------------------------------------------------------
-- Products
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS retail.products (
    product_id          BIGINT          PRIMARY KEY,
    sku                 VARCHAR(50)     NOT NULL UNIQUE,
    product_name        VARCHAR(255)    NOT NULL,
    category            VARCHAR(100)    NOT NULL,
    subcategory         VARCHAR(100)    NOT NULL,
    brand               VARCHAR(100)    NOT NULL,
    unit_price          NUMERIC(12, 2)  NOT NULL,
    unit_cost           NUMERIC(12, 2)  NOT NULL,
    currency            CHAR(3)         NOT NULL DEFAULT 'USD',
    is_active           BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    source_system       VARCHAR(100)    NOT NULL DEFAULT 'retail_postgres_generator',

    CONSTRAINT chk_products_unit_price_positive
        CHECK (unit_price > 0),
    CONSTRAINT chk_products_unit_cost_non_negative
        CHECK (unit_cost >= 0)
);

CREATE INDEX IF NOT EXISTS idx_products_category
    ON retail.products (category);

CREATE INDEX IF NOT EXISTS idx_products_sku
    ON retail.products (sku);

-- ---------------------------------------------------------------------------
-- Orders
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS retail.orders (
    order_id            BIGINT          PRIMARY KEY,
    customer_id         BIGINT          NOT NULL,
    order_date          TIMESTAMPTZ     NOT NULL,
    order_status        VARCHAR(50)     NOT NULL,
    shipping_country    CHAR(2)         NOT NULL,
    shipping_city       VARCHAR(100)    NOT NULL,
    currency            CHAR(3)         NOT NULL DEFAULT 'USD',
    subtotal_amount     NUMERIC(14, 2)  NOT NULL,
    discount_amount     NUMERIC(14, 2)  NOT NULL DEFAULT 0,
    shipping_amount     NUMERIC(14, 2)  NOT NULL DEFAULT 0,
    tax_amount          NUMERIC(14, 2)  NOT NULL DEFAULT 0,
    total_amount        NUMERIC(14, 2)  NOT NULL,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    source_system       VARCHAR(100)    NOT NULL DEFAULT 'retail_postgres_generator',

    CONSTRAINT fk_orders_customer
        FOREIGN KEY (customer_id) REFERENCES retail.customers (customer_id),
    CONSTRAINT chk_orders_status
        CHECK (order_status IN ('completed', 'cancelled', 'refunded', 'pending', 'failed')),
    CONSTRAINT chk_orders_amounts_non_negative
        CHECK (
            subtotal_amount >= 0
            AND discount_amount >= 0
            AND shipping_amount >= 0
            AND tax_amount >= 0
            AND total_amount >= 0
        )
);

CREATE INDEX IF NOT EXISTS idx_orders_customer_id
    ON retail.orders (customer_id);

CREATE INDEX IF NOT EXISTS idx_orders_order_date
    ON retail.orders (order_date);

CREATE INDEX IF NOT EXISTS idx_orders_status
    ON retail.orders (order_status);

-- ---------------------------------------------------------------------------
-- Order Items
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS retail.order_items (
    order_item_id       BIGINT          PRIMARY KEY,
    order_id            BIGINT          NOT NULL,
    product_id          BIGINT          NOT NULL,
    quantity            INTEGER         NOT NULL,
    unit_price          NUMERIC(12, 2)  NOT NULL,
    discount_pct        NUMERIC(5, 4)   NOT NULL DEFAULT 0,
    line_total          NUMERIC(14, 2)  NOT NULL,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    source_system       VARCHAR(100)    NOT NULL DEFAULT 'retail_postgres_generator',

    CONSTRAINT fk_order_items_order
        FOREIGN KEY (order_id) REFERENCES retail.orders (order_id),
    CONSTRAINT fk_order_items_product
        FOREIGN KEY (product_id) REFERENCES retail.products (product_id),
    CONSTRAINT chk_order_items_quantity_positive
        CHECK (quantity > 0),
    CONSTRAINT chk_order_items_unit_price_positive
        CHECK (unit_price > 0),
    CONSTRAINT chk_order_items_discount_pct
        CHECK (discount_pct >= 0 AND discount_pct <= 1),
    CONSTRAINT chk_order_items_line_total_non_negative
        CHECK (line_total >= 0)
);

CREATE INDEX IF NOT EXISTS idx_order_items_order_id
    ON retail.order_items (order_id);

CREATE INDEX IF NOT EXISTS idx_order_items_product_id
    ON retail.order_items (product_id);

-- ---------------------------------------------------------------------------
-- Payments
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS retail.payments (
    payment_id          BIGINT          PRIMARY KEY,
    order_id            BIGINT          NOT NULL,
    payment_date        TIMESTAMPTZ     NOT NULL,
    payment_method      VARCHAR(50)     NOT NULL,
    payment_status      VARCHAR(50)     NOT NULL,
    amount              NUMERIC(14, 2)  NOT NULL,
    currency            CHAR(3)         NOT NULL DEFAULT 'USD',
    transaction_ref     VARCHAR(100)    NOT NULL UNIQUE,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    source_system       VARCHAR(100)    NOT NULL DEFAULT 'retail_postgres_generator',

    CONSTRAINT fk_payments_order
        FOREIGN KEY (order_id) REFERENCES retail.orders (order_id),
    CONSTRAINT chk_payments_method
        CHECK (payment_method IN ('credit_card', 'debit_card', 'paypal', 'bank_transfer', 'gift_card')),
    CONSTRAINT chk_payments_status
        CHECK (payment_status IN ('succeeded', 'failed', 'pending', 'refunded')),
    CONSTRAINT chk_payments_amount_non_negative
        CHECK (amount >= 0)
);

CREATE INDEX IF NOT EXISTS idx_payments_order_id
    ON retail.payments (order_id);

CREATE INDEX IF NOT EXISTS idx_payments_payment_date
    ON retail.payments (payment_date);

CREATE INDEX IF NOT EXISTS idx_payments_status
    ON retail.payments (payment_status);

-- ---------------------------------------------------------------------------
-- Audit helper: updated_at trigger
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION retail.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_customers_updated_at
    BEFORE UPDATE ON retail.customers
    FOR EACH ROW EXECUTE FUNCTION retail.set_updated_at();

CREATE TRIGGER trg_products_updated_at
    BEFORE UPDATE ON retail.products
    FOR EACH ROW EXECUTE FUNCTION retail.set_updated_at();

CREATE TRIGGER trg_orders_updated_at
    BEFORE UPDATE ON retail.orders
    FOR EACH ROW EXECUTE FUNCTION retail.set_updated_at();

CREATE TRIGGER trg_payments_updated_at
    BEFORE UPDATE ON retail.payments
    FOR EACH ROW EXECUTE FUNCTION retail.set_updated_at();

-- ---------------------------------------------------------------------------
-- Validation view (useful for Phase 1 smoke checks)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW retail.v_table_counts AS
SELECT 'customers'   AS table_name, COUNT(*) AS row_count FROM retail.customers
UNION ALL
SELECT 'products',    COUNT(*) FROM retail.products
UNION ALL
SELECT 'orders',      COUNT(*) FROM retail.orders
UNION ALL
SELECT 'order_items', COUNT(*) FROM retail.order_items
UNION ALL
SELECT 'payments',    COUNT(*) FROM retail.payments;
