CREATE TABLE customers (
    id BIGINT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    region VARCHAR(50),
    tier VARCHAR(20)
);

CREATE TABLE products (
    id BIGINT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    price DECIMAL(10,2)
);

CREATE TABLE orders (
    id BIGINT PRIMARY KEY,
    amount DECIMAL(10,2),
    status VARCHAR(20),
    category VARCHAR(50),
    region VARCHAR(50),
    customer_id BIGINT,
    product_id BIGINT,
    created_date DATE,
    CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES customers(id),
    CONSTRAINT fk_orders_product FOREIGN KEY (product_id) REFERENCES products(id)
);

CREATE TABLE payments (
    id BIGINT PRIMARY KEY,
    order_id BIGINT,
    amount DECIMAL(10,2),
    status VARCHAR(20),
    payment_date DATE,
    CONSTRAINT fk_payments_order FOREIGN KEY (order_id) REFERENCES orders(id)
);

CREATE TABLE order_summaries (
    order_id BIGINT PRIMARY KEY,
    customer_name VARCHAR(100),
    customer_tier VARCHAR(20),
    product_name VARCHAR(100),
    original_amount DECIMAL(10,2),
    tax_amount DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    final_amount DECIMAL(10,2),
    priority VARCHAR(20),
    processed_date DATE,
    CONSTRAINT fk_summaries_order FOREIGN KEY (order_id) REFERENCES orders(id)
);

CREATE TABLE order_analytics (
    order_id BIGINT PRIMARY KEY,
    amount DECIMAL(10,2),
    created_date DATE,
    customer_name VARCHAR(100),
    region VARCHAR(50),
    tier VARCHAR(20),
    customer_order_seq BIGINT,
    customer_running_total DECIMAL(12,2),
    prev_amount DECIMAL(10,2),
    region_amount_rank BIGINT,
    region_spend_pct DECIMAL(10,6),
    spend_quartile BIGINT,
    trend VARCHAR(20),
    velocity_flag VARCHAR(20),
    CONSTRAINT fk_analytics_order FOREIGN KEY (order_id) REFERENCES orders(id)
);
