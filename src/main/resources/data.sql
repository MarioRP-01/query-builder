INSERT INTO customers (id, name, region, tier) VALUES (1, 'Acme Corp', 'US', 'GOLD');
INSERT INTO customers (id, name, region, tier) VALUES (2, 'GlobalTech', 'EU', 'SILVER');
INSERT INTO customers (id, name, region, tier) VALUES (3, 'MegaStore', 'APAC', 'GOLD');

INSERT INTO products (id, name, category, price) VALUES (1, 'Widget A', 'ELECTRONICS', 99.99);
INSERT INTO products (id, name, category, price) VALUES (2, 'Widget B', 'BOOKS', 24.99);
INSERT INTO products (id, name, category, price) VALUES (3, 'Widget C', 'ELECTRONICS', 149.99);

INSERT INTO orders (id, amount, status, category, region, customer_id, product_id, created_date)
VALUES (1, 250.00, 'PENDING', 'ELECTRONICS', 'US', 1, 1, DATE '2024-01-15');
INSERT INTO orders (id, amount, status, category, region, customer_id, product_id, created_date)
VALUES (2, 1500.00, 'COMPLETED', 'BOOKS', 'EU', 2, 2, DATE '2024-01-10');
INSERT INTO orders (id, amount, status, category, region, customer_id, product_id, created_date)
VALUES (3, 75.50, 'PENDING', 'ELECTRONICS', 'APAC', 3, 3, DATE '2024-02-01');
INSERT INTO orders (id, amount, status, category, region, customer_id, product_id, created_date)
VALUES (4, 3200.00, 'PENDING', 'ELECTRONICS', 'US', 1, 3, DATE '2024-02-15');
INSERT INTO orders (id, amount, status, category, region, customer_id, product_id, created_date)
VALUES (5, 499.99, 'SHIPPED', 'BOOKS', 'EU', 2, 2, DATE '2024-01-20');

INSERT INTO payments (id, order_id, amount, status, payment_date)
VALUES (1, 1, 250.00, 'PAID', DATE '2024-01-16');
INSERT INTO payments (id, order_id, amount, status, payment_date)
VALUES (2, 2, 1500.00, 'PAID', DATE '2024-01-12');
INSERT INTO payments (id, order_id, amount, status, payment_date)
VALUES (3, 5, 499.99, 'PENDING', DATE '2024-01-25');
