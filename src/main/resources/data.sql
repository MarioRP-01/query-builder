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

-- Additional orders for analytics richness (non-PENDING — existing tests unaffected)
INSERT INTO orders (id, amount, status, category, region, customer_id, product_id, created_date)
VALUES (6, 800.00, 'COMPLETED', 'BOOKS', 'US', 1, 2, DATE '2024-01-20');
INSERT INTO orders (id, amount, status, category, region, customer_id, product_id, created_date)
VALUES (7, 2100.00, 'COMPLETED', 'ELECTRONICS', 'EU', 2, 1, DATE '2024-02-10');

INSERT INTO payments (id, order_id, amount, status, payment_date)
VALUES (4, 6, 800.00, 'PAID', DATE '2024-01-22');
INSERT INTO payments (id, order_id, amount, status, payment_date)
VALUES (5, 7, 2100.00, 'PAID', DATE '2024-02-12');

-- Evaluation groups
INSERT INTO evaluation_groups (id, name, status) VALUES (1, 'Alpha Group', 'ACTIVE');
INSERT INTO evaluation_groups (id, name, status) VALUES (2, 'Beta Group', 'ACTIVE');
INSERT INTO evaluation_groups (id, name, status) VALUES (3, 'Gamma Group', 'INACTIVE');

-- Group 1 elements (avg = 250.00)
INSERT INTO group_elements (id, group_id, element_value, category, created_date) VALUES (1, 1, 150.00, 'A', DATE '2024-03-01');
INSERT INTO group_elements (id, group_id, element_value, category, created_date) VALUES (2, 1, 50.00, 'B', DATE '2024-03-05');
INSERT INTO group_elements (id, group_id, element_value, category, created_date) VALUES (3, 1, 600.00, 'A', DATE '2024-03-10');
INSERT INTO group_elements (id, group_id, element_value, category, created_date) VALUES (4, 1, 200.00, 'B', DATE '2024-03-15');

-- Group 2 elements (avg = 225.00)
INSERT INTO group_elements (id, group_id, element_value, category, created_date) VALUES (5, 2, 300.00, 'A', DATE '2024-03-02');
INSERT INTO group_elements (id, group_id, element_value, category, created_date) VALUES (6, 2, 80.00, 'B', DATE '2024-03-06');
INSERT INTO group_elements (id, group_id, element_value, category, created_date) VALUES (7, 2, 400.00, 'A', DATE '2024-03-11');
INSERT INTO group_elements (id, group_id, element_value, category, created_date) VALUES (8, 2, 120.00, 'B', DATE '2024-03-16');

-- Group 1 conditions: GT 100, LTE 500, GTE avg(250)
INSERT INTO group_conditions (id, group_id, condition_code, operator, threshold, aggregate_key)
VALUES (1, 1, 'MIN_VALUE', 'GT', 100.00, NULL);
INSERT INTO group_conditions (id, group_id, condition_code, operator, threshold, aggregate_key)
VALUES (2, 1, 'MAX_VALUE', 'LTE', 500.00, NULL);
INSERT INTO group_conditions (id, group_id, condition_code, operator, threshold, aggregate_key)
VALUES (3, 1, 'ABOVE_AVG', 'GTE', 0.00, 'AVG_VALUE');

-- Group 2 conditions: GTE 100, LT 350, GT avg(225)
INSERT INTO group_conditions (id, group_id, condition_code, operator, threshold, aggregate_key)
VALUES (4, 2, 'FLOOR', 'GTE', 100.00, NULL);
INSERT INTO group_conditions (id, group_id, condition_code, operator, threshold, aggregate_key)
VALUES (5, 2, 'CEILING', 'LT', 350.00, NULL);
INSERT INTO group_conditions (id, group_id, condition_code, operator, threshold, aggregate_key)
VALUES (6, 2, 'ABOVE_AVG', 'GT', 0.00, 'AVG_VALUE');
