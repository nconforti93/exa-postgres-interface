CREATE SCHEMA IF NOT EXISTS pg_demo;

DROP TABLE IF EXISTS pg_demo.orders;

CREATE TABLE pg_demo.orders (
    order_id DECIMAL(18, 0),
    order_ts TIMESTAMP,
    customer_name VARCHAR(200),
    amount DECIMAL(18, 4)
);

INSERT INTO pg_demo.orders (order_id, order_ts, customer_name, amount) VALUES
    (1, TIMESTAMP '2026-01-02 08:15:00', 'Acme GmbH', 125.5000),
    (2, TIMESTAMP '2026-01-03 09:30:00', 'ACME Labs', 210.0000),
    (3, TIMESTAMP '2026-01-04 10:45:00', 'Beta AG', 99.9900),
    (4, TIMESTAMP '2026-01-05 11:00:00', 'acme Services', 42.4200);
