CREATE TABLE IF NOT EXISTS products (
    id TEXT PRIMARY KEY CHECK (char_length(id) BETWEEN 1 AND 64),
    name TEXT NOT NULL CHECK (char_length(name) BETWEEN 1 AND 200),
    category TEXT NOT NULL CHECK (char_length(category) BETWEEN 1 AND 64),
    price BIGINT NOT NULL CHECK (price >= 0),
    stock BIGINT NOT NULL CHECK (stock >= 0)
);
CREATE INDEX IF NOT EXISTS products_category_price_id_idx
    ON products (category, price, id COLLATE "C");
CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY CHECK (length(order_id) > 0),
    user_id TEXT NOT NULL CHECK (char_length(user_id) BETWEEN 1 AND 64),
    product_id TEXT NOT NULL REFERENCES products(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, product_id)
);
CREATE INDEX IF NOT EXISTS orders_user_idx ON orders (user_id) INCLUDE (order_id, product_id);
