-- PostgreSQL初期化スクリプト
-- データベースとユーザーの作成

-- データベースはcompose.ymlのPOSTGRES_DBで自動作成されます

-- 拡張機能の有効化
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- ユーザーテーブル作成
CREATE TABLE IF NOT EXISTS users (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    age INTEGER NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'active',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- カテゴリテーブル作成
CREATE TABLE IF NOT EXISTS categories (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    parent_id BIGINT REFERENCES categories(id),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- 商品テーブル作成
CREATE TABLE IF NOT EXISTS products (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10,2) NOT NULL,
    stock INTEGER NOT NULL DEFAULT 0,
    category_id BIGINT NOT NULL REFERENCES categories(id),
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- 注文テーブル作成
CREATE TABLE IF NOT EXISTS orders (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id),
    total_amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    order_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    shipped_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- 注文項目テーブル作成
CREATE TABLE IF NOT EXISTS order_items (
    id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_id BIGINT NOT NULL REFERENCES products(id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    subtotal DECIMAL(10,2) NOT NULL
);

-- インデックス作成
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_status ON users(status);
CREATE INDEX IF NOT EXISTS idx_users_created_at ON users(created_at);

CREATE INDEX IF NOT EXISTS idx_categories_name ON categories(name);
CREATE INDEX IF NOT EXISTS idx_categories_parent_id ON categories(parent_id);

CREATE INDEX IF NOT EXISTS idx_products_name ON products(name);
CREATE INDEX IF NOT EXISTS idx_products_category_id ON products(category_id);
CREATE INDEX IF NOT EXISTS idx_products_price ON products(price);
CREATE INDEX IF NOT EXISTS idx_products_stock ON products(stock);

CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_order_date ON orders(order_date);

CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items(product_id);

-- サンプルデータ挿入
INSERT INTO categories (name, description) VALUES
('Electronics', 'Electronic devices and accessories'),
('Books', 'Physical and digital books'),
('Clothing', 'Apparel and fashion items'),
('Home & Garden', 'Home improvement and garden supplies'),
('Sports', 'Sports equipment and accessories')
ON CONFLICT DO NOTHING;

INSERT INTO users (name, email, age, status) VALUES
('Alice Johnson', 'alice@example.com', 28, 'active'),
('Bob Smith', 'bob@example.com', 35, 'active'),
('Carol Brown', 'carol@example.com', 22, 'active'),
('David Wilson', 'david@example.com', 41, 'active'),
('Eve Davis', 'eve@example.com', 33, 'active'),
('Frank Miller', 'frank@example.com', 29, 'active'),
('Grace Lee', 'grace@example.com', 26, 'active'),
('Henry Taylor', 'henry@example.com', 38, 'active'),
('Ivy Chen', 'ivy@example.com', 24, 'active'),
('Jack Anderson', 'jack@example.com', 31, 'active')
ON CONFLICT (email) DO NOTHING;

INSERT INTO products (name, description, price, stock, category_id, metadata) VALUES
('Laptop Computer', 'High-performance laptop for business and gaming', 1299.99, 50, 1, '{"brand": "TechCorp", "warranty": "2 years"}'),
('Smartphone', 'Latest model smartphone with advanced features', 899.99, 100, 1, '{"brand": "PhoneMaker", "storage": "256GB"}'),
('Programming Book', 'Complete guide to modern programming practices', 49.99, 200, 2, '{"author": "John Doe", "pages": 500}'),
('T-Shirt', 'Comfortable cotton t-shirt', 19.99, 300, 3, '{"material": "100% cotton", "sizes": ["S", "M", "L", "XL"]}'),
('Garden Tools Set', 'Complete set of essential garden tools', 89.99, 75, 4, '{"pieces": 10, "material": "stainless steel"}'),
('Tennis Racket', 'Professional tennis racket for competitive play', 159.99, 40, 5, '{"weight": "300g", "string": "included"}'),
('Wireless Headphones', 'Noise-canceling wireless headphones', 249.99, 80, 1, '{"battery": "30 hours", "noise_canceling": true}'),
('Cookbook', 'International cuisine cookbook with 200 recipes', 29.99, 150, 2, '{"author": "Chef Maria", "cuisine": "international"}'),
('Running Shoes', 'High-performance running shoes', 129.99, 120, 5, '{"brand": "RunFast", "type": "road running"}'),
('Office Chair', 'Ergonomic office chair with lumbar support', 399.99, 25, 4, '{"adjustable": true, "material": "mesh"}}')
ON CONFLICT DO NOTHING;

-- 統計情報更新
ANALYZE users;
ANALYZE categories;
ANALYZE products;
ANALYZE orders;
ANALYZE order_items;

-- パフォーマンス監視用ビュー作成
CREATE OR REPLACE VIEW db_performance_stats AS
SELECT 
    schemaname,
    tablename,
    attname,
    n_distinct,
    correlation
FROM pg_stats 
WHERE schemaname = 'public';

-- 接続監視用ビュー作成
CREATE OR REPLACE VIEW db_connections AS
SELECT 
    datname,
    state,
    COUNT(*) as connection_count
FROM pg_stat_activity 
WHERE datname = 'testdb'
GROUP BY datname, state;

-- ユーザー権限設定
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;

-- PostgreSQL特有の最適化設定
-- WAL設定
ALTER SYSTEM SET wal_level = replica;
ALTER SYSTEM SET max_wal_senders = 3;
ALTER SYSTEM SET wal_keep_segments = 32;

-- パフォーマンス調整
ALTER SYSTEM SET shared_buffers = '256MB';
ALTER SYSTEM SET effective_cache_size = '1GB';
ALTER SYSTEM SET maintenance_work_mem = '64MB';
ALTER SYSTEM SET checkpoint_completion_target = 0.9;
ALTER SYSTEM SET wal_buffers = '16MB';
ALTER SYSTEM SET default_statistics_target = 100;

SELECT pg_reload_conf();
