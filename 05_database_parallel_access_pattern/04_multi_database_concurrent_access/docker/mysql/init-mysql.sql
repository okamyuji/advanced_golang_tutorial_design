-- MySQL初期化スクリプト
-- データベースとテーブルの作成

-- データベース作成（既に存在する場合はスキップ）
CREATE DATABASE IF NOT EXISTS testdb CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE testdb;

-- ユーザーテーブル作成
CREATE TABLE IF NOT EXISTS users (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    age INT NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'active',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_users_email (email),
    INDEX idx_users_status (status),
    INDEX idx_users_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- カテゴリテーブル作成
CREATE TABLE IF NOT EXISTS categories (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    parent_id BIGINT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_categories_name (name),
    INDEX idx_categories_parent_id (parent_id),
    FOREIGN KEY (parent_id) REFERENCES categories(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 商品テーブル作成
CREATE TABLE IF NOT EXISTS products (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10,2) NOT NULL,
    stock INT NOT NULL DEFAULT 0,
    category_id BIGINT NOT NULL,
    metadata JSON,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_products_name (name),
    INDEX idx_products_category_id (category_id),
    INDEX idx_products_price (price),
    INDEX idx_products_stock (stock),
    FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 注文テーブル作成
CREATE TABLE IF NOT EXISTS orders (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    order_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    shipped_at TIMESTAMP NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_orders_user_id (user_id),
    INDEX idx_orders_status (status),
    INDEX idx_orders_order_date (order_date),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 注文項目テーブル作成
CREATE TABLE IF NOT EXISTS order_items (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    order_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    subtotal DECIMAL(10,2) NOT NULL,
    INDEX idx_order_items_order_id (order_id),
    INDEX idx_order_items_product_id (product_id),
    FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- サンプルデータ挿入
INSERT IGNORE INTO categories (id, name, description) VALUES
(1, 'Electronics', 'Electronic devices and accessories'),
(2, 'Books', 'Physical and digital books'),
(3, 'Clothing', 'Apparel and fashion items'),
(4, 'Home & Garden', 'Home improvement and garden supplies'),
(5, 'Sports', 'Sports equipment and accessories');

INSERT IGNORE INTO users (id, name, email, age, status) VALUES
(1, 'Alice Johnson', 'alice@example.com', 28, 'active'),
(2, 'Bob Smith', 'bob@example.com', 35, 'active'),
(3, 'Carol Brown', 'carol@example.com', 22, 'active'),
(4, 'David Wilson', 'david@example.com', 41, 'active'),
(5, 'Eve Davis', 'eve@example.com', 33, 'active'),
(6, 'Frank Miller', 'frank@example.com', 29, 'active'),
(7, 'Grace Lee', 'grace@example.com', 26, 'active'),
(8, 'Henry Taylor', 'henry@example.com', 38, 'active'),
(9, 'Ivy Chen', 'ivy@example.com', 24, 'active'),
(10, 'Jack Anderson', 'jack@example.com', 31, 'active');

INSERT IGNORE INTO products (id, name, description, price, stock, category_id, metadata) VALUES
(1, 'Laptop Computer', 'High-performance laptop for business and gaming', 1299.99, 50, 1, '{"brand": "TechCorp", "warranty": "2 years"}'),
(2, 'Smartphone', 'Latest model smartphone with advanced features', 899.99, 100, 1, '{"brand": "PhoneMaker", "storage": "256GB"}'),
(3, 'Programming Book', 'Complete guide to modern programming practices', 49.99, 200, 2, '{"author": "John Doe", "pages": 500}'),
(4, 'T-Shirt', 'Comfortable cotton t-shirt', 19.99, 300, 3, '{"material": "100% cotton", "sizes": ["S", "M", "L", "XL"]}'),
(5, 'Garden Tools Set', 'Complete set of essential garden tools', 89.99, 75, 4, '{"pieces": 10, "material": "stainless steel"}'),
(6, 'Tennis Racket', 'Professional tennis racket for competitive play', 159.99, 40, 5, '{"weight": "300g", "string": "included"}'),
(7, 'Wireless Headphones', 'Noise-canceling wireless headphones', 249.99, 80, 1, '{"battery": "30 hours", "noise_canceling": true}'),
(8, 'Cookbook', 'International cuisine cookbook with 200 recipes', 29.99, 150, 2, '{"author": "Chef Maria", "cuisine": "international"}'),
(9, 'Running Shoes', 'High-performance running shoes', 129.99, 120, 5, '{"brand": "RunFast", "type": "road running"}'),
(10, 'Office Chair', 'Ergonomic office chair with lumbar support', 399.99, 25, 4, '{"adjustable": true, "material": "mesh"}');

-- パフォーマンス最適化用設定
-- インデックス最適化（統計情報更新）
ANALYZE TABLE users, categories, products, orders, order_items;

-- MySQL固有の最適化設定
SET GLOBAL innodb_adaptive_hash_index = ON;
SET GLOBAL innodb_adaptive_flushing = ON;
SET GLOBAL innodb_change_buffering = all;

-- パフォーマンス監視用ビューの作成
-- テーブル統計情報ビュー
CREATE OR REPLACE VIEW table_statistics AS
SELECT 
    TABLE_SCHEMA,
    TABLE_NAME,
    TABLE_ROWS,
    DATA_LENGTH,
    INDEX_LENGTH,
    DATA_FREE,
    ENGINE,
    CREATE_TIME,
    UPDATE_TIME
FROM information_schema.TABLES 
WHERE TABLE_SCHEMA = 'testdb';

-- インデックス使用状況ビュー
CREATE OR REPLACE VIEW index_statistics AS
SELECT 
    OBJECT_SCHEMA,
    OBJECT_NAME,
    INDEX_NAME,
    COUNT_FETCH,
    COUNT_INSERT,
    COUNT_UPDATE,
    COUNT_DELETE
FROM performance_schema.table_io_waits_summary_by_index_usage
WHERE OBJECT_SCHEMA = 'testdb';

-- 接続統計ビュー
CREATE OR REPLACE VIEW connection_statistics AS
SELECT 
    USER,
    HOST,
    DB,
    COMMAND,
    TIME,
    STATE,
    INFO
FROM information_schema.PROCESSLIST
WHERE DB = 'testdb';

-- MySQL 8.0 特有の機能設定
-- JSON関数の最適化
SET optimizer_switch = 'derived_merge=on';

-- パーティション最適化（将来の拡張用）
-- ALTER TABLE orders PARTITION BY RANGE (YEAR(order_date)) (
--     PARTITION p2023 VALUES LESS THAN (2024),
--     PARTITION p2024 VALUES LESS THAN (2025),
--     PARTITION p_future VALUES LESS THAN MAXVALUE
-- );

-- クエリキャッシュの代替（MySQL 8.0では削除されたため、プリペアドステートメントを推奨）
-- 一般的なクエリをプリペアしておく
PREPARE get_user_by_id FROM 'SELECT * FROM users WHERE id = ?';
PREPARE get_products_by_category FROM 'SELECT * FROM products WHERE category_id = ? LIMIT ?';
PREPARE get_user_orders FROM 'SELECT * FROM orders WHERE user_id = ? ORDER BY order_date DESC LIMIT ?';

-- トリガーの作成（updated_at自動更新用）
DELIMITER $$

CREATE TRIGGER users_updated_at_trigger
    BEFORE UPDATE ON users
    FOR EACH ROW
BEGIN
    SET NEW.updated_at = CURRENT_TIMESTAMP;
END$$

CREATE TRIGGER categories_updated_at_trigger
    BEFORE UPDATE ON categories
    FOR EACH ROW
BEGIN
    SET NEW.updated_at = CURRENT_TIMESTAMP;
END$$

CREATE TRIGGER products_updated_at_trigger
    BEFORE UPDATE ON products
    FOR EACH ROW
BEGIN
    SET NEW.updated_at = CURRENT_TIMESTAMP;
END$$

CREATE TRIGGER orders_updated_at_trigger
    BEFORE UPDATE ON orders
    FOR EACH ROW
BEGIN
    SET NEW.updated_at = CURRENT_TIMESTAMP;
END$$

DELIMITER ;

-- ストアドプロシージャの作成（高性能バッチ処理用）
DELIMITER $$

CREATE PROCEDURE BatchInsertUsers(
    IN user_data JSON
)
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE user_name VARCHAR(255);
    DECLARE user_email VARCHAR(255);
    DECLARE user_age INT;
    DECLARE user_status VARCHAR(50);
    DECLARE i INT DEFAULT 0;
    DECLARE user_count INT;
    
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;
    
    SET user_count = JSON_LENGTH(user_data);
    
    START TRANSACTION;
    
    WHILE i < user_count DO
        SET user_name = JSON_UNQUOTE(JSON_EXTRACT(user_data, CONCAT('$[', i, '].name')));
        SET user_email = JSON_UNQUOTE(JSON_EXTRACT(user_data, CONCAT('$[', i, '].email')));
        SET user_age = JSON_EXTRACT(user_data, CONCAT('$[', i, '].age'));
        SET user_status = JSON_UNQUOTE(JSON_EXTRACT(user_data, CONCAT('$[', i, '].status')));
        
        INSERT INTO users (name, email, age, status) 
        VALUES (user_name, user_email, user_age, IFNULL(user_status, 'active'));
        
        SET i = i + 1;
    END WHILE;
    
    COMMIT;
END$$

DELIMITER ;

-- 権限設定
CREATE USER IF NOT EXISTS 'appuser'@'%' IDENTIFIED BY 'apppass';
GRANT SELECT, INSERT, UPDATE, DELETE ON testdb.* TO 'appuser'@'%';
GRANT EXECUTE ON testdb.* TO 'appuser'@'%';
FLUSH PRIVILEGES;

-- 統計情報の更新
ANALYZE TABLE users, categories, products, orders, order_items;

-- MySQL特有の最適化コマンド
OPTIMIZE TABLE users, categories, products, orders, order_items;
