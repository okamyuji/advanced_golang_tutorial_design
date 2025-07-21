-- Sample Data for Chapter 6 Database Parallel Access Patterns
-- 第6章用のサンプルデータ投入スクリプト

-- 既存データのクリア（テスト環境用）
TRUNCATE TABLE order_items, payments, orders, inventory, products, batch_history, products_archive RESTART IDENTITY CASCADE;

-- 商品データの投入
INSERT INTO products (name, price, category_id) VALUES
-- 電子機器カテゴリ (category_id: 1)
('iPhone 15 Pro', 149800.00, 1),
('MacBook Air M2', 164800.00, 1),
('iPad Pro 12.9', 159800.00, 1),
('AirPods Pro', 39800.00, 1),
('Apple Watch Series 9', 59800.00, 1),
('Samsung Galaxy S24', 139800.00, 1),
('Sony WH-1000XM5', 49500.00, 1),
('Nintendo Switch', 37980.00, 1),
('PlayStation 5', 66980.00, 1),
('Surface Laptop 5', 158800.00, 1),

-- 書籍カテゴリ (category_id: 2)
('Go言語による並行プログラミング', 3600.00, 2),
('データベース設計の教科書', 2800.00, 2),
('システム設計の面接試験', 3200.00, 2),
('Clean Code', 4200.00, 2),
('プログラマが知るべき97のこと', 2200.00, 2),
('リファクタリング第2版', 4400.00, 2),
('デザインパターン', 5800.00, 2),
('アルゴリズムとデータ構造', 3800.00, 2),
('Effective Go', 3600.00, 2),
('Goで学ぶマイクロサービス', 3400.00, 2),

-- 家具カテゴリ (category_id: 3)
('エルゴノミクスチェア', 89800.00, 3),
('スタンディングデスク', 65000.00, 3),
('本棚 5段', 15800.00, 3),
('デスクライト LED', 8900.00, 3),
('モニターアーム', 12800.00, 3),
('キーボードトレイ', 5800.00, 3),
('足置き台', 3900.00, 3),
('デスクマット', 2800.00, 3),
('収納ボックス', 1980.00, 3),
('プリンタースタンド', 7800.00, 3),

-- 文房具カテゴリ (category_id: 4)
('万年筆 高級', 25000.00, 4),
('ノートブック A4', 800.00, 4),
('ボールペン セット', 1200.00, 4),
('ホワイトボード A3', 4800.00, 4),
('マーカーペン 12色セット', 1800.00, 4),
('付箋セット', 600.00, 4),
('クリップボード', 1200.00, 4),
('ファイルボックス', 980.00, 4),
('電卓 関数', 3200.00, 4),
('ラベルプリンター', 8900.00, 4),

-- 服飾カテゴリ (category_id: 5)
('ビジネススーツ', 89000.00, 5),
('ワイシャツ 白', 6800.00, 5),
('ネクタイ シルク', 8900.00, 5),
('革靴 ビジネス', 32000.00, 5),
('ベルト 革', 12000.00, 5),
('腕時計 アナログ', 158000.00, 5),
('財布 長財布', 18000.00, 5),
('カフス', 15000.00, 5),
('ハンカチ セット', 2400.00, 5),
('靴下 5足セット', 2800.00, 5);

-- 在庫データの投入（複数倉庫）
-- 倉庫1（東京）
INSERT INTO inventory (product_id, quantity, reserved_quantity, warehouse_id) 
SELECT id, 
       CASE 
           WHEN category_id = 1 THEN 50 + (id % 20)  -- 電子機器: 50-70個
           WHEN category_id = 2 THEN 100 + (id % 50) -- 書籍: 100-150個
           WHEN category_id = 3 THEN 20 + (id % 10)  -- 家具: 20-30個
           WHEN category_id = 4 THEN 200 + (id % 100) -- 文房具: 200-300個
           WHEN category_id = 5 THEN 30 + (id % 15)  -- 服飾: 30-45個
       END as quantity,
       CASE 
           WHEN id % 5 = 0 THEN 5  -- 20%の商品で予約済み在庫あり
           ELSE 0
       END as reserved_quantity,
       1 as warehouse_id
FROM products;

-- 倉庫2（大阪）
INSERT INTO inventory (product_id, quantity, reserved_quantity, warehouse_id) 
SELECT id, 
       CASE 
           WHEN category_id = 1 THEN 30 + (id % 15)
           WHEN category_id = 2 THEN 80 + (id % 40)
           WHEN category_id = 3 THEN 15 + (id % 8)
           WHEN category_id = 4 THEN 150 + (id % 80)
           WHEN category_id = 5 THEN 25 + (id % 12)
       END as quantity,
       0 as reserved_quantity,
       2 as warehouse_id
FROM products;

-- 倉庫3（福岡）- 一部商品のみ
INSERT INTO inventory (product_id, quantity, reserved_quantity, warehouse_id) 
SELECT id, 
       CASE 
           WHEN category_id = 1 THEN 20 + (id % 10)
           WHEN category_id = 2 THEN 60 + (id % 30)
           WHEN category_id = 4 THEN 100 + (id % 50)
           ELSE 0
       END as quantity,
       0 as reserved_quantity,
       3 as warehouse_id
FROM products
WHERE category_id IN (1, 2, 4); -- 電子機器、書籍、文房具のみ

-- 注文データの投入（過去30日分のサンプル）
DO $$
DECLARE
    i INTEGER;
    customer_count INTEGER := 1000;
    order_date TIMESTAMP;
    order_num VARCHAR(50);
    prod_id INTEGER;
    prod_price DECIMAL(10,2);
    order_id INTEGER;
    item_quantity INTEGER;
    total_amount DECIMAL(10,2);
BEGIN
    FOR i IN 1..500 LOOP
        -- ランダムな日付（過去30日）
        order_date := CURRENT_TIMESTAMP - INTERVAL '1 day' * (random() * 30);
        
        -- 注文番号生成
        order_num := 'ORD-' || TO_CHAR(order_date, 'YYYYMMDD') || '-' || LPAD(i::TEXT, 6, '0');
        
        -- 注文の作成
        INSERT INTO orders (order_number, customer_id, total_amount, status, created_at, updated_at)
        VALUES (order_num, 
                (random() * customer_count)::INTEGER + 1,
                0, -- 後で更新
                CASE 
                    WHEN random() < 0.8 THEN 'completed'
                    WHEN random() < 0.95 THEN 'shipped'
                    ELSE 'pending'
                END,
                order_date,
                order_date)
        RETURNING id INTO order_id;
        
        total_amount := 0;
        
        -- 注文明細の作成（1-5商品）
        FOR j IN 1..(1 + (random() * 4)::INTEGER) LOOP
            prod_id := (random() * 50)::INTEGER + 1;
            SELECT price INTO prod_price FROM products WHERE id = prod_id;
            item_quantity := (random() * 3)::INTEGER + 1;
            
            INSERT INTO order_items (order_id, product_id, quantity, unit_price, total_price)
            VALUES (order_id, prod_id, item_quantity, prod_price, prod_price * item_quantity);
            
            total_amount := total_amount + (prod_price * item_quantity);
        END LOOP;
        
        -- 注文合計金額の更新
        UPDATE orders SET total_amount = total_amount WHERE id = order_id;
        
        -- 決済データの作成（完了・出荷済み注文のみ）
        IF (SELECT status FROM orders WHERE id = order_id) IN ('completed', 'shipped') THEN
            INSERT INTO payments (order_id, payment_method, amount, status, transaction_id)
            VALUES (order_id,
                    CASE 
                        WHEN random() < 0.4 THEN 'credit_card'
                        WHEN random() < 0.7 THEN 'bank_transfer'
                        WHEN random() < 0.9 THEN 'digital_wallet'
                        ELSE 'convenience_store'
                    END,
                    total_amount,
                    'completed',
                    'TXN-' || order_num || '-' || (random() * 999999)::INTEGER);
        END IF;
    END LOOP;
END $$;

-- バッチ処理履歴のサンプルデータ
INSERT INTO batch_history (batch_type, total_records, processed_records, failed_records, status, started_at, completed_at)
VALUES 
('product_sync', 1000, 1000, 0, 'completed', CURRENT_TIMESTAMP - INTERVAL '2 days', CURRENT_TIMESTAMP - INTERVAL '2 days' + INTERVAL '30 minutes'),
('inventory_update', 5000, 4980, 20, 'completed', CURRENT_TIMESTAMP - INTERVAL '1 day', CURRENT_TIMESTAMP - INTERVAL '1 day' + INTERVAL '1 hour'),
('order_export', 500, 250, 0, 'running', CURRENT_TIMESTAMP - INTERVAL '2 hours', NULL),
('payment_reconciliation', 300, 300, 0, 'completed', CURRENT_TIMESTAMP - INTERVAL '6 hours', CURRENT_TIMESTAMP - INTERVAL '5 hours'),
('data_migration', 10000, 0, 0, 'pending', NULL, NULL);

-- 統計情報の更新
ANALYZE products;
ANALYZE inventory;
ANALYZE orders;
ANALYZE order_items;
ANALYZE payments;
ANALYZE batch_history;

-- サンプルデータの確認用クエリ
SELECT 'products' as table_name, COUNT(*) as record_count FROM products
UNION ALL
SELECT 'inventory', COUNT(*) FROM inventory
UNION ALL
SELECT 'orders', COUNT(*) FROM orders
UNION ALL
SELECT 'order_items', COUNT(*) FROM order_items
UNION ALL
SELECT 'payments', COUNT(*) FROM payments
UNION ALL
SELECT 'batch_history', COUNT(*) FROM batch_history;

-- 在庫サマリーの確認
SELECT 
    category_id,
    COUNT(*) as product_count,
    SUM(total_quantity) as total_inventory,
    SUM(total_reserved) as total_reserved,
    SUM(available_quantity) as total_available
FROM inventory_summary
GROUP BY category_id
ORDER BY category_id;

-- 注文統計の確認
SELECT 
    status,
    COUNT(*) as order_count,
    SUM(total_amount) as total_sales,
    AVG(total_amount) as avg_order_value
FROM orders
GROUP BY status
ORDER BY total_sales DESC;
