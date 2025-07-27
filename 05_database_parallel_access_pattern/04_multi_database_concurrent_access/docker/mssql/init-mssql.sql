-- MSSQL Server 初期化スクリプト
-- データベースとテーブルの作成

-- データベース作成
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'testdb')
BEGIN
    CREATE DATABASE testdb
    ON (
        NAME = 'testdb_data',
        FILENAME = '/var/opt/mssql/data/testdb.mdf',
        SIZE = 100MB,
        MAXSIZE = 10GB,
        FILEGROWTH = 10MB
    )
    LOG ON (
        NAME = 'testdb_log',
        FILENAME = '/var/opt/mssql/data/testdb.ldf',
        SIZE = 10MB,
        MAXSIZE = 1GB,
        FILEGROWTH = 10%
    );
    
    -- データベース設定最適化
    ALTER DATABASE testdb SET RECOVERY SIMPLE;
    ALTER DATABASE testdb SET AUTO_CLOSE OFF;
    ALTER DATABASE testdb SET AUTO_SHRINK OFF;
    ALTER DATABASE testdb SET AUTO_CREATE_STATISTICS ON;
    ALTER DATABASE testdb SET AUTO_UPDATE_STATISTICS ON;
    ALTER DATABASE testdb SET AUTO_UPDATE_STATISTICS_ASYNC ON;
    ALTER DATABASE testdb SET PARAMETERIZATION SIMPLE;
    ALTER DATABASE testdb SET READ_COMMITTED_SNAPSHOT ON;
    
    PRINT 'Database testdb created successfully.';
END
ELSE
BEGIN
    PRINT 'Database testdb already exists.';
END
GO

-- データベース使用
USE testdb;
GO

-- ユーザーテーブル作成
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[users]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[users] (
        [id] BIGINT IDENTITY(1,1) PRIMARY KEY,
        [name] NVARCHAR(255) NOT NULL,
        [email] NVARCHAR(255) NOT NULL UNIQUE,
        [age] INT NOT NULL,
        [status] NVARCHAR(50) NOT NULL DEFAULT 'active',
        [created_at] DATETIME2(7) NOT NULL DEFAULT GETUTCDATE(),
        [updated_at] DATETIME2(7) NOT NULL DEFAULT GETUTCDATE()
    );
    
    -- インデックス作成
    CREATE NONCLUSTERED INDEX [IX_users_email] ON [dbo].[users] ([email]);
    CREATE NONCLUSTERED INDEX [IX_users_status] ON [dbo].[users] ([status]);
    CREATE NONCLUSTERED INDEX [IX_users_created_at] ON [dbo].[users] ([created_at]);
    
    PRINT 'Table users created successfully.';
END
GO

-- カテゴリテーブル作成
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[categories]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[categories] (
        [id] BIGINT IDENTITY(1,1) PRIMARY KEY,
        [name] NVARCHAR(255) NOT NULL,
        [description] NVARCHAR(MAX),
        [parent_id] BIGINT NULL,
        [created_at] DATETIME2(7) NOT NULL DEFAULT GETUTCDATE(),
        [updated_at] DATETIME2(7) NOT NULL DEFAULT GETUTCDATE(),
        FOREIGN KEY ([parent_id]) REFERENCES [dbo].[categories]([id])
    );
    
    -- インデックス作成
    CREATE NONCLUSTERED INDEX [IX_categories_name] ON [dbo].[categories] ([name]);
    CREATE NONCLUSTERED INDEX [IX_categories_parent_id] ON [dbo].[categories] ([parent_id]);
    
    PRINT 'Table categories created successfully.';
END
GO

-- 商品テーブル作成
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[products]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[products] (
        [id] BIGINT IDENTITY(1,1) PRIMARY KEY,
        [name] NVARCHAR(255) NOT NULL,
        [description] NVARCHAR(MAX),
        [price] DECIMAL(10,2) NOT NULL,
        [stock] INT NOT NULL DEFAULT 0,
        [category_id] BIGINT NOT NULL,
        [metadata] NVARCHAR(MAX) CHECK (ISJSON([metadata]) = 1),
        [created_at] DATETIME2(7) NOT NULL DEFAULT GETUTCDATE(),
        [updated_at] DATETIME2(7) NOT NULL DEFAULT GETUTCDATE(),
        FOREIGN KEY ([category_id]) REFERENCES [dbo].[categories]([id])
    );
    
    -- インデックス作成
    CREATE NONCLUSTERED INDEX [IX_products_name] ON [dbo].[products] ([name]);
    CREATE NONCLUSTERED INDEX [IX_products_category_id] ON [dbo].[products] ([category_id]);
    CREATE NONCLUSTERED INDEX [IX_products_price] ON [dbo].[products] ([price]);
    CREATE NONCLUSTERED INDEX [IX_products_stock] ON [dbo].[products] ([stock]);
    
    PRINT 'Table products created successfully.';
END
GO

-- 注文テーブル作成
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[orders]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[orders] (
        [id] BIGINT IDENTITY(1,1) PRIMARY KEY,
        [user_id] BIGINT NOT NULL,
        [total_amount] DECIMAL(10,2) NOT NULL,
        [status] NVARCHAR(50) NOT NULL DEFAULT 'pending',
        [order_date] DATETIME2(7) NOT NULL DEFAULT GETUTCDATE(),
        [shipped_at] DATETIME2(7) NULL,
        [created_at] DATETIME2(7) NOT NULL DEFAULT GETUTCDATE(),
        [updated_at] DATETIME2(7) NOT NULL DEFAULT GETUTCDATE(),
        FOREIGN KEY ([user_id]) REFERENCES [dbo].[users]([id])
    );
    
    -- インデックス作成
    CREATE NONCLUSTERED INDEX [IX_orders_user_id] ON [dbo].[orders] ([user_id]);
    CREATE NONCLUSTERED INDEX [IX_orders_status] ON [dbo].[orders] ([status]);
    CREATE NONCLUSTERED INDEX [IX_orders_order_date] ON [dbo].[orders] ([order_date]);
    
    PRINT 'Table orders created successfully.';
END
GO

-- 注文項目テーブル作成
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[order_items]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[order_items] (
        [id] BIGINT IDENTITY(1,1) PRIMARY KEY,
        [order_id] BIGINT NOT NULL,
        [product_id] BIGINT NOT NULL,
        [quantity] INT NOT NULL,
        [unit_price] DECIMAL(10,2) NOT NULL,
        [subtotal] DECIMAL(10,2) NOT NULL,
        FOREIGN KEY ([order_id]) REFERENCES [dbo].[orders]([id]) ON DELETE CASCADE,
        FOREIGN KEY ([product_id]) REFERENCES [dbo].[products]([id])
    );
    
    -- インデックス作成
    CREATE NONCLUSTERED INDEX [IX_order_items_order_id] ON [dbo].[order_items] ([order_id]);
    CREATE NONCLUSTERED INDEX [IX_order_items_product_id] ON [dbo].[order_items] ([product_id]);
    
    PRINT 'Table order_items created successfully.';
END
GO

-- サンプルデータ挿入
-- カテゴリデータ
IF NOT EXISTS (SELECT 1 FROM [dbo].[categories])
BEGIN
    SET IDENTITY_INSERT [dbo].[categories] ON;
    
    INSERT INTO [dbo].[categories] ([id], [name], [description]) VALUES
    (1, 'Electronics', 'Electronic devices and accessories'),
    (2, 'Books', 'Physical and digital books'),
    (3, 'Clothing', 'Apparel and fashion items'),
    (4, 'Home & Garden', 'Home improvement and garden supplies'),
    (5, 'Sports', 'Sports equipment and accessories');
    
    SET IDENTITY_INSERT [dbo].[categories] OFF;
    
    PRINT 'Sample categories inserted.';
END
GO

-- ユーザーデータ
IF NOT EXISTS (SELECT 1 FROM [dbo].[users])
BEGIN
    SET IDENTITY_INSERT [dbo].[users] ON;
    
    INSERT INTO [dbo].[users] ([id], [name], [email], [age], [status]) VALUES
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
    
    SET IDENTITY_INSERT [dbo].[users] OFF;
    
    PRINT 'Sample users inserted.';
END
GO

-- 商品データ
IF NOT EXISTS (SELECT 1 FROM [dbo].[products])
BEGIN
    SET IDENTITY_INSERT [dbo].[products] ON;
    
    INSERT INTO [dbo].[products] ([id], [name], [description], [price], [stock], [category_id], [metadata]) VALUES
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
    
    SET IDENTITY_INSERT [dbo].[products] OFF;
    
    PRINT 'Sample products inserted.';
END
GO

-- トリガー作成（updated_at自動更新用）
-- ユーザーテーブル用
IF NOT EXISTS (SELECT * FROM sys.triggers WHERE name = 'trg_users_updated_at')
BEGIN
    CREATE TRIGGER [dbo].[trg_users_updated_at]
    ON [dbo].[users]
    AFTER UPDATE
    AS
    BEGIN
        SET NOCOUNT ON;
        UPDATE [dbo].[users]
        SET [updated_at] = GETUTCDATE()
        FROM [dbo].[users] u
        INNER JOIN inserted i ON u.[id] = i.[id];
    END
    
    PRINT 'Trigger trg_users_updated_at created.';
END
GO

-- カテゴリテーブル用
IF NOT EXISTS (SELECT * FROM sys.triggers WHERE name = 'trg_categories_updated_at')
BEGIN
    CREATE TRIGGER [dbo].[trg_categories_updated_at]
    ON [dbo].[categories]
    AFTER UPDATE
    AS
    BEGIN
        SET NOCOUNT ON;
        UPDATE [dbo].[categories]
        SET [updated_at] = GETUTCDATE()
        FROM [dbo].[categories] c
        INNER JOIN inserted i ON c.[id] = i.[id];
    END
    
    PRINT 'Trigger trg_categories_updated_at created.';
END
GO

-- 商品テーブル用
IF NOT EXISTS (SELECT * FROM sys.triggers WHERE name = 'trg_products_updated_at')
BEGIN
    CREATE TRIGGER [dbo].[trg_products_updated_at]
    ON [dbo].[products]
    AFTER UPDATE
    AS
    BEGIN
        SET NOCOUNT ON;
        UPDATE [dbo].[products]
        SET [updated_at] = GETUTCDATE()
        FROM [dbo].[products] p
        INNER JOIN inserted i ON p.[id] = i.[id];
    END
    
    PRINT 'Trigger trg_products_updated_at created.';
END
GO

-- 注文テーブル用
IF NOT EXISTS (SELECT * FROM sys.triggers WHERE name = 'trg_orders_updated_at')
BEGIN
    CREATE TRIGGER [dbo].[trg_orders_updated_at]
    ON [dbo].[orders]
    AFTER UPDATE
    AS
    BEGIN
        SET NOCOUNT ON;
        UPDATE [dbo].[orders]
        SET [updated_at] = GETUTCDATE()
        FROM [dbo].[orders] o
        INNER JOIN inserted i ON o.[id] = i.[id];
    END
    
    PRINT 'Trigger trg_orders_updated_at created.';
END
GO

-- ストアドプロシージャ作成（一括処理用）
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[sp_BulkInsertUsers]') AND type in (N'P', N'PC'))
    DROP PROCEDURE [dbo].[sp_BulkInsertUsers];
GO

CREATE PROCEDURE [dbo].[sp_BulkInsertUsers]
    @UserJSON NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        INSERT INTO [dbo].[users] ([name], [email], [age], [status])
        SELECT 
            JSON_VALUE(value, '$.name') as [name],
            JSON_VALUE(value, '$.email') as [email],
            CAST(JSON_VALUE(value, '$.age') AS INT) as [age],
            ISNULL(JSON_VALUE(value, '$.status'), 'active') as [status]
        FROM OPENJSON(@UserJSON);
        
        COMMIT TRANSACTION;
        
        SELECT 'Success' as [Status], @@ROWCOUNT as [RowsAffected];
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        
        SELECT 'Error' as [Status], ERROR_MESSAGE() as [ErrorMessage];
    END CATCH
END
GO

-- パフォーマンス監視用ビュー作成
IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_table_statistics')
    DROP VIEW [dbo].[vw_table_statistics];
GO

CREATE VIEW [dbo].[vw_table_statistics]
AS
SELECT 
    SCHEMA_NAME(t.schema_id) AS [schema_name],
    t.name AS [table_name],
    p.rows AS [row_count],
    CAST(ROUND(((SUM(a.used_pages) * 8) / 1024.00), 2) AS DECIMAL(18,2)) AS [used_space_mb],
    CAST(ROUND(((SUM(a.total_pages) - SUM(a.used_pages)) * 8) / 1024.00, 2) AS DECIMAL(18,2)) AS [unused_space_mb]
FROM sys.tables t
INNER JOIN sys.indexes i ON t.object_id = i.object_id
INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
WHERE t.name NOT LIKE 'dt%' AND t.is_ms_shipped = 0 AND i.object_id > 255
GROUP BY t.schema_id, t.name, p.rows;
GO

-- インデックス使用統計ビュー
IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_index_usage_stats')
    DROP VIEW [dbo].[vw_index_usage_stats];
GO

CREATE VIEW [dbo].[vw_index_usage_stats]
AS
SELECT 
    SCHEMA_NAME(t.schema_id) AS [schema_name],
    t.name AS [table_name],
    i.name AS [index_name],
    s.user_seeks,
    s.user_scans,
    s.user_lookups,
    s.user_updates,
    s.last_user_seek,
    s.last_user_scan,
    s.last_user_lookup,
    s.last_user_update
FROM sys.dm_db_index_usage_stats s
INNER JOIN sys.indexes i ON s.object_id = i.object_id AND s.index_id = i.index_id
INNER JOIN sys.tables t ON i.object_id = t.object_id
WHERE s.database_id = DB_ID() AND t.is_ms_shipped = 0;
GO

-- 統計情報更新
UPDATE STATISTICS [dbo].[users];
UPDATE STATISTICS [dbo].[categories];
UPDATE STATISTICS [dbo].[products];
UPDATE STATISTICS [dbo].[orders];
UPDATE STATISTICS [dbo].[order_items];

PRINT 'MSSQL Server initialization completed successfully!';
