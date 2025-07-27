-- MSSQL初期化スクリプト
-- データベース作成
IF NOT EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE name = N'testdb')
BEGIN
    CREATE DATABASE testdb;
END
GO

USE testdb;
GO

-- ユーザーテーブル作成
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='users' AND xtype='U')
BEGIN
    CREATE TABLE users (
        id BIGINT IDENTITY(1,1) PRIMARY KEY,
        name NVARCHAR(255) NOT NULL,
        email NVARCHAR(255) UNIQUE NOT NULL,
        age INT,
        created_at DATETIME2 DEFAULT GETDATE(),
        updated_at DATETIME2 DEFAULT GETDATE()
    );

    -- インデックス作成
    CREATE INDEX idx_users_email ON users(email);
    CREATE INDEX idx_users_name ON users(name);
    CREATE INDEX idx_users_created_at ON users(created_at);

    -- テストデータ挿入
    DECLARE @i INT = 1;
    WHILE @i <= 10000
    BEGIN
        INSERT INTO users (name, email, age) 
        VALUES (
            'User' + CAST(@i AS NVARCHAR(10)),
            'user' + CAST(@i AS NVARCHAR(10)) + '@example.com',
            20 + (@i % 50)
        );
        SET @i = @i + 1;
    END
END
GO

-- パフォーマンステスト用のストアドプロシージャ
CREATE OR ALTER PROCEDURE GetUsersByAgeRange
    @MinAge INT,
    @MaxAge INT
AS
BEGIN
    SET NOCOUNT ON;
    SELECT id, name, email, age, created_at, updated_at
    FROM users 
    WHERE age BETWEEN @MinAge AND @MaxAge
    ORDER BY created_at DESC;
END
GO

-- バッチ挿入用のストアドプロシージャ
CREATE OR ALTER PROCEDURE BulkInsertUsers
    @UserData NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    
    -- JSON形式のユーザーデータを処理
    INSERT INTO users (name, email, age)
    SELECT name, email, age
    FROM OPENJSON(@UserData)
    WITH (
        name NVARCHAR(255),
        email NVARCHAR(255),
        age INT
    );
END
GO

PRINT 'MSSQL初期化完了';
