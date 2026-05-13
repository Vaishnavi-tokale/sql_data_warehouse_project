USE master;
GO

-- =========================================================
-- SAFETY CONFIRMATION VARIABLE
-- =========================================================
DECLARE @allow_drop BIT = 0;

-- =========================================================
-- SAFETY CHECK
-- Prevent accidental database deletion
-- =========================================================
IF @allow_drop = 0
   AND EXISTS (
       SELECT 1
       FROM sys.databases
       WHERE name = 'DataWarehouse'
   )
BEGIN
    RAISERROR(
        'Safety guard active: DataWarehouse already exists. '
        + 'Set @allow_drop = 1 to confirm DROP DATABASE execution.',
        16,
        1
    );

    RETURN;
END;

-- =========================================================
-- DROP EXISTING DATABASE
-- Only executes when:
-- 1. Database exists
-- 2. @allow_drop = 1
-- =========================================================
IF EXISTS (
    SELECT 1
    FROM sys.databases
    WHERE name = 'DataWarehouse'
)
BEGIN
    PRINT '>> Safety confirmed. Dropping existing DataWarehouse database...';

    ALTER DATABASE DataWarehouse
    SET SINGLE_USER
    WITH ROLLBACK IMMEDIATE;

    DROP DATABASE DataWarehouse;

    PRINT '>> Existing database dropped successfully.';
END;
GO

-- =========================================================
-- CREATE DATABASE
-- =========================================================
PRINT '>> Creating DataWarehouse database...';

CREATE DATABASE DataWarehouse;

PRINT '>> DataWarehouse created successfully.';
GO

USE DataWarehouse;
GO

-- =========================================================
-- CREATE SCHEMAS
-- =========================================================
PRINT '>> Creating schemas...';

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO

PRINT '>> All schemas created successfully.';
GO