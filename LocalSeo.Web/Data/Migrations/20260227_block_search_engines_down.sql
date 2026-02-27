IF OBJECT_ID('dbo.AppSettings', 'U') IS NULL
BEGIN
    RETURN;
END;

IF COL_LENGTH('dbo.AppSettings', 'BlockSearchEngines') IS NOT NULL
BEGIN
    DECLARE @ConstraintName sysname;
    DECLARE @DropSql nvarchar(max);

    SELECT @ConstraintName = dc.name
    FROM sys.default_constraints dc
    INNER JOIN sys.columns c
        ON c.default_object_id = dc.object_id
    WHERE dc.parent_object_id = OBJECT_ID('dbo.AppSettings')
      AND c.name = 'BlockSearchEngines';

    IF @ConstraintName IS NOT NULL
    BEGIN
        SET @DropSql = N'ALTER TABLE dbo.AppSettings DROP CONSTRAINT ' + QUOTENAME(@ConstraintName) + N';';
        EXEC sp_executesql @DropSql;
    END;

    ALTER TABLE dbo.AppSettings DROP COLUMN BlockSearchEngines;
END;
