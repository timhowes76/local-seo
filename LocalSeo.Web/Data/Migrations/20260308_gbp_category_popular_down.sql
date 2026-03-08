IF EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_GoogleBusinessProfileCategory_Status_Region_Language_Popular_DisplayName'
      AND object_id = OBJECT_ID('dbo.GoogleBusinessProfileCategory')
)
BEGIN
    DROP INDEX IX_GoogleBusinessProfileCategory_Status_Region_Language_Popular_DisplayName
        ON dbo.GoogleBusinessProfileCategory;
END;

IF COL_LENGTH('dbo.GoogleBusinessProfileCategory', 'Popular') IS NOT NULL
BEGIN
    DECLARE @PopularDefaultConstraintName sysname;

    SELECT @PopularDefaultConstraintName = dc.name
    FROM sys.default_constraints dc
    JOIN sys.columns c
        ON c.default_object_id = dc.object_id
    WHERE dc.parent_object_id = OBJECT_ID('dbo.GoogleBusinessProfileCategory')
      AND c.name = 'Popular';

    IF @PopularDefaultConstraintName IS NOT NULL
        EXEC(N'ALTER TABLE dbo.GoogleBusinessProfileCategory DROP CONSTRAINT ' + QUOTENAME(@PopularDefaultConstraintName) + N';');

    ALTER TABLE dbo.GoogleBusinessProfileCategory
        DROP COLUMN Popular;
END;
