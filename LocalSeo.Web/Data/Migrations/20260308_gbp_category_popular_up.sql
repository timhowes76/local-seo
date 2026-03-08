IF COL_LENGTH('dbo.GoogleBusinessProfileCategory', 'Popular') IS NULL
BEGIN
    ALTER TABLE dbo.GoogleBusinessProfileCategory
        ADD Popular bit NOT NULL
            CONSTRAINT DF_GoogleBusinessProfileCategory_Popular DEFAULT(0);
END;

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_GoogleBusinessProfileCategory_Status_Region_Language_Popular_DisplayName'
      AND object_id = OBJECT_ID('dbo.GoogleBusinessProfileCategory')
)
BEGIN
    CREATE INDEX IX_GoogleBusinessProfileCategory_Status_Region_Language_Popular_DisplayName
        ON dbo.GoogleBusinessProfileCategory(Status, RegionCode, LanguageCode, Popular DESC, DisplayName, CategoryId);
END;
