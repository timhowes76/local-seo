IF COL_LENGTH('dbo.GoogleBusinessProfileCategory', 'LastSeenCycleId') IS NULL
BEGIN
  ALTER TABLE dbo.GoogleBusinessProfileCategory
    ADD LastSeenCycleId uniqueidentifier NULL;
END;

IF NOT EXISTS (
  SELECT 1
  FROM sys.indexes
  WHERE name = 'IX_GoogleBusinessProfileCategory_Region_Language_Cycle'
    AND object_id = OBJECT_ID('dbo.GoogleBusinessProfileCategory')
)
BEGIN
  CREATE INDEX IX_GoogleBusinessProfileCategory_Region_Language_Cycle
    ON dbo.GoogleBusinessProfileCategory(RegionCode, LanguageCode, LastSeenCycleId, CategoryId);
END;

IF OBJECT_ID('dbo.GoogleBusinessProfileCategorySyncCursor', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.GoogleBusinessProfileCategorySyncCursor(
    RegionCode nvarchar(10) NOT NULL,
    LanguageCode nvarchar(20) NOT NULL,
    CycleId uniqueidentifier NOT NULL,
    NextPageToken nvarchar(2048) NULL,
    UpdatedUtc datetime2(0) NOT NULL
      CONSTRAINT DF_GoogleBusinessProfileCategorySyncCursor_UpdatedUtc DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_GoogleBusinessProfileCategorySyncCursor PRIMARY KEY(RegionCode, LanguageCode)
  );
END;
