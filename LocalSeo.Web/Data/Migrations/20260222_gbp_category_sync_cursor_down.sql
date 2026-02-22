IF OBJECT_ID('dbo.GoogleBusinessProfileCategorySyncCursor', 'U') IS NOT NULL
BEGIN
  DROP TABLE dbo.GoogleBusinessProfileCategorySyncCursor;
END;

IF EXISTS (
  SELECT 1
  FROM sys.indexes
  WHERE name = 'IX_GoogleBusinessProfileCategory_Region_Language_Cycle'
    AND object_id = OBJECT_ID('dbo.GoogleBusinessProfileCategory')
)
BEGIN
  DROP INDEX IX_GoogleBusinessProfileCategory_Region_Language_Cycle
    ON dbo.GoogleBusinessProfileCategory;
END;

IF COL_LENGTH('dbo.GoogleBusinessProfileCategory', 'LastSeenCycleId') IS NOT NULL
BEGIN
  ALTER TABLE dbo.GoogleBusinessProfileCategory
    DROP COLUMN LastSeenCycleId;
END;
