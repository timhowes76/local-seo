IF OBJECT_ID('dbo.ExternalApiHealth','U') IS NOT NULL
BEGIN
  IF EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_ExternalApiHealth_Name' AND object_id=OBJECT_ID('dbo.ExternalApiHealth'))
    DROP INDEX UX_ExternalApiHealth_Name ON dbo.ExternalApiHealth;

  DROP TABLE dbo.ExternalApiHealth;
END;
