SET NOCOUNT ON;

IF OBJECT_ID('dbo.UserOtp', 'U') IS NOT NULL
  DROP TABLE dbo.UserOtp;

IF COL_LENGTH('dbo.[User]', 'SessionVersion') IS NOT NULL
BEGIN
  DECLARE @ConstraintName sysname;
  SELECT @ConstraintName = dc.name
  FROM sys.default_constraints dc
  INNER JOIN sys.columns c
    ON c.object_id = dc.parent_object_id
   AND c.column_id = dc.parent_column_id
  WHERE dc.parent_object_id = OBJECT_ID('dbo.[User]')
    AND c.name = 'SessionVersion';

  IF @ConstraintName IS NOT NULL
    EXEC('ALTER TABLE dbo.[User] DROP CONSTRAINT [' + @ConstraintName + ']');

  ALTER TABLE dbo.[User] DROP COLUMN SessionVersion;
END;
