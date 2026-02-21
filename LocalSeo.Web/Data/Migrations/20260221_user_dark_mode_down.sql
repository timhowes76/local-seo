IF COL_LENGTH('dbo.[User]', 'IsDarkMode') IS NOT NULL
BEGIN
  DECLARE @constraintName sysname;

  SELECT @constraintName = dc.name
  FROM sys.default_constraints dc
  INNER JOIN sys.columns c
    ON c.default_object_id = dc.object_id
  WHERE dc.parent_object_id = OBJECT_ID('dbo.[User]')
    AND c.name = 'IsDarkMode';

  IF @constraintName IS NOT NULL
    EXEC('ALTER TABLE dbo.[User] DROP CONSTRAINT ' + QUOTENAME(@constraintName) + ';');

  ALTER TABLE dbo.[User] DROP COLUMN IsDarkMode;
END;
