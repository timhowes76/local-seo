SET NOCOUNT ON;

IF COL_LENGTH('dbo.[User]', 'UseGravatar') IS NOT NULL
BEGIN
  DECLARE @ConstraintName sysname;
  SELECT @ConstraintName = dc.name
  FROM sys.default_constraints dc
  INNER JOIN sys.columns c
    ON c.object_id = dc.parent_object_id
   AND c.column_id = dc.parent_column_id
  WHERE dc.parent_object_id = OBJECT_ID('dbo.[User]')
    AND c.name = 'UseGravatar';

  IF @ConstraintName IS NOT NULL
    EXEC('ALTER TABLE dbo.[User] DROP CONSTRAINT [' + @ConstraintName + ']');

  ALTER TABLE dbo.[User] DROP COLUMN UseGravatar;
END;
