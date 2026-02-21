SET NOCOUNT ON;

IF COL_LENGTH('dbo.[User]', 'UseGravatar') IS NULL
BEGIN
  ALTER TABLE dbo.[User]
    ADD UseGravatar bit NOT NULL CONSTRAINT DF_User_UseGravatar DEFAULT(0);
END;
