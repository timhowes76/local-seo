IF COL_LENGTH('dbo.[User]', 'IsDarkMode') IS NULL
BEGIN
  ALTER TABLE dbo.[User]
    ADD IsDarkMode bit NOT NULL CONSTRAINT DF_User_IsDarkMode DEFAULT (0);
END;
