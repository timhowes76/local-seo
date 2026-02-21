SET NOCOUNT ON;

IF COL_LENGTH('dbo.[User]', 'SessionVersion') IS NULL
BEGIN
  ALTER TABLE dbo.[User]
    ADD SessionVersion int NOT NULL CONSTRAINT DF_User_SessionVersion DEFAULT(0);
END;

IF OBJECT_ID('dbo.UserOtp', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.UserOtp(
    UserOtpId bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
    UserId int NOT NULL,
    Purpose nvarchar(30) NOT NULL,
    CodeHash varbinary(32) NOT NULL,
    ExpiresAtUtc datetime2(3) NOT NULL,
    UsedAtUtc datetime2(3) NULL,
    SentAtUtc datetime2(3) NOT NULL CONSTRAINT DF_UserOtp_SentAtUtc DEFAULT SYSUTCDATETIME(),
    AttemptCount int NOT NULL CONSTRAINT DF_UserOtp_AttemptCount DEFAULT(0),
    LockedUntilUtc datetime2(3) NULL,
    CorrelationId nvarchar(64) NULL,
    RequestedFromIp varchar(45) NULL
  );
END;

IF NOT EXISTS (
  SELECT 1
  FROM sys.foreign_key_columns fkc
  JOIN sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
  JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
  WHERE fkc.parent_object_id = OBJECT_ID('dbo.UserOtp')
    AND fkc.referenced_object_id = OBJECT_ID('dbo.[User]')
    AND pc.name = 'UserId'
    AND rc.name = 'Id'
)
  ALTER TABLE dbo.UserOtp
    WITH CHECK ADD CONSTRAINT FK_UserOtp_User FOREIGN KEY (UserId) REFERENCES dbo.[User](Id) ON DELETE CASCADE;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_UserOtp_User_Purpose_ExpiresAtUtc' AND object_id = OBJECT_ID('dbo.UserOtp'))
  CREATE INDEX IX_UserOtp_User_Purpose_ExpiresAtUtc ON dbo.UserOtp(UserId, Purpose, ExpiresAtUtc DESC);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_UserOtp_User_Purpose_UsedAtUtc' AND object_id = OBJECT_ID('dbo.UserOtp'))
  CREATE INDEX IX_UserOtp_User_Purpose_UsedAtUtc ON dbo.UserOtp(UserId, Purpose, UsedAtUtc);
