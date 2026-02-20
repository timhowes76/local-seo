SET NOCOUNT ON;

IF OBJECT_ID('dbo.EmailCodes','U') IS NOT NULL
  DROP TABLE dbo.EmailCodes;

IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_User_EmailAddressNormalized' AND object_id = OBJECT_ID('dbo.[User]'))
  DROP INDEX UX_User_EmailAddressNormalized ON dbo.[User];

IF OBJECT_ID('dbo.[User]','U') IS NOT NULL
  DROP TABLE dbo.[User];

IF OBJECT_ID('dbo.LoginCode','U') IS NULL
BEGIN
  CREATE TABLE dbo.LoginCode(
    LoginCodeId bigint IDENTITY(1,1) PRIMARY KEY,
    Email nvarchar(320) NOT NULL,
    CodeHash varbinary(64) NOT NULL,
    Salt varbinary(32) NOT NULL,
    ExpiresAtUtc datetime2(0) NOT NULL,
    CreatedAtUtc datetime2(0) NOT NULL CONSTRAINT DF_LoginCode_CreatedAtUtc DEFAULT SYSUTCDATETIME(),
    FailedAttempts int NOT NULL CONSTRAINT DF_LoginCode_FailedAttempts DEFAULT(0),
    IsUsed bit NOT NULL CONSTRAINT DF_LoginCode_IsUsed DEFAULT(0)
  );
END;

IF OBJECT_ID('dbo.LoginThrottle','U') IS NULL
BEGIN
  CREATE TABLE dbo.LoginThrottle(
    Email nvarchar(320) NOT NULL PRIMARY KEY,
    WindowStartUtc datetime2(0) NOT NULL,
    SendCount int NOT NULL
  );
END;
