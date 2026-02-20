SET NOCOUNT ON;

IF OBJECT_ID('dbo.[User]','U') IS NULL
BEGIN
  CREATE TABLE dbo.[User](
    Id int IDENTITY(1,1) NOT NULL PRIMARY KEY,
    FirstName varchar(100) NOT NULL,
    LastName varchar(100) NOT NULL,
    EmailAddress varchar(320) NOT NULL,
    EmailAddressNormalized varchar(320) NOT NULL,
    PasswordHash varbinary(256) NULL,
    Salt varbinary(32) NULL,
    PasswordHashVersion tinyint NOT NULL CONSTRAINT DF_User_PasswordHashVersion DEFAULT(1),
    IsActive bit NOT NULL CONSTRAINT DF_User_IsActive DEFAULT(1),
    IsAdmin bit NOT NULL CONSTRAINT DF_User_IsAdmin DEFAULT(0),
    DateCreatedAtUtc datetime2(0) NOT NULL CONSTRAINT DF_User_DateCreatedAtUtc DEFAULT SYSUTCDATETIME(),
    DatePasswordLastSetUtc datetime2(0) NULL,
    LastLoginAtUtc datetime2(0) NULL,
    FailedPasswordAttempts int NOT NULL CONSTRAINT DF_User_FailedPasswordAttempts DEFAULT(0),
    LockedoutUntilUtc datetime2(0) NULL
  );
END;

IF COL_LENGTH('dbo.[User]', 'IsAdmin') IS NULL
  ALTER TABLE dbo.[User] ADD IsAdmin bit NOT NULL CONSTRAINT DF_User_IsAdmin_Alt DEFAULT(0);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_User_EmailAddressNormalized' AND object_id = OBJECT_ID('dbo.[User]'))
  CREATE UNIQUE INDEX UX_User_EmailAddressNormalized ON dbo.[User](EmailAddressNormalized);

IF NOT EXISTS (SELECT 1 FROM dbo.[User] WHERE EmailAddressNormalized = 'tim.howes@kontrolit.net')
BEGIN
  INSERT INTO dbo.[User](FirstName, LastName, EmailAddress, EmailAddressNormalized, IsActive, IsAdmin)
  VALUES('Tim', 'Howes', 'tim.howes@kontrolit.net', 'tim.howes@kontrolit.net', 1, 1);
END
ELSE
BEGIN
  UPDATE dbo.[User]
  SET IsAdmin = 1,
      IsActive = 1
  WHERE EmailAddressNormalized = 'tim.howes@kontrolit.net';
END;

IF OBJECT_ID('dbo.EmailCodes','U') IS NULL
BEGIN
  CREATE TABLE dbo.EmailCodes(
    EmailCodeId int IDENTITY(1,1) NOT NULL PRIMARY KEY,
    Purpose tinyint NOT NULL,
    Email varchar(320) NOT NULL,
    EmailNormalized varchar(320) NOT NULL,
    CodeHash varbinary(64) NOT NULL,
    Salt varbinary(32) NOT NULL,
    ExpiresAtUtc datetime2(0) NOT NULL,
    CreatedAtUtc datetime2(0) NOT NULL CONSTRAINT DF_EmailCodes_CreatedAtUtc DEFAULT SYSUTCDATETIME(),
    FailedAttempts int NOT NULL CONSTRAINT DF_EmailCodes_FailedAttempts DEFAULT(0),
    IsUsed bit NOT NULL CONSTRAINT DF_EmailCodes_IsUsed DEFAULT(0),
    RequestedFromIp varchar(45) NULL,
    RequestedUserAgent varchar(256) NULL
  );
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_EmailCodes_EmailPurposeUsedExpiry' AND object_id = OBJECT_ID('dbo.EmailCodes'))
  CREATE INDEX IX_EmailCodes_EmailPurposeUsedExpiry
    ON dbo.EmailCodes(EmailNormalized, Purpose, IsUsed, ExpiresAtUtc)
    INCLUDE (CreatedAtUtc, FailedAttempts);

IF OBJECT_ID('dbo.LoginCode','U') IS NOT NULL
BEGIN
  INSERT INTO dbo.EmailCodes(
    Purpose,
    Email,
    EmailNormalized,
    CodeHash,
    Salt,
    ExpiresAtUtc,
    CreatedAtUtc,
    FailedAttempts,
    IsUsed)
  SELECT
    1,
    CAST(Email AS varchar(320)),
    LOWER(LTRIM(RTRIM(CAST(Email AS varchar(320))))),
    CodeHash,
    Salt,
    ExpiresAtUtc,
    CreatedAtUtc,
    FailedAttempts,
    IsUsed
  FROM dbo.LoginCode;

  DROP TABLE dbo.LoginCode;
END;

IF OBJECT_ID('dbo.LoginThrottle','U') IS NOT NULL
  DROP TABLE dbo.LoginThrottle;
