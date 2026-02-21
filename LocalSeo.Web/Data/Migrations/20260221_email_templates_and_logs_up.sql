IF OBJECT_ID('dbo.EmailTemplate', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.EmailTemplate
    (
        Id int IDENTITY(1,1) NOT NULL CONSTRAINT PK_EmailTemplate PRIMARY KEY,
        [Key] nvarchar(100) NOT NULL,
        [Name] nvarchar(200) NOT NULL,
        FromName nvarchar(200) NULL,
        FromEmail nvarchar(320) NOT NULL,
        SubjectTemplate nvarchar(max) NOT NULL,
        BodyHtmlTemplate nvarchar(max) NOT NULL,
        IsSensitive bit NOT NULL CONSTRAINT DF_EmailTemplate_IsSensitive_Migration DEFAULT(0),
        IsEnabled bit NOT NULL CONSTRAINT DF_EmailTemplate_IsEnabled_Migration DEFAULT(1),
        CreatedUtc datetime2(3) NOT NULL CONSTRAINT DF_EmailTemplate_CreatedUtc_Migration DEFAULT SYSUTCDATETIME(),
        UpdatedUtc datetime2(3) NOT NULL CONSTRAINT DF_EmailTemplate_UpdatedUtc_Migration DEFAULT SYSUTCDATETIME()
    );
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_EmailTemplate_Key' AND object_id = OBJECT_ID('dbo.EmailTemplate'))
    CREATE UNIQUE INDEX UX_EmailTemplate_Key ON dbo.EmailTemplate([Key]);

IF OBJECT_ID('dbo.EmailLog', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.EmailLog
    (
        Id bigint IDENTITY(1,1) NOT NULL CONSTRAINT PK_EmailLog PRIMARY KEY,
        CreatedUtc datetime2(3) NOT NULL CONSTRAINT DF_EmailLog_CreatedUtc_Migration DEFAULT SYSUTCDATETIME(),
        TemplateKey nvarchar(100) NOT NULL,
        ToEmail nvarchar(320) NOT NULL,
        ToEmailHash binary(32) NOT NULL,
        FromName nvarchar(200) NULL,
        FromEmail nvarchar(320) NOT NULL,
        SubjectRendered nvarchar(max) NOT NULL,
        BodyHtmlRendered nvarchar(max) NOT NULL,
        IsSensitive bit NOT NULL CONSTRAINT DF_EmailLog_IsSensitive_Migration DEFAULT(0),
        RedactionApplied bit NOT NULL CONSTRAINT DF_EmailLog_RedactionApplied_Migration DEFAULT(0),
        [Status] nvarchar(20) NOT NULL,
        [Error] nvarchar(max) NULL,
        CorrelationId nvarchar(64) NULL,
        SendGridMessageId nvarchar(200) NULL,
        LastProviderEvent nvarchar(50) NULL,
        LastProviderEventUtc datetime2(3) NULL
    );
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_EmailLog_CreatedUtc' AND object_id = OBJECT_ID('dbo.EmailLog'))
    CREATE INDEX IX_EmailLog_CreatedUtc ON dbo.EmailLog(CreatedUtc DESC);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_EmailLog_ToEmailHash' AND object_id = OBJECT_ID('dbo.EmailLog'))
    CREATE INDEX IX_EmailLog_ToEmailHash ON dbo.EmailLog(ToEmailHash);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_EmailLog_TemplateKey' AND object_id = OBJECT_ID('dbo.EmailLog'))
    CREATE INDEX IX_EmailLog_TemplateKey ON dbo.EmailLog(TemplateKey);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_EmailLog_SendGridMessageId' AND object_id = OBJECT_ID('dbo.EmailLog'))
    CREATE INDEX IX_EmailLog_SendGridMessageId ON dbo.EmailLog(SendGridMessageId) WHERE SendGridMessageId IS NOT NULL;

IF OBJECT_ID('dbo.EmailProviderEvent', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.EmailProviderEvent
    (
        Id bigint IDENTITY(1,1) NOT NULL CONSTRAINT PK_EmailProviderEvent PRIMARY KEY,
        EmailLogId bigint NULL,
        Provider nvarchar(50) NOT NULL CONSTRAINT DF_EmailProviderEvent_Provider_Migration DEFAULT(N'SendGrid'),
        EventType nvarchar(50) NOT NULL,
        EventUtc datetime2(3) NOT NULL,
        ProviderMessageId nvarchar(200) NOT NULL,
        PayloadJson nvarchar(max) NULL,
        CreatedUtc datetime2(3) NOT NULL CONSTRAINT DF_EmailProviderEvent_CreatedUtc_Migration DEFAULT SYSUTCDATETIME()
    );
END;

IF NOT EXISTS (
    SELECT 1
    FROM sys.foreign_keys
    WHERE name = 'FK_EmailProviderEvent_EmailLog'
      AND parent_object_id = OBJECT_ID('dbo.EmailProviderEvent')
)
    ALTER TABLE dbo.EmailProviderEvent
        WITH CHECK ADD CONSTRAINT FK_EmailProviderEvent_EmailLog
        FOREIGN KEY (EmailLogId) REFERENCES dbo.EmailLog(Id) ON DELETE SET NULL;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_EmailProviderEvent_Idempotency' AND object_id = OBJECT_ID('dbo.EmailProviderEvent'))
    CREATE UNIQUE INDEX UX_EmailProviderEvent_Idempotency ON dbo.EmailProviderEvent(Provider, ProviderMessageId, EventType, EventUtc);

MERGE dbo.EmailTemplate AS target
USING (VALUES
    (N'TwoFactorCode', N'Two Factor Code', N'Local SEO Tool', N'noreply@kontrolit.net', N'Your Local SEO login code', N'<p>Your 2FA login code is <strong>[%Code%]</strong>.</p><p>It expires in [%ExpiryMinutes%] minutes.</p>', 1, 1),
    (N'PasswordReset', N'Password Reset', N'Local SEO Tool', N'noreply@kontrolit.net', N'Your Local SEO password reset code', N'<p>Use code <strong>[%Code%]</strong> to reset your password.</p><p><a href="[%ResetUrl%]">Reset password</a></p><p>This code expires in [%ExpiryMinutes%] minutes.</p>', 1, 1),
    (N'NewUserInvite', N'New User Invite', N'Local SEO Tool', N'noreply@kontrolit.net', N'You have been invited to Local SEO', N'<p>Hi [%RecipientName%],</p><p>You have been invited to Local SEO.</p><p><a href="[%InviteUrl%]">Open invite link</a></p><p>This link expires at [%ExpiresAtUtc%].</p>', 1, 1),
    (N'InviteOtp', N'Invite OTP', N'Local SEO Tool', N'noreply@kontrolit.net', N'Your Local SEO invite verification code', N'<p>Your invite verification code is <strong>[%Code%]</strong>.</p><p>It expires at [%ExpiresAtUtc%].</p>', 1, 1),
    (N'ChangePasswordOtp', N'Change Password OTP', N'Local SEO Tool', N'noreply@kontrolit.net', N'Your Local SEO change password verification code', N'<p>Your change password verification code is <strong>[%Code%]</strong>.</p><p>It expires at [%ExpiresAtUtc%].</p>', 1, 1)
) AS source([Key], [Name], FromName, FromEmail, SubjectTemplate, BodyHtmlTemplate, IsSensitive, IsEnabled)
ON target.[Key] = source.[Key]
WHEN NOT MATCHED THEN
    INSERT([Key], [Name], FromName, FromEmail, SubjectTemplate, BodyHtmlTemplate, IsSensitive, IsEnabled, CreatedUtc, UpdatedUtc)
    VALUES(source.[Key], source.[Name], source.FromName, source.FromEmail, source.SubjectTemplate, source.BodyHtmlTemplate, source.IsSensitive, source.IsEnabled, SYSUTCDATETIME(), SYSUTCDATETIME());
