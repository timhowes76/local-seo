IF OBJECT_ID('dbo.EmailTemplate','U') IS NULL
BEGIN
    CREATE TABLE dbo.EmailTemplate
    (
        Id int IDENTITY(1,1) NOT NULL CONSTRAINT PK_EmailTemplate PRIMARY KEY,
        EmailTemplateId AS (CONVERT(int, [Id])) PERSISTED,
        [Key] nvarchar(100) NOT NULL,
        [Name] nvarchar(200) NOT NULL CONSTRAINT DF_EmailTemplate_Name_Migration DEFAULT(N''),
        FromName nvarchar(200) NULL,
        FromEmail nvarchar(320) NOT NULL CONSTRAINT DF_EmailTemplate_FromEmail_Migration DEFAULT(N'noreply@example.local'),
        SubjectTemplate nvarchar(255) NOT NULL,
        ViewPath nvarchar(260) NOT NULL,
        BodyHtmlTemplate nvarchar(max) NOT NULL CONSTRAINT DF_EmailTemplate_BodyHtmlTemplate_Migration DEFAULT(N''),
        IsSensitive bit NOT NULL CONSTRAINT DF_EmailTemplate_IsSensitive_Migration DEFAULT(0),
        IsEnabled bit NOT NULL CONSTRAINT DF_EmailTemplate_IsEnabled_Migration DEFAULT(1),
        CreatedUtc datetime2(3) NOT NULL CONSTRAINT DF_EmailTemplate_CreatedUtc_Migration DEFAULT SYSUTCDATETIME(),
        UpdatedUtc datetime2(3) NOT NULL CONSTRAINT DF_EmailTemplate_UpdatedUtc_Migration DEFAULT SYSUTCDATETIME()
    );
END;

IF COL_LENGTH('dbo.EmailTemplate', 'EmailTemplateId') IS NULL
    ALTER TABLE dbo.EmailTemplate ADD EmailTemplateId AS (CONVERT(int, [Id])) PERSISTED;

IF COL_LENGTH('dbo.EmailTemplate', 'SubjectTemplate') IS NULL
    ALTER TABLE dbo.EmailTemplate ADD SubjectTemplate nvarchar(255) NOT NULL CONSTRAINT DF_EmailTemplate_SubjectTemplate_Migration DEFAULT(N'');

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id=OBJECT_ID('dbo.EmailTemplate') AND name='SubjectTemplate' AND max_length = -1)
BEGIN
    UPDATE dbo.EmailTemplate
    SET SubjectTemplate = LEFT(ISNULL(SubjectTemplate, N''), 255)
    WHERE LEN(ISNULL(SubjectTemplate, N'')) > 255;

    ALTER TABLE dbo.EmailTemplate ALTER COLUMN SubjectTemplate nvarchar(255) NOT NULL;
END;

IF COL_LENGTH('dbo.EmailTemplate', 'ViewPath') IS NULL
    ALTER TABLE dbo.EmailTemplate ADD ViewPath nvarchar(260) NOT NULL CONSTRAINT DF_EmailTemplate_ViewPath_Migration DEFAULT(N'PasswordReset.cshtml');

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_EmailTemplate_Key' AND object_id = OBJECT_ID('dbo.EmailTemplate'))
    CREATE UNIQUE INDEX UX_EmailTemplate_Key ON dbo.EmailTemplate([Key]);

IF COL_LENGTH('dbo.EmailTemplate', 'EmailTemplateId') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_EmailTemplate_EmailTemplateId' AND object_id = OBJECT_ID('dbo.EmailTemplate'))
BEGIN
    EXEC(N'CREATE UNIQUE INDEX UX_EmailTemplate_EmailTemplateId ON dbo.EmailTemplate(EmailTemplateId);');
END;

IF COL_LENGTH('dbo.EmailTemplate', 'ViewPath') IS NOT NULL
BEGIN
    EXEC(N'
UPDATE dbo.EmailTemplate
SET ViewPath = CASE
                  WHEN [Key] = N''PasswordReset'' THEN N''PasswordReset.cshtml''
                  WHEN LEN(LTRIM(RTRIM(ISNULL(ViewPath, N'''')))) = 0 THEN CONCAT([Key], N''.cshtml'')
                  ELSE ViewPath
               END
WHERE ViewPath IS NULL
   OR LEN(LTRIM(RTRIM(ViewPath))) = 0;');
END;

IF OBJECT_ID('dbo.EmailSettings','U') IS NULL
BEGIN
    CREATE TABLE dbo.EmailSettings
    (
        EmailSettingsId int IDENTITY(1,1) NOT NULL CONSTRAINT PK_EmailSettings PRIMARY KEY,
        FromEmail nvarchar(320) NOT NULL,
        FromName nvarchar(200) NOT NULL,
        GlobalSignatureHtml nvarchar(max) NOT NULL,
        WrapperViewPath nvarchar(260) NOT NULL,
        UpdatedUtc datetime2(3) NOT NULL CONSTRAINT DF_EmailSettings_UpdatedUtc_Migration DEFAULT SYSUTCDATETIME()
    );
END;

IF NOT EXISTS (SELECT 1 FROM dbo.EmailSettings)
BEGIN
    INSERT INTO dbo.EmailSettings(FromEmail, FromName, GlobalSignatureHtml, WrapperViewPath, UpdatedUtc)
    VALUES (N'noreply@kontrolit.net', N'Local SEO Tool', N'<p>Kind regards,<br/>Local SEO Team</p>', N'_EmailWrapper.cshtml', SYSUTCDATETIME());
END;

MERGE dbo.EmailTemplate AS target
USING (VALUES
    (N'PasswordReset', N'Password Reset', N'Local SEO Tool', N'noreply@kontrolit.net', N'Password reset for {{RecipientName}}', N'PasswordReset.cshtml', N'', 0, 1)
) AS source([Key], [Name], FromName, FromEmail, SubjectTemplate, ViewPath, BodyHtmlTemplate, IsSensitive, IsEnabled)
ON target.[Key] = source.[Key]
WHEN MATCHED THEN
    UPDATE SET
      SubjectTemplate = source.SubjectTemplate,
      ViewPath = source.ViewPath,
      IsEnabled = source.IsEnabled,
      UpdatedUtc = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT([Key], [Name], FromName, FromEmail, SubjectTemplate, ViewPath, BodyHtmlTemplate, IsSensitive, IsEnabled, CreatedUtc, UpdatedUtc)
    VALUES(source.[Key], source.[Name], source.FromName, source.FromEmail, source.SubjectTemplate, source.ViewPath, source.BodyHtmlTemplate, source.IsSensitive, source.IsEnabled, SYSUTCDATETIME(), SYSUTCDATETIME());
