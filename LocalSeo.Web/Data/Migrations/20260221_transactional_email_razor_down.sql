IF OBJECT_ID('dbo.EmailSettings', 'U') IS NOT NULL
BEGIN
    DROP TABLE dbo.EmailSettings;
END;

IF OBJECT_ID('dbo.EmailTemplate', 'U') IS NOT NULL
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_EmailTemplate_EmailTemplateId' AND object_id = OBJECT_ID('dbo.EmailTemplate'))
        DROP INDEX UX_EmailTemplate_EmailTemplateId ON dbo.EmailTemplate;

    IF COL_LENGTH('dbo.EmailTemplate', 'ViewPath') IS NOT NULL
        ALTER TABLE dbo.EmailTemplate DROP COLUMN ViewPath;

    IF COL_LENGTH('dbo.EmailTemplate', 'EmailTemplateId') IS NOT NULL
        ALTER TABLE dbo.EmailTemplate DROP COLUMN EmailTemplateId;
END;
