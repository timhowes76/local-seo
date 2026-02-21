IF OBJECT_ID('dbo.EmailProviderEvent', 'U') IS NOT NULL
BEGIN
    DROP TABLE dbo.EmailProviderEvent;
END;

IF OBJECT_ID('dbo.EmailLog', 'U') IS NOT NULL
BEGIN
    DROP TABLE dbo.EmailLog;
END;

IF OBJECT_ID('dbo.EmailTemplate', 'U') IS NOT NULL
BEGIN
    DROP TABLE dbo.EmailTemplate;
END;
