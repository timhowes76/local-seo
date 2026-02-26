IF OBJECT_ID('dbo.ExternalApiHealth','U') IS NULL
BEGIN
  CREATE TABLE dbo.ExternalApiHealth(
    Id int IDENTITY(1,1) NOT NULL PRIMARY KEY,
    [Name] nvarchar(64) NOT NULL,
    IsUp bit NOT NULL CONSTRAINT DF_ExternalApiHealth_IsUp DEFAULT(0),
    IsDegraded bit NOT NULL CONSTRAINT DF_ExternalApiHealth_IsDegraded DEFAULT(0),
    CheckedAtUtc datetime2(3) NOT NULL CONSTRAINT DF_ExternalApiHealth_CheckedAtUtc DEFAULT SYSUTCDATETIME(),
    LatencyMs int NOT NULL CONSTRAINT DF_ExternalApiHealth_LatencyMs DEFAULT(0),
    EndpointCalled nvarchar(256) NOT NULL CONSTRAINT DF_ExternalApiHealth_EndpointCalled DEFAULT(N''),
    HttpStatusCode int NULL,
    [Error] nvarchar(512) NULL
  );
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_ExternalApiHealth_Name' AND object_id=OBJECT_ID('dbo.ExternalApiHealth'))
  CREATE UNIQUE INDEX UX_ExternalApiHealth_Name ON dbo.ExternalApiHealth([Name]);
