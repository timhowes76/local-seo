IF COL_LENGTH('dbo.Place', 'WebsiteType') IS NULL
  ALTER TABLE dbo.Place ADD WebsiteType tinyint NOT NULL CONSTRAINT DF_Place_WebsiteType DEFAULT(0);
GO

IF COL_LENGTH('dbo.Place', 'WebsiteType') IS NOT NULL
  AND NOT EXISTS (
  SELECT 1
  FROM sys.check_constraints
  WHERE name = 'CK_Place_WebsiteType_Valid'
    AND parent_object_id = OBJECT_ID('dbo.Place')
)
  EXEC(N'ALTER TABLE dbo.Place WITH CHECK ADD CONSTRAINT CK_Place_WebsiteType_Valid CHECK (WebsiteType IN (0, 1, 2));');
GO

;WITH SocialDomains AS (
  SELECT Domain FROM (VALUES
    (N'facebook.com'),
    (N'm.facebook.com'),
    (N'fb.com'),
    (N'instagram.com'),
    (N'linkedin.com'),
    (N'lnk.bio'),
    (N'linktr.ee'),
    (N'x.com'),
    (N'twitter.com'),
    (N'tiktok.com'),
    (N'youtube.com'),
    (N'youtu.be'),
    (N'pinterest.com'),
    (N'pin.it'),
    (N'snapchat.com'),
    (N'threads.net'),
    (N'whatsapp.com'),
    (N'wa.me'),
    (N'telegram.me'),
    (N't.me')
  ) AS v(Domain)
),
NormalizedPlace AS (
  SELECT
    p.PlaceId,
    LOWER(LTRIM(RTRIM(COALESCE(p.WebsiteUri, N'')))) AS NormalizedWebsite,
    p.WebsiteType
  FROM dbo.Place p
),
PreparedUrl AS (
  SELECT
    np.PlaceId,
    np.NormalizedWebsite,
    np.WebsiteType,
    CASE
      WHEN np.NormalizedWebsite = N'' THEN N''
      WHEN np.NormalizedWebsite LIKE N'http://%' OR np.NormalizedWebsite LIKE N'https://%' THEN np.NormalizedWebsite
      WHEN np.NormalizedWebsite LIKE N'//%' THEN N'https:' + np.NormalizedWebsite
      ELSE N'https://' + np.NormalizedWebsite
    END AS UrlWithScheme
  FROM NormalizedPlace np
),
UrlWithoutScheme AS (
  SELECT
    pu.PlaceId,
    pu.NormalizedWebsite,
    pu.WebsiteType,
    CASE
      WHEN pu.UrlWithScheme = N'' THEN N''
      WHEN CHARINDEX(N'://', pu.UrlWithScheme) > 0 THEN SUBSTRING(pu.UrlWithScheme, CHARINDEX(N'://', pu.UrlWithScheme) + 3, LEN(pu.UrlWithScheme))
      ELSE pu.UrlWithScheme
    END AS ValueAfterScheme
  FROM PreparedUrl pu
),
HostPortAndPath AS (
  SELECT
    uws.PlaceId,
    uws.NormalizedWebsite,
    uws.WebsiteType,
    CASE
      WHEN uws.ValueAfterScheme = N'' THEN N''
      WHEN PATINDEX(N'%[/?#]%', uws.ValueAfterScheme) = 0 THEN uws.ValueAfterScheme
      ELSE LEFT(uws.ValueAfterScheme, PATINDEX(N'%[/?#]%', uws.ValueAfterScheme) - 1)
    END AS HostPortAndPath
  FROM UrlWithoutScheme uws
),
HostWithoutUserInfo AS (
  SELECT
    hp.PlaceId,
    hp.NormalizedWebsite,
    hp.WebsiteType,
    CASE
      WHEN hp.HostPortAndPath = N'' THEN N''
      WHEN CHARINDEX(N'@', hp.HostPortAndPath) > 0 THEN RIGHT(hp.HostPortAndPath, LEN(hp.HostPortAndPath) - CHARINDEX(N'@', hp.HostPortAndPath))
      ELSE hp.HostPortAndPath
    END AS HostWithPort
  FROM HostPortAndPath hp
),
ResolvedHost AS (
  SELECT
    hui.PlaceId,
    hui.NormalizedWebsite,
    hui.WebsiteType,
    LOWER(LTRIM(RTRIM(
      CASE
        WHEN hui.HostWithPort = N'' THEN N''
        WHEN CHARINDEX(N':', hui.HostWithPort) > 0 THEN LEFT(hui.HostWithPort, CHARINDEX(N':', hui.HostWithPort) - 1)
        ELSE hui.HostWithPort
      END
    ))) AS Host
  FROM HostWithoutUserInfo hui
)
UPDATE p
SET WebsiteType = CASE
  WHEN rh.NormalizedWebsite = N'' THEN 0
  WHEN EXISTS (
    SELECT 1
    FROM SocialDomains sd
    WHERE rh.Host = sd.Domain
       OR rh.Host LIKE N'%.' + sd.Domain
  ) THEN 2
  ELSE 1
END
FROM dbo.Place p
JOIN ResolvedHost rh ON rh.PlaceId = p.PlaceId
WHERE rh.WebsiteType NOT IN (0, 1, 2)
   OR (rh.WebsiteType = 0 AND rh.NormalizedWebsite <> N'')
   OR (rh.WebsiteType IN (1, 2) AND rh.NormalizedWebsite = N'')
   OR (rh.WebsiteType = 1 AND EXISTS (
        SELECT 1
        FROM SocialDomains sd
        WHERE rh.Host = sd.Domain
           OR rh.Host LIKE N'%.' + sd.Domain
      ))
   OR (rh.WebsiteType = 2 AND rh.NormalizedWebsite <> N'' AND NOT EXISTS (
        SELECT 1
        FROM SocialDomains sd
        WHERE rh.Host = sd.Domain
           OR rh.Host LIKE N'%.' + sd.Domain
      ));

UPDATE dbo.SeoAuditRule
SET
  [Description] = N'No proper website is stored, or only a social profile URL is present.',
  WhyItMattersText = N'A proper website gives the business a direct conversion path. Social profile links alone do not count as a business website.',
  RecommendedActionText = N'Add the correct business website URL to the profile. Social media profile links should not be used as the main website.',
  UpdatedAtUtc = SYSUTCDATETIME()
WHERE RuleKey = N'NoWebsite';

;WITH NoWebsiteRule AS (
  SELECT TOP 1
    SeoAuditRuleId,
    FailScoreImpact,
    SortOrder,
    WhyItMattersText,
    RecommendedActionText
  FROM dbo.SeoAuditRule
  WHERE RuleKey = N'NoWebsite'
  ORDER BY SeoAuditRuleId ASC
),
NoWebsiteResults AS (
  SELECT
    p.PlaceId,
    r.SeoAuditRuleId,
    CASE WHEN p.WebsiteType = 1 THEN N'Pass' ELSE N'Fail' END AS [Status],
    CASE WHEN p.WebsiteType = 1 THEN 0 ELSE r.FailScoreImpact END AS ScoreImpactApplied,
    CASE
      WHEN p.WebsiteType = 1 THEN N'Proper website present'
      WHEN p.WebsiteType = 2 THEN N'Social profile URL only'
      ELSE N'No website'
    END AS ActualValue,
    N'Proper website present' AS ExpectedValue,
    CASE WHEN p.WebsiteType = 1 THEN N'0' ELSE N'1 missing proper website' END AS GapValue,
    CASE
      WHEN p.WebsiteType = 1 THEN N'A proper business website is stored.'
      WHEN p.WebsiteType = 2 THEN N'Business profile does not provide a proper website. Social media profile links do not count as a business website.'
      ELSE N'No proper business website is stored.'
    END AS SummaryText,
    r.WhyItMattersText,
    r.RecommendedActionText,
    r.SortOrder
  FROM dbo.Place p
  CROSS JOIN NoWebsiteRule r
)
MERGE dbo.SeoAuditResult AS target
USING NoWebsiteResults AS source
ON target.PlaceId = source.PlaceId
   AND target.SeoAuditRuleId = source.SeoAuditRuleId
WHEN MATCHED THEN UPDATE SET
  [Status] = source.[Status],
  ScoreImpactApplied = source.ScoreImpactApplied,
  ActualValue = source.ActualValue,
  ExpectedValue = source.ExpectedValue,
  GapValue = source.GapValue,
  SummaryText = source.SummaryText,
  WhyItMattersText = source.WhyItMattersText,
  RecommendedActionText = source.RecommendedActionText,
  SortOrderSnapshot = source.SortOrder,
  LastEvaluatedAtUtc = SYSUTCDATETIME(),
  UpdatedAtUtc = SYSUTCDATETIME()
WHEN NOT MATCHED THEN INSERT(
  PlaceId,
  SeoAuditRuleId,
  [Status],
  ScoreImpactApplied,
  ActualValue,
  ExpectedValue,
  GapValue,
  SummaryText,
  WhyItMattersText,
  RecommendedActionText,
  SortOrderSnapshot,
  LastEvaluatedAtUtc,
  CreatedAtUtc,
  UpdatedAtUtc
)
VALUES(
  source.PlaceId,
  source.SeoAuditRuleId,
  source.[Status],
  source.ScoreImpactApplied,
  source.ActualValue,
  source.ExpectedValue,
  source.GapValue,
  source.SummaryText,
  source.WhyItMattersText,
  source.RecommendedActionText,
  source.SortOrder,
  SYSUTCDATETIME(),
  SYSUTCDATETIME(),
  SYSUTCDATETIME()
);
