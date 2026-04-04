DELETE FROM dbo.SeoAuditRule
WHERE RuleKey IN (
  N'HtmlNapMatchesGbpNap',
  N'KeywordsInLandingPageTitleTag',
  N'KeywordsInLandingPageHeadings',
  N'HomepageNicheFocus',
  N'HomepageTopicalKeywordRelevance',
  N'HomepageInternalLinking',
  N'KeywordsInInternalLinkAnchorText',
  N'WebsiteUsesHttpsByDefault',
  N'KeywordsInDomainName'
);
