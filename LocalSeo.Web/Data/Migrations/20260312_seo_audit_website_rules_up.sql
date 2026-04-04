MERGE dbo.SeoAuditRule AS target
USING (VALUES
  (N'HtmlNapMatchesGbpNap', N'HTML NAP matching GBP NAP', N'Checks whether the homepage NAP signals match the stored GBP business name, phone, and address closely enough to support consistency.', N'Website', N'Benchmark', N'HomepageNapMatch', N'WEBSITE', N'Critical', 77, 153, 280, 1, 1, N'Consistent homepage NAP helps reinforce local trust and relevance, and mismatches can create conversion friction.', N'Align the homepage business name, phone, and address with the GBP profile.'),
  (N'KeywordsInLandingPageTitleTag', N'Keywords in GBP landing page title tag', N'Checks whether the homepage title tag matches the run keyword and town using close variants.', N'Website', N'Benchmark', N'HomepageTitleKeywordMatch', N'WEBSITE', N'Opportunity', 73, 146, 290, 1, 1, N'Homepage title tags help communicate local service relevance and are one of the clearest homepage optimisation opportunities.', N'Update the homepage title tag so it reflects the main service and town more clearly.'),
  (N'KeywordsInLandingPageHeadings', N'Keywords in GBP landing page headings', N'Checks whether homepage headings such as H1, H2, and H3 reflect the run keyword and town using close variants.', N'Website', N'Benchmark', N'HomepageHeadingKeywordMatch', N'WEBSITE', N'Opportunity', 68, 135, 300, 1, 1, N'Homepage headings shape topical clarity for both users and search engines and often reveal weak page messaging.', N'Strengthen the homepage heading structure so the main service and town are clearer.'),
  (N'HomepageNicheFocus', N'Website''s degree of focus on a specific niche', N'Uses homepage content only to judge whether the site appears strongly focused on one clear niche or service family.', N'Website', N'Benchmark', N'HomepageNicheFocus', N'WEBSITE', N'Opportunity', 59, 118, 310, 1, 1, N'A tightly focused homepage usually communicates expertise more clearly than broad generic messaging.', N'Sharpen the homepage messaging around the strongest niche or service family.'),
  (N'HomepageTopicalKeywordRelevance', N'Topical keyword relevance across website', N'Uses homepage content only as a proxy for how relevant the site appears to the run service and town terms.', N'Website', N'Benchmark', N'HomepageTopicalKeywordRelevance', N'WEBSITE', N'Opportunity', 59, 117, 320, 1, 1, N'Homepage topical relevance is a strong proxy for whether the site is supporting the target service intent.', N'Make the homepage more topically relevant to the target service and town terms.'),
  (N'HomepageInternalLinking', N'Internal linking across website', N'Uses homepage internal links only as a proxy for how well key service pages are linked from the homepage.', N'Website', N'Benchmark', N'HomepageInternalLinking', N'WEBSITE', N'Opportunity', 54, 107, 330, 1, 1, N'Homepage internal links help users and search engines reach important service pages quickly.', N'Add clearer homepage links to the main service pages.'),
  (N'KeywordsInInternalLinkAnchorText', N'Keywords in anchor text of internal links', N'Checks whether homepage internal anchor text reflects the run keyword and town using close variants.', N'Website', N'Benchmark', N'HomepageAnchorTextKeywordMatch', N'WEBSITE', N'Opportunity', 52, 103, 340, 1, 1, N'Homepage internal anchor text helps reinforce topical clarity and service relevance.', N'Improve homepage internal link anchor text so it reflects real service and location intent.'),
  (N'WebsiteUsesHttpsByDefault', N'Website uses HTTPS by default', N'Checks whether the fetched homepage resolves over HTTPS by default.', N'Website', N'Benchmark', N'HomepageHttpsDefault', N'WEBSITE', N'Warning', 0, 102, 350, 1, 1, N'HTTPS is a basic trust and technical quality expectation for business websites.', N'Serve the homepage over HTTPS by default.'),
  (N'KeywordsInDomainName', N'Keywords in domain name', N'Checks whether the domain name reflects the run keyword and town using close variants.', N'Website', N'Benchmark', N'DomainKeywordMatch', N'WEBSITE', N'Info', 49, 97, 360, 1, 1, N'A domain name that reflects service intent can reinforce relevance, even though it is not the only signal that matters.', N'Consider whether the domain branding supports the target service and town positioning.')
) AS source(RuleKey, [Name], [Description], RuleGroup, RuleMode, RuleType, EntityType, Severity, WarningScoreImpact, FailScoreImpact, SortOrder, IsActive, ShowInActionsTab, WhyItMattersText, RecommendedActionText)
ON target.RuleKey = source.RuleKey
WHEN MATCHED THEN
  UPDATE SET
    target.[Name] = source.[Name],
    target.[Description] = source.[Description],
    target.RuleGroup = source.RuleGroup,
    target.RuleMode = source.RuleMode,
    target.RuleType = source.RuleType,
    target.EntityType = source.EntityType,
    target.Severity = source.Severity,
    target.WarningScoreImpact = source.WarningScoreImpact,
    target.FailScoreImpact = source.FailScoreImpact,
    target.SortOrder = source.SortOrder,
    target.IsActive = source.IsActive,
    target.ShowInActionsTab = source.ShowInActionsTab,
    target.WhyItMattersText = source.WhyItMattersText,
    target.RecommendedActionText = source.RecommendedActionText,
    target.UpdatedAtUtc = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
  INSERT(RuleKey, [Name], [Description], RuleGroup, RuleMode, RuleType, EntityType, Severity, WarningScoreImpact, FailScoreImpact, SortOrder, IsActive, ShowInActionsTab, WhyItMattersText, RecommendedActionText, CreatedAtUtc, UpdatedAtUtc)
  VALUES(source.RuleKey, source.[Name], source.[Description], source.RuleGroup, source.RuleMode, source.RuleType, source.EntityType, source.Severity, source.WarningScoreImpact, source.FailScoreImpact, source.SortOrder, source.IsActive, source.ShowInActionsTab, source.WhyItMattersText, source.RecommendedActionText, SYSUTCDATETIME(), SYSUTCDATETIME());
