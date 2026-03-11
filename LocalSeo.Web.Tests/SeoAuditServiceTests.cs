using LocalSeo.Web.Models;
using LocalSeo.Web.Services;
using Microsoft.Extensions.Logging.Abstractions;

namespace LocalSeo.Web.Tests;

public sealed class SeoAuditServiceTests
{
    [Fact]
    public async Task GetAuditSummaryForPlaceAsync_GroupsInfoSeverityIntoInformationOnly()
    {
        var service = new SeoAuditService(
            new StubSeoAuditRepository(
                [
                    new SeoAuditPlaceResultRow
                    {
                        SeoAuditRuleId = 1,
                        RuleKey = "InfoRule",
                        Name = "Info Rule",
                        Severity = SeoAuditSeverityLevels.Info,
                        Status = SeoAuditStatuses.Warning,
                        ScoreImpactApplied = 3,
                        PossiblePoints = 5,
                        SummaryText = "Context only",
                        SortOrderSnapshot = 10,
                        LastEvaluatedAtUtc = new DateTime(2026, 3, 10, 10, 0, 0, DateTimeKind.Utc)
                    },
                    new SeoAuditPlaceResultRow
                    {
                        SeoAuditRuleId = 2,
                        RuleKey = "WarnRule",
                        Name = "Warn Rule",
                        Severity = SeoAuditSeverityLevels.Warning,
                        Status = SeoAuditStatuses.Warning,
                        ScoreImpactApplied = 4,
                        PossiblePoints = 8,
                        SummaryText = "Needs work",
                        SortOrderSnapshot = 20,
                        LastEvaluatedAtUtc = new DateTime(2026, 3, 10, 10, 0, 0, DateTimeKind.Utc)
                    },
                    new SeoAuditPlaceResultRow
                    {
                        SeoAuditRuleId = 3,
                        RuleKey = "PassRule",
                        Name = "Pass Rule",
                        Severity = SeoAuditSeverityLevels.Warning,
                        Status = SeoAuditStatuses.Pass,
                        ScoreImpactApplied = 0,
                        PossiblePoints = 6,
                        SummaryText = "Good",
                        SortOrderSnapshot = 30,
                        LastEvaluatedAtUtc = new DateTime(2026, 3, 10, 10, 0, 0, DateTimeKind.Utc)
                    },
                    new SeoAuditPlaceResultRow
                    {
                        SeoAuditRuleId = 4,
                        RuleKey = "InfoNaRule",
                        Name = "Info N/A Rule",
                        Severity = SeoAuditSeverityLevels.Info,
                        Status = SeoAuditStatuses.NotApplicable,
                        ScoreImpactApplied = 0,
                        PossiblePoints = 0,
                        SummaryText = "N/A",
                        SortOrderSnapshot = 40,
                        LastEvaluatedAtUtc = new DateTime(2026, 3, 10, 10, 0, 0, DateTimeKind.Utc)
                    }
                ]),
            [],
            TimeProvider.System,
            NullLogger<SeoAuditService>.Instance);

        var summary = await service.GetAuditSummaryForPlaceAsync("place-1", CancellationToken.None);

        Assert.NotNull(summary);
        Assert.Single(summary.InformationOnly);
        Assert.Equal("InfoRule", summary.InformationOnly[0].RuleKey);
        Assert.Single(summary.ActionsNeeded);
        Assert.Equal("WarnRule", summary.ActionsNeeded[0].RuleKey);
        Assert.Single(summary.AlreadyGood);
        Assert.Equal("PassRule", summary.AlreadyGood[0].RuleKey);
    }

    private sealed class StubSeoAuditRepository(IReadOnlyList<SeoAuditPlaceResultRow> rows) : ISeoAuditRepository
    {
        public Task<IReadOnlyList<SeoAuditRuleDefinition>> GetAllRulesAsync(CancellationToken ct) => Task.FromResult<IReadOnlyList<SeoAuditRuleDefinition>>([]);
        public Task<IReadOnlyList<SeoAuditRuleListRow>> GetAdminRuleListAsync(CancellationToken ct) => Task.FromResult<IReadOnlyList<SeoAuditRuleListRow>>([]);
        public Task<SeoAuditRuleDefinition?> GetRuleByIdAsync(long ruleId, CancellationToken ct) => Task.FromResult<SeoAuditRuleDefinition?>(null);
        public Task<long> CreateRuleAsync(SeoAuditRuleUpsertRequest request, CancellationToken ct) => throw new NotSupportedException();
        public Task<bool> UpdateRuleAsync(long ruleId, SeoAuditRuleUpsertRequest request, CancellationToken ct) => throw new NotSupportedException();
        public Task<bool> SetRuleActiveAsync(long ruleId, bool isActive, CancellationToken ct) => throw new NotSupportedException();
        public Task<PlaceAuditContext?> GetPlaceAuditContextAsync(string placeId, CancellationToken ct) => throw new NotSupportedException();
        public Task UpsertAuditResultsAsync(string placeId, long? lastSourceSearchRunId, IReadOnlyList<SeoAuditEvaluationResult> results, DateTime nowUtc, CancellationToken ct) => throw new NotSupportedException();
        public Task<IReadOnlyList<SeoAuditPlaceResultRow>> GetAuditResultsForPlaceAsync(string placeId, CancellationToken ct) => Task.FromResult(rows);
        public Task<IReadOnlyList<string>> GetPlaceIdsMissingResultsAsync(CancellationToken ct) => Task.FromResult<IReadOnlyList<string>>([]);
        public Task<IReadOnlyList<string>> GetAllPlaceIdsAsync(CancellationToken ct) => Task.FromResult<IReadOnlyList<string>>([]);
    }
}
