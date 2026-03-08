using LocalSeo.Web.Models;

namespace LocalSeo.Web.Services;

public sealed class SeoAuditService(
    ISeoAuditRepository repository,
    IEnumerable<ISeoAuditRuleHandler> ruleHandlers,
    TimeProvider timeProvider,
    ILogger<SeoAuditService> logger) : ISeoAuditService
{
    public async Task<SeoAuditPlaceSummary?> GetAuditSummaryForPlaceAsync(string placeId, CancellationToken ct)
    {
        var rows = await repository.GetAuditResultsForPlaceAsync(placeId, ct);
        if (rows.Count == 0)
            return null;

        return BuildSummary(placeId, rows);
    }

    public Task<IReadOnlyList<SeoAuditPlaceResultRow>> GetAuditResultsForPlaceAsync(string placeId, CancellationToken ct)
    {
        return repository.GetAuditResultsForPlaceAsync(placeId, ct);
    }

    public Task<SeoAuditPlaceSummary> RecalculateAuditForPlaceAsync(string placeId, CancellationToken ct)
    {
        return EvaluatePlaceAsync(placeId, ct);
    }

    public async Task<SeoAuditPlaceSummary> EvaluatePlaceAsync(string placeId, CancellationToken ct)
    {
        var normalizedPlaceId = NormalizeRequired(placeId);
        if (normalizedPlaceId is null)
            throw new InvalidOperationException("PlaceId is required.");

        var context = await repository.GetPlaceAuditContextAsync(normalizedPlaceId, ct)
            ?? throw new InvalidOperationException($"Place '{normalizedPlaceId}' was not found.");
        var rules = await repository.GetAllRulesAsync(ct);
        var activeRules = rules
            .Where(x => x.IsActive)
            .OrderBy(x => x.SortOrder)
            .ThenBy(x => x.SeoAuditRuleId)
            .ToList();

        var evaluations = new List<SeoAuditEvaluationResult>(activeRules.Count);
        foreach (var rule in activeRules)
        {
            var handler = ruleHandlers.FirstOrDefault(x => x.CanEvaluate(rule));
            var evaluation = handler is null
                ? new SeoAuditEvaluationResult(
                    rule.SeoAuditRuleId,
                    rule.RuleKey,
                    SeoAuditStatuses.NotApplicable,
                    0,
                    null,
                    null,
                    null,
                    "This rule mode is not implemented yet.",
                    NormalizeNullable(rule.WhyItMattersText),
                    NormalizeNullable(rule.RecommendedActionText),
                    rule.SortOrder)
                : handler.Evaluate(rule, context);
            evaluations.Add(evaluation);
        }

        var nowUtc = timeProvider.GetUtcNow().UtcDateTime;
        await repository.UpsertAuditResultsAsync(normalizedPlaceId, context.LastSourceSearchRunId, evaluations, nowUtc, ct);
        var savedRows = await repository.GetAuditResultsForPlaceAsync(normalizedPlaceId, ct);
        return BuildSummary(normalizedPlaceId, savedRows);
    }

    public async Task<int> EvaluatePlacesAsync(IEnumerable<string> placeIds, CancellationToken ct)
    {
        var normalizedPlaceIds = placeIds
            .Select(NormalizeRequired)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        var evaluated = 0;
        foreach (var placeId in normalizedPlaceIds)
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                await EvaluatePlaceAsync(placeId!, ct);
                evaluated++;
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                logger.LogWarning(ex, "Audit evaluation failed for place {PlaceId}.", placeId);
            }
        }

        return evaluated;
    }

    public async Task<int> RecalculateAllMissingAuditResultsAsync(CancellationToken ct)
    {
        var placeIds = await repository.GetPlaceIdsMissingResultsAsync(ct);
        return await EvaluatePlacesAsync(placeIds, ct);
    }

    public async Task<int> RecalculateAllAuditResultsAsync(CancellationToken ct)
    {
        var placeIds = await repository.GetAllPlaceIdsAsync(ct);
        return await EvaluatePlacesAsync(placeIds, ct);
    }

    public Task<IReadOnlyList<SeoAuditRuleListRow>> GetAdminRuleListAsync(CancellationToken ct)
    {
        return repository.GetAdminRuleListAsync(ct);
    }

    public async Task<SeoAuditRuleEditModel?> GetAdminRuleEditModelAsync(long ruleId, CancellationToken ct)
    {
        var rule = await repository.GetRuleByIdAsync(ruleId, ct);
        if (rule is null)
            return null;

        return new SeoAuditRuleEditModel
        {
            SeoAuditRuleId = rule.SeoAuditRuleId,
            RuleKey = rule.RuleKey,
            Name = rule.Name,
            Description = rule.Description,
            RuleGroup = rule.RuleGroup,
            RuleMode = rule.RuleMode,
            RuleType = rule.RuleType,
            EntityType = rule.EntityType,
            Severity = rule.Severity,
            WarningScoreImpact = rule.WarningScoreImpact,
            FailScoreImpact = rule.FailScoreImpact,
            SortOrder = rule.SortOrder,
            IsActive = rule.IsActive,
            ShowInActionsTab = rule.ShowInActionsTab,
            WhyItMattersText = rule.WhyItMattersText,
            RecommendedActionText = rule.RecommendedActionText,
            Parameters = rule.Parameters
                .Select(x => new SeoAuditRuleParameterEditModel
                {
                    SeoAuditRuleParameterId = x.SeoAuditRuleParameterId,
                    ParameterName = x.ParameterName,
                    ParameterValue = x.ParameterValue,
                    ValueType = x.ValueType,
                    SortOrder = x.SortOrder,
                    IsActive = x.IsActive
                })
                .ToList()
        };
    }

    public async Task<(bool Success, string Message, long? RuleId)> CreateRuleAsync(SeoAuditRuleEditModel model, CancellationToken ct)
    {
        var normalized = NormalizeRuleModel(model, isCreate: true, timeProvider.GetUtcNow().UtcDateTime);
        if (!normalized.Success || normalized.Request is null)
            return (false, normalized.Message, null);

        var allRules = await repository.GetAllRulesAsync(ct);
        if (allRules.Any(x => string.Equals(x.RuleKey, normalized.Request.RuleKey, StringComparison.OrdinalIgnoreCase)))
            return (false, "Rule key already exists.", null);

        var ruleId = await repository.CreateRuleAsync(normalized.Request, ct);
        return (true, "Audit rule created.", ruleId);
    }

    public async Task<(bool Success, string Message)> UpdateRuleAsync(long ruleId, SeoAuditRuleEditModel model, CancellationToken ct)
    {
        var existingRule = await repository.GetRuleByIdAsync(ruleId, ct);
        if (existingRule is null)
            return (false, "Audit rule not found.");

        model.RuleKey = existingRule.RuleKey;
        var normalized = NormalizeRuleModel(model, isCreate: false, timeProvider.GetUtcNow().UtcDateTime);
        if (!normalized.Success || normalized.Request is null)
            return (false, normalized.Message);

        var updated = await repository.UpdateRuleAsync(ruleId, normalized.Request, ct);
        return updated
            ? (true, "Audit rule updated.")
            : (false, "Audit rule not found.");
    }

    public async Task<(bool Success, string Message)> ToggleRuleActiveAsync(long ruleId, bool isActive, CancellationToken ct)
    {
        var changed = await repository.SetRuleActiveAsync(ruleId, isActive, ct);
        return changed
            ? (true, isActive ? "Audit rule activated." : "Audit rule deactivated.")
            : (false, "Audit rule not found.");
    }

    private static SeoAuditPlaceSummary BuildSummary(string placeId, IReadOnlyList<SeoAuditPlaceResultRow> rows)
    {
        var orderedRows = rows
            .OrderByDescending(x => x.ScoreImpactApplied)
            .ThenBy(x => x.SortOrderSnapshot)
            .ThenBy(x => x.SeoAuditRuleId)
            .ToList();

        var applicableRows = orderedRows
            .Where(x => !string.Equals(x.Status, SeoAuditStatuses.NotApplicable, StringComparison.OrdinalIgnoreCase))
            .ToList();
        var possiblePoints = applicableRows.Sum(x => Math.Max(0, x.PossiblePoints));
        var earnedPoints = applicableRows.Sum(x => x.Status switch
        {
            SeoAuditStatuses.Pass => Math.Max(0, x.PossiblePoints),
            SeoAuditStatuses.Warning => Math.Max(0, x.PossiblePoints - Math.Min(x.PossiblePoints, Math.Max(0, x.ScoreImpactApplied))),
            SeoAuditStatuses.Fail => 0,
            _ => 0
        });

        var scorePercentage = possiblePoints <= 0
            ? 100
            : (int)Math.Round((decimal)earnedPoints * 100m / possiblePoints, MidpointRounding.AwayFromZero);

        return new SeoAuditPlaceSummary
        {
            PlaceId = placeId,
            ScorePercentage = scorePercentage,
            LastEvaluatedAtUtc = orderedRows.Count == 0 ? null : orderedRows.Max(x => x.LastEvaluatedAtUtc),
            LastSourceSearchRunId = orderedRows
                .Where(x => x.LastSourceSearchRunId.HasValue)
                .OrderByDescending(x => x.LastEvaluatedAtUtc)
                .Select(x => x.LastSourceSearchRunId)
                .FirstOrDefault(),
            HasResults = orderedRows.Count > 0,
            ActionsNeeded = orderedRows
                .Where(x => string.Equals(x.Status, SeoAuditStatuses.Warning, StringComparison.OrdinalIgnoreCase) || string.Equals(x.Status, SeoAuditStatuses.Fail, StringComparison.OrdinalIgnoreCase))
                .ToList(),
            AlreadyGood = orderedRows
                .Where(x => string.Equals(x.Status, SeoAuditStatuses.Pass, StringComparison.OrdinalIgnoreCase))
                .ToList()
        };
    }

    private static (bool Success, string Message, SeoAuditRuleUpsertRequest? Request) NormalizeRuleModel(SeoAuditRuleEditModel model, bool isCreate, DateTime nowUtc)
    {
        var ruleKey = NormalizeRequired(model.RuleKey);
        if (isCreate && ruleKey is null)
            return (false, "Rule key is required.", null);

        var name = NormalizeRequired(model.Name);
        if (name is null)
            return (false, "Name is required.", null);

        if (model.WarningScoreImpact < 0)
            return (false, "Warning score impact cannot be negative.", null);
        if (model.FailScoreImpact < 0)
            return (false, "Fail score impact cannot be negative.", null);
        if (model.FailScoreImpact < model.WarningScoreImpact)
            return (false, "Fail score impact must be greater than or equal to warning score impact.", null);

        var parameters = NormalizeParameters(model.Parameters);
        return (true, string.Empty, new SeoAuditRuleUpsertRequest(
            ruleKey ?? string.Empty,
            name,
            NormalizeNullable(model.Description),
            NormalizeNullable(model.RuleGroup),
            NormalizeChoice(model.RuleMode, SeoAuditRuleModes.All, SeoAuditRuleModes.Fixed),
            NormalizeChoice(model.RuleType, SeoAuditRuleTypes.All, SeoAuditRuleTypes.MissingField),
            NormalizeChoice(model.EntityType, SeoAuditEntityTypes.All, SeoAuditEntityTypes.GbpProfile),
            NormalizeChoice(model.Severity, SeoAuditSeverityLevels.All, SeoAuditSeverityLevels.Warning),
            model.WarningScoreImpact,
            model.FailScoreImpact,
            model.SortOrder,
            model.IsActive,
            model.ShowInActionsTab,
            NormalizeNullable(model.WhyItMattersText),
            NormalizeNullable(model.RecommendedActionText),
            parameters,
            nowUtc));
    }

    private static IReadOnlyList<SeoAuditRuleParameterUpsertRequest> NormalizeParameters(IEnumerable<SeoAuditRuleParameterEditModel> parameters)
    {
        var normalized = new List<SeoAuditRuleParameterUpsertRequest>();
        foreach (var parameter in parameters)
        {
            var name = NormalizeRequired(parameter.ParameterName);
            var value = NormalizeRequired(parameter.ParameterValue);
            if (name is null || value is null)
                continue;

            normalized.Add(new SeoAuditRuleParameterUpsertRequest(
                parameter.SeoAuditRuleParameterId,
                name,
                value,
                NormalizeChoice(parameter.ValueType, SeoAuditParameterValueTypes.All, SeoAuditParameterValueTypes.String),
                parameter.SortOrder,
                parameter.IsActive));
        }

        return normalized;
    }

    private static string NormalizeChoice(string? value, IReadOnlyList<string> allowedValues, string fallback)
    {
        var trimmed = NormalizeRequired(value);
        if (trimmed is null)
            return fallback;

        return allowedValues.FirstOrDefault(x => string.Equals(x, trimmed, StringComparison.OrdinalIgnoreCase)) ?? fallback;
    }

    private static string? NormalizeRequired(string? value)
    {
        var trimmed = (value ?? string.Empty).Trim();
        return trimmed.Length == 0 ? null : trimmed;
    }

    private static string? NormalizeNullable(string? value)
    {
        return NormalizeRequired(value);
    }
}
