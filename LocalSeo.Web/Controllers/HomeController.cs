using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using LocalSeo.Web.Models;
using LocalSeo.Web.Services;

namespace LocalSeo.Web.Controllers;

public class HomeController(
    IDataForSeoAccountStatusService dataForSeoAccountStatusService,
    IApiStatusService apiStatusService) : Controller
{
    private const string DataForSeoWidgetKey = "dataforseo.appendix.status";
    private const string DataForSeoWidgetDisplayName = "DataForSEO";
    private const string DataForSeoWidgetCategory = "DataForSEO";

    [AllowAnonymous]
    public async Task<IActionResult> Index(CancellationToken ct)
    {
        if (User.Identity?.IsAuthenticated != true)
            return RedirectToAction("Index", "Login");

        var model = await dataForSeoAccountStatusService.GetDashboardAsync(ct);
        var apiStatusSnapshot = await apiStatusService.GetDashboardSnapshotAsync(ct);
        model.ApiStatusRetrievedAtUtc = MaxUtc(model.RetrievedAtUtc, apiStatusSnapshot.RetrievedUtc);
        model.ApiStatusWidgets = BuildApiStatusWidgets(model.ApiStatuses, model.RetrievedAtUtc, apiStatusSnapshot.Widgets);
        return View(model);
    }

    [AllowAnonymous]
    public IActionResult Error() => View();

    private static IReadOnlyList<ApiStatusWidgetModel> BuildApiStatusWidgets(
        IReadOnlyList<DataForSeoApiStatusRow> dataForSeoStatuses,
        DateTime dataForSeoCheckedUtc,
        IReadOnlyList<ApiStatusWidgetModel> apiStatusWidgets)
    {
        var widgets = new List<ApiStatusWidgetModel>(apiStatusWidgets.Count + 1)
        {
            BuildDataForSeoWidget(dataForSeoStatuses, dataForSeoCheckedUtc)
        };

        widgets.AddRange(apiStatusWidgets);
        return widgets;
    }

    private static ApiStatusWidgetModel BuildDataForSeoWidget(
        IReadOnlyList<DataForSeoApiStatusRow> statuses,
        DateTime checkedUtc)
    {
        var upCount = 0;
        var degradedCount = 0;
        var downCount = 0;
        var unknownCount = 0;

        foreach (var row in statuses)
        {
            switch (MapEndpointStatus(row))
            {
                case ApiHealthStatus.Up:
                    upCount++;
                    break;
                case ApiHealthStatus.Degraded:
                    degradedCount++;
                    break;
                case ApiHealthStatus.Down:
                    downCount++;
                    break;
                default:
                    unknownCount++;
                    break;
            }
        }

        var total = statuses.Count;
        var overallStatus = ResolveOverallStatus(upCount, degradedCount, downCount, unknownCount);
        var message = BuildSummaryMessage(total, upCount, degradedCount, downCount, unknownCount);

        return new ApiStatusWidgetModel(
            0,
            DataForSeoWidgetKey,
            DataForSeoWidgetDisplayName,
            DataForSeoWidgetCategory,
            true,
            overallStatus,
            checkedUtc > DateTime.MinValue ? checkedUtc : null,
            null,
            message);
    }

    private static ApiHealthStatus ResolveOverallStatus(int upCount, int degradedCount, int downCount, int unknownCount)
    {
        if (downCount > 0)
            return ApiHealthStatus.Down;
        if (degradedCount > 0)
            return ApiHealthStatus.Degraded;
        if (upCount == 0 && unknownCount > 0)
            return ApiHealthStatus.Unknown;
        if (unknownCount > 0)
            return ApiHealthStatus.Degraded;
        return ApiHealthStatus.Up;
    }

    private static string BuildSummaryMessage(int total, int upCount, int degradedCount, int downCount, int unknownCount)
    {
        if (total <= 0)
            return "No DataForSEO endpoint status data is available.";

        if (upCount == total)
            return $"All {total} DataForSEO endpoints are operational.";

        var parts = new List<string> { $"{upCount}/{total} endpoints up" };
        if (degradedCount > 0)
            parts.Add($"{degradedCount} degraded");
        if (downCount > 0)
            parts.Add($"{downCount} down");
        if (unknownCount > 0)
            parts.Add($"{unknownCount} unknown");

        return string.Join(", ", parts) + ".";
    }

    private static ApiHealthStatus MapEndpointStatus(DataForSeoApiStatusRow row)
    {
        if (row.StatusCode is >= 200 and < 300)
            return ApiHealthStatus.Up;
        if (row.StatusCode is >= 500)
            return ApiHealthStatus.Down;
        if (row.StatusCode is >= 400 and < 500)
            return ApiHealthStatus.Degraded;

        var statusText = (row.StatusLabel ?? string.Empty).Trim().ToLowerInvariant();
        if (statusText.Contains("ok") || statusText.Contains("online") || statusText.Contains("operational"))
            return ApiHealthStatus.Up;
        if (statusText.Contains("degraded") || statusText.Contains("pending") || statusText.Contains("processing") || statusText.Contains("timeout"))
            return ApiHealthStatus.Degraded;
        if (statusText.Contains("error") || statusText.Contains("down") || statusText.Contains("unavailable") || statusText.Contains("offline"))
            return ApiHealthStatus.Down;

        return ApiHealthStatus.Unknown;
    }

    private static DateTime MaxUtc(DateTime first, DateTime second)
    {
        if (first <= DateTime.MinValue)
            return second;
        if (second <= DateTime.MinValue)
            return first;
        return first >= second ? first : second;
    }
}
