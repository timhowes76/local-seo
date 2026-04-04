using System.ComponentModel.DataAnnotations;

namespace LocalSeo.Web.Models;

public sealed class CloudflareWorkerListRowModel
{
    public int CloudflareWorkerId { get; init; }
    public string Name { get; init; } = string.Empty;
    public string WorkerKey { get; init; } = string.Empty;
    public string BaseUrl { get; init; } = string.Empty;
    public string RoutePath { get; init; } = string.Empty;
    public bool IsEnabled { get; init; }
    public int TimeoutSeconds { get; init; }
    public int DisplayOrder { get; init; }
    public DateTime UpdatedUtc { get; init; }
}

public sealed class CloudflareWorkerListViewModel
{
    public string? Search { get; init; }
    public string? Message { get; init; }
    public IReadOnlyList<CloudflareWorkerListRowModel> Rows { get; init; } = [];
}

public sealed class CloudflareWorkerEditModel
{
    public int CloudflareWorkerId { get; set; }

    [Required]
    [StringLength(200)]
    public string Name { get; set; } = string.Empty;

    [Required]
    [StringLength(200)]
    public string WorkerKey { get; set; } = string.Empty;

    [StringLength(1000)]
    public string BaseUrl { get; set; } = string.Empty;

    [Required]
    [StringLength(500)]
    public string RoutePath { get; set; } = string.Empty;

    [StringLength(200)]
    public string? AuthHeaderName { get; set; }

    [StringLength(1000)]
    public string? AuthToken { get; set; }

    public string? AuthTokenMasked { get; set; }

    [Range(1, 300)]
    public int TimeoutSeconds { get; set; } = 30;

    public bool IsEnabled { get; set; } = true;

    [Range(0, 100000)]
    public int DisplayOrder { get; set; }

    [StringLength(2000)]
    public string? Notes { get; set; }
}

public sealed class CloudflareWorkerEditViewModel
{
    public string Mode { get; init; } = "create";
    public string? Message { get; init; }
    public CloudflareWorkerEditModel Worker { get; init; } = new();
}

public sealed record CloudflareWorkerRuntimeModel(
    int CloudflareWorkerId,
    string Name,
    string WorkerKey,
    string BaseUrl,
    string RoutePath,
    string? AuthHeaderName,
    string? AuthToken,
    int TimeoutSeconds,
    bool IsEnabled,
    int DisplayOrder,
    string? Notes,
    DateTime CreatedUtc,
    DateTime UpdatedUtc);
