using System.Globalization;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Dapper;
using LocalSeo.Web.Data;
using LocalSeo.Web.Models;
using LocalSeo.Web.Options;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Services;

public sealed class DataForSeoReviewsProvider(
    ISqlConnectionFactory connectionFactory,
    IHttpClientFactory httpClientFactory,
    IOptions<DataForSeoOptions> options,
    ILogger<DataForSeoReviewsProvider> logger) : IReviewsProvider, IDataForSeoTaskTracker
{
    public async Task FetchAndStoreReviewsAsync(
        string placeId,
        int? reviewCount,
        string? locationName,
        decimal? centerLat,
        decimal? centerLng,
        int? radiusMeters,
        CancellationToken ct)
    {
        var cfg = options.Value;
        if (string.IsNullOrWhiteSpace(cfg.Login) || string.IsNullOrWhiteSpace(cfg.Password))
        {
            await LogTaskFailureAsync(placeId, locationName, "DataForSEO credentials are not configured.", ct);
            logger.LogWarning("DataForSEO credentials are not configured; skipping review fetch for place {PlaceId}.", placeId);
            return;
        }

        TaskCreationResult createdTask;
        try
        {
            createdTask = await CreateTaskAsync(placeId, reviewCount, locationName, centerLat, centerLng, radiusMeters, cfg, ct);
        }
        catch (Exception ex)
        {
            await LogTaskFailureAsync(placeId, locationName, ex.Message, ct);
            throw;
        }

        if (string.IsNullOrWhiteSpace(createdTask.TaskId))
        {
            await LogTaskFailureAsync(placeId, locationName, createdTask.StatusMessage ?? "DataForSEO did not return a task id.", ct);
            logger.LogWarning("DataForSEO did not return a task id for place {PlaceId}.", placeId);
            return;
        }

        var endpoint = GetTaskGetPath(cfg, createdTask.TaskId);
        var status = createdTask.StatusCode >= 20000 && createdTask.StatusCode < 30000 ? "Created" : "Error";

        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(@"
MERGE dbo.DataForSeoReviewTask AS target
USING (SELECT @DataForSeoTaskId AS DataForSeoTaskId) AS source
ON target.DataForSeoTaskId = source.DataForSeoTaskId
WHEN MATCHED THEN UPDATE SET
  PlaceId=@PlaceId,
  LocationName=@LocationName,
  Status=@Status,
  TaskStatusCode=@TaskStatusCode,
  TaskStatusMessage=@TaskStatusMessage,
  Endpoint=@Endpoint,
  LastCheckedUtc=SYSUTCDATETIME(),
  CallbackReceivedAtUtc=CASE WHEN @CallbackReceivedAtUtc IS NULL THEN CallbackReceivedAtUtc ELSE @CallbackReceivedAtUtc END,
  CallbackTaskId=COALESCE(@CallbackTaskId, CallbackTaskId),
  LastError=@LastError
WHEN NOT MATCHED THEN
  INSERT(
    DataForSeoTaskId,PlaceId,LocationName,Status,TaskStatusCode,TaskStatusMessage,Endpoint,CreatedAtUtc,LastCheckedUtc,CallbackReceivedAtUtc,CallbackTaskId,LastError
  )
  VALUES(
    @DataForSeoTaskId,@PlaceId,@LocationName,@Status,@TaskStatusCode,@TaskStatusMessage,@Endpoint,SYSUTCDATETIME(),SYSUTCDATETIME(),@CallbackReceivedAtUtc,@CallbackTaskId,@LastError
  );",
            new
            {
                DataForSeoTaskId = createdTask.TaskId,
                PlaceId = placeId,
                LocationName = locationName,
                Status = status,
                TaskStatusCode = createdTask.StatusCode,
                TaskStatusMessage = createdTask.StatusMessage,
                Endpoint = endpoint,
                CallbackReceivedAtUtc = (DateTime?)null,
                CallbackTaskId = (string?)null,
                LastError = status == "Error" ? createdTask.StatusMessage : null
            }, cancellationToken: ct));

        logger.LogInformation(
            "DataForSEO task created for place {PlaceId}. TaskId={TaskId}, StatusCode={StatusCode}, StatusMessage={StatusMessage}",
            placeId,
            createdTask.TaskId,
            createdTask.StatusCode,
            createdTask.StatusMessage);

        try
        {
            var synced = await RefreshTaskStatusesAsync(ct);
            logger.LogInformation("DataForSEO tasks_ready sync after task_post touched {Count} row(s).", synced);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            logger.LogWarning(ex, "DataForSEO tasks_ready sync failed after task_post for task {TaskId}.", createdTask.TaskId);
        }
    }

    private async Task LogTaskFailureAsync(string placeId, string? locationName, string errorMessage, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var syntheticTaskId = $"err-{Guid.NewGuid():N}";
        await conn.ExecuteAsync(new CommandDefinition(@"
INSERT INTO dbo.DataForSeoReviewTask(
  DataForSeoTaskId, PlaceId, LocationName, Status, TaskStatusCode, TaskStatusMessage, CreatedAtUtc, LastCheckedUtc, LastError
)
VALUES(
  @DataForSeoTaskId, @PlaceId, @LocationName, 'Error', 0, @TaskStatusMessage, SYSUTCDATETIME(), SYSUTCDATETIME(), @LastError
);",
            new
            {
                DataForSeoTaskId = syntheticTaskId,
                PlaceId = placeId,
                LocationName = locationName,
                TaskStatusMessage = errorMessage,
                LastError = errorMessage
            }, cancellationToken: ct));
    }

    public async Task<IReadOnlyList<DataForSeoTaskRow>> GetLatestTasksAsync(int take, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<DataForSeoTaskRow>(new CommandDefinition(@"
SELECT TOP (@Take)
  DataForSeoReviewTaskId,
  DataForSeoTaskId,
  PlaceId,
  LocationName,
  Status,
  TaskStatusCode,
  TaskStatusMessage,
  Endpoint,
  CreatedAtUtc,
  LastCheckedUtc,
  ReadyAtUtc,
  PopulatedAtUtc,
  LastAttemptedPopulateUtc,
  LastPopulateReviewCount,
  CallbackReceivedAtUtc,
  CallbackTaskId,
  LastError
FROM dbo.DataForSeoReviewTask
ORDER BY DataForSeoReviewTaskId DESC;", new { Take = Math.Clamp(take, 1, 2000) }, cancellationToken: ct));
        return rows.ToList();
    }

    public async Task<int> RefreshTaskStatusesAsync(CancellationToken ct)
    {
        var cfg = options.Value;
        if (string.IsNullOrWhiteSpace(cfg.Login) || string.IsNullOrWhiteSpace(cfg.Password))
            return 0;

        var readyMap = await GetReadyTasksMapAsync(cfg, ct);
        var touched = 0;

        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var candidates = (await conn.QueryAsync<TaskTrackingCandidate>(new CommandDefinition(@"
SELECT DataForSeoReviewTaskId, DataForSeoTaskId, Status
FROM dbo.DataForSeoReviewTask
WHERE Status NOT IN ('Populated','CompletedNoReviews')
ORDER BY DataForSeoReviewTaskId DESC;", cancellationToken: ct))).ToList();
        var existingTaskIds = candidates.Select(x => x.DataForSeoTaskId).ToHashSet(StringComparer.Ordinal);

        foreach (var candidate in candidates)
        {
            ct.ThrowIfCancellationRequested();

            if (readyMap.TryGetValue(candidate.DataForSeoTaskId, out var ready))
            {
                var updated = await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.DataForSeoReviewTask
SET
  Status='Ready',
  Endpoint=COALESCE(@Endpoint, Endpoint),
  TaskStatusCode=COALESCE(@TaskStatusCode, TaskStatusCode),
  TaskStatusMessage=COALESCE(@TaskStatusMessage, TaskStatusMessage),
  LastCheckedUtc=SYSUTCDATETIME(),
  ReadyAtUtc=COALESCE(ReadyAtUtc, SYSUTCDATETIME()),
  LastError=NULL
WHERE DataForSeoReviewTaskId=@DataForSeoReviewTaskId;",
                    new
                    {
                        candidate.DataForSeoReviewTaskId,
                        ready.Endpoint,
                        TaskStatusCode = ready.StatusCode,
                        TaskStatusMessage = ready.StatusMessage
                    }, cancellationToken: ct));
                touched += updated;
                continue;
            }

            if (string.Equals(candidate.Status, "Created", StringComparison.OrdinalIgnoreCase)
                || string.Equals(candidate.Status, "Pending", StringComparison.OrdinalIgnoreCase))
            {
                touched += await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.DataForSeoReviewTask
SET
  Status='Pending',
  LastCheckedUtc=SYSUTCDATETIME()
WHERE DataForSeoReviewTaskId=@DataForSeoReviewTaskId;", new { candidate.DataForSeoReviewTaskId }, cancellationToken: ct));
            }
        }

        foreach (var item in readyMap)
        {
            ct.ThrowIfCancellationRequested();

            if (existingTaskIds.Contains(item.Key))
                continue;
            if (string.IsNullOrWhiteSpace(item.Value.Tag))
                continue;

            touched += await conn.ExecuteAsync(new CommandDefinition(@"
INSERT INTO dbo.DataForSeoReviewTask(
  DataForSeoTaskId, PlaceId, LocationName, Status, TaskStatusCode, TaskStatusMessage, Endpoint, CreatedAtUtc, LastCheckedUtc, ReadyAtUtc, LastError
)
SELECT
  @DataForSeoTaskId,
  p.PlaceId,
  p.SearchLocationName,
  'Ready',
  @TaskStatusCode,
  @TaskStatusMessage,
  @Endpoint,
  SYSUTCDATETIME(),
  SYSUTCDATETIME(),
  SYSUTCDATETIME(),
  NULL
FROM dbo.Place p
WHERE p.PlaceId=@PlaceId
  AND NOT EXISTS (SELECT 1 FROM dbo.DataForSeoReviewTask t WHERE t.DataForSeoTaskId=@DataForSeoTaskId);",
                new
                {
                    DataForSeoTaskId = item.Key,
                    PlaceId = item.Value.Tag,
                    TaskStatusCode = item.Value.StatusCode,
                    TaskStatusMessage = item.Value.StatusMessage,
                    item.Value.Endpoint
                }, cancellationToken: ct));
        }

        return touched;
    }

    public async Task<DataForSeoPopulateResult> PopulateTaskAsync(long dataForSeoReviewTaskId, CancellationToken ct)
    {
        var cfg = options.Value;
        if (string.IsNullOrWhiteSpace(cfg.Login) || string.IsNullOrWhiteSpace(cfg.Password))
            return new DataForSeoPopulateResult(false, "DataForSEO credentials are not configured.", 0);

        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var task = await conn.QuerySingleOrDefaultAsync<TaskPopulateTarget>(new CommandDefinition(@"
SELECT DataForSeoReviewTaskId, DataForSeoTaskId, PlaceId, Endpoint
FROM dbo.DataForSeoReviewTask
WHERE DataForSeoReviewTaskId=@Id;", new { Id = dataForSeoReviewTaskId }, cancellationToken: ct));

        if (task is null)
            return new DataForSeoPopulateResult(false, "Task record not found.", 0);

        var endpoint = string.IsNullOrWhiteSpace(task.Endpoint)
            ? GetTaskGetPath(cfg, task.DataForSeoTaskId)
            : task.Endpoint;

        var client = httpClientFactory.CreateClient();
        var snapshot = await GetTaskGetSnapshotAsync(client, BuildApiUrl(cfg.BaseUrl, endpoint), cfg, ct);

        if (snapshot.StatusCode >= 40000)
        {
            await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.DataForSeoReviewTask
SET
  Status='Error',
  TaskStatusCode=@TaskStatusCode,
  TaskStatusMessage=@TaskStatusMessage,
  LastCheckedUtc=SYSUTCDATETIME(),
  LastAttemptedPopulateUtc=SYSUTCDATETIME(),
  LastError=@TaskStatusMessage
WHERE DataForSeoReviewTaskId=@DataForSeoReviewTaskId;",
                new
                {
                    task.DataForSeoReviewTaskId,
                    TaskStatusCode = snapshot.StatusCode,
                    TaskStatusMessage = snapshot.StatusMessage
                }, cancellationToken: ct));

            return new DataForSeoPopulateResult(false, snapshot.StatusMessage ?? "Task failed.", 0);
        }

        if (snapshot.Reviews.Count == 0)
        {
            var status = snapshot.IsCompleted ? "CompletedNoReviews" : "Pending";
            await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.DataForSeoReviewTask
SET
  Status=@Status,
  TaskStatusCode=@TaskStatusCode,
  TaskStatusMessage=@TaskStatusMessage,
  LastCheckedUtc=SYSUTCDATETIME(),
  LastAttemptedPopulateUtc=SYSUTCDATETIME(),
  LastPopulateReviewCount=0,
  ReadyAtUtc=CASE WHEN @Status='CompletedNoReviews' THEN COALESCE(ReadyAtUtc, SYSUTCDATETIME()) ELSE ReadyAtUtc END,
  PopulatedAtUtc=CASE WHEN @Status='CompletedNoReviews' THEN SYSUTCDATETIME() ELSE PopulatedAtUtc END,
  LastError=NULL
WHERE DataForSeoReviewTaskId=@DataForSeoReviewTaskId;",
                new
                {
                    task.DataForSeoReviewTaskId,
                    Status = status,
                    TaskStatusCode = snapshot.StatusCode,
                    TaskStatusMessage = snapshot.StatusMessage
                }, cancellationToken: ct));

            var msg = snapshot.IsCompleted ? "Task completed but had no reviews to import." : "Task is not ready yet.";
            return new DataForSeoPopulateResult(snapshot.IsCompleted, msg, 0);
        }

        await using var tx = await conn.BeginTransactionAsync(ct);
        var upserted = await UpsertReviewsAsync(conn, tx, task.PlaceId, task.DataForSeoTaskId, snapshot.Reviews.Values, ct);

        await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.DataForSeoReviewTask
SET
  Status='Populated',
  TaskStatusCode=@TaskStatusCode,
  TaskStatusMessage=@TaskStatusMessage,
  LastCheckedUtc=SYSUTCDATETIME(),
  ReadyAtUtc=COALESCE(ReadyAtUtc, SYSUTCDATETIME()),
  PopulatedAtUtc=SYSUTCDATETIME(),
  LastAttemptedPopulateUtc=SYSUTCDATETIME(),
  LastPopulateReviewCount=@LastPopulateReviewCount,
  LastError=NULL
WHERE DataForSeoReviewTaskId=@DataForSeoReviewTaskId;",
            new
            {
                task.DataForSeoReviewTaskId,
                TaskStatusCode = snapshot.StatusCode,
                TaskStatusMessage = snapshot.StatusMessage,
                LastPopulateReviewCount = upserted
            }, tx, cancellationToken: ct));

        await tx.CommitAsync(ct);

        return new DataForSeoPopulateResult(true, $"Upserted {upserted} reviews.", upserted);
    }

    public async Task<DataForSeoPostbackResult> HandlePostbackAsync(string? taskIdFromQuery, string? tagFromQuery, string payloadJson, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(payloadJson))
            return new DataForSeoPostbackResult(false, "Empty payload.");

        var summary = ParsePostbackSummary(payloadJson);
        var taskId = !string.IsNullOrWhiteSpace(taskIdFromQuery) ? taskIdFromQuery : summary.TaskId;
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        if (string.IsNullOrWhiteSpace(taskId) && !string.IsNullOrWhiteSpace(tagFromQuery))
        {
            taskId = await conn.QuerySingleOrDefaultAsync<string>(new CommandDefinition(@"
SELECT TOP 1 DataForSeoTaskId
FROM dbo.DataForSeoReviewTask
WHERE PlaceId=@PlaceId AND Status IN ('Created','Pending','Ready')
ORDER BY DataForSeoReviewTaskId DESC;", new { PlaceId = tagFromQuery }, cancellationToken: ct));
        }
        if (string.IsNullOrWhiteSpace(taskId))
            return new DataForSeoPostbackResult(false, "Task id missing in callback.");

        var statusCode = summary.TaskStatusCode;
        var statusMessage = summary.TaskStatusMessage;
        var status = !statusCode.HasValue
            ? "Pending"
            : statusCode is >= 20000 and < 30000 ? "Ready" : "Error";
        var endpoint = summary.Endpoint;

        var affected = await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.DataForSeoReviewTask
SET
  Status=@Status,
  TaskStatusCode=@TaskStatusCode,
  TaskStatusMessage=@TaskStatusMessage,
  Endpoint=COALESCE(@Endpoint, Endpoint),
  LastCheckedUtc=SYSUTCDATETIME(),
  ReadyAtUtc=CASE WHEN @Status='Ready' THEN COALESCE(ReadyAtUtc, SYSUTCDATETIME()) ELSE ReadyAtUtc END,
  CallbackReceivedAtUtc=SYSUTCDATETIME(),
  CallbackTaskId=@CallbackTaskId,
  LastError=CASE WHEN @Status='Error' THEN @TaskStatusMessage ELSE NULL END
WHERE DataForSeoTaskId=@DataForSeoTaskId;",
            new
            {
                Status = status,
                TaskStatusCode = statusCode,
                TaskStatusMessage = statusMessage,
                Endpoint = endpoint,
                CallbackTaskId = taskId,
                DataForSeoTaskId = taskId
            }, cancellationToken: ct));

        if (affected == 0)
        {
            logger.LogWarning("DataForSEO callback received for unknown task {TaskId}. Tag={Tag}", taskId, tagFromQuery);
            return new DataForSeoPostbackResult(false, $"Task {taskId} not found.");
        }

        logger.LogInformation(
            "DataForSEO callback recorded for task {TaskId}. StatusCode={StatusCode}, StatusMessage={StatusMessage}, Endpoint={Endpoint}",
            taskId,
            statusCode,
            statusMessage,
            endpoint);
        if (string.Equals(status, "Ready", StringComparison.OrdinalIgnoreCase))
        {
            var rowId = await conn.QuerySingleOrDefaultAsync<long?>(new CommandDefinition(@"
SELECT TOP 1 DataForSeoReviewTaskId
FROM dbo.DataForSeoReviewTask
WHERE DataForSeoTaskId=@DataForSeoTaskId;", new { DataForSeoTaskId = taskId }, cancellationToken: ct));

            if (rowId.HasValue)
            {
                var populateResult = await PopulateTaskAsync(rowId.Value, ct);
                return new DataForSeoPostbackResult(
                    populateResult.Success,
                    $"Task {taskId} callback received. {populateResult.Message}");
            }
        }

        return new DataForSeoPostbackResult(true, $"Task {taskId} updated to {status}.");
    }

    private async Task<int> UpsertReviewsAsync(
        Microsoft.Data.SqlClient.SqlConnection conn,
        System.Data.Common.DbTransaction tx,
        string placeId,
        string sourceTaskId,
        IEnumerable<ReviewPayload> reviews,
        CancellationToken ct)
    {
        var count = 0;
        foreach (var review in reviews)
        {
            await conn.ExecuteAsync(new CommandDefinition(@"
MERGE dbo.PlaceReview AS target
USING (SELECT @PlaceId AS PlaceId, @ReviewId AS ReviewId) AS source
ON target.PlaceId = source.PlaceId AND target.ReviewId = source.ReviewId
WHEN MATCHED THEN UPDATE SET
  ReviewUrl=@ReviewUrl,
  ProfileName=@ProfileName,
  ProfileUrl=@ProfileUrl,
  ProfileImageUrl=@ProfileImageUrl,
  ReviewText=@ReviewText,
  OriginalReviewText=@OriginalReviewText,
  OriginalLanguage=@OriginalLanguage,
  Rating=@Rating,
  ReviewsCount=@ReviewsCount,
  PhotosCount=@PhotosCount,
  LocalGuide=@LocalGuide,
  TimeAgo=@TimeAgo,
  ReviewTimestampUtc=@ReviewTimestampUtc,
  OwnerAnswer=@OwnerAnswer,
  OriginalOwnerAnswer=@OriginalOwnerAnswer,
  OwnerTimeAgo=@OwnerTimeAgo,
  OwnerTimestampUtc=@OwnerTimestampUtc,
  SourceTaskId=@SourceTaskId,
  RawJson=@RawJson,
  LastSeenUtc=SYSUTCDATETIME()
WHEN NOT MATCHED THEN
  INSERT(
    PlaceId,ReviewId,ReviewUrl,ProfileName,ProfileUrl,ProfileImageUrl,ReviewText,OriginalReviewText,OriginalLanguage,Rating,ReviewsCount,PhotosCount,LocalGuide,TimeAgo,ReviewTimestampUtc,OwnerAnswer,OriginalOwnerAnswer,OwnerTimeAgo,OwnerTimestampUtc,SourceTaskId,RawJson,FirstSeenUtc,LastSeenUtc
  )
  VALUES(
    @PlaceId,@ReviewId,@ReviewUrl,@ProfileName,@ProfileUrl,@ProfileImageUrl,@ReviewText,@OriginalReviewText,@OriginalLanguage,@Rating,@ReviewsCount,@PhotosCount,@LocalGuide,@TimeAgo,@ReviewTimestampUtc,@OwnerAnswer,@OriginalOwnerAnswer,@OwnerTimeAgo,@OwnerTimestampUtc,@SourceTaskId,@RawJson,SYSUTCDATETIME(),SYSUTCDATETIME()
  );",
                new
                {
                    PlaceId = placeId,
                    review.ReviewId,
                    review.ReviewUrl,
                    review.ProfileName,
                    review.ProfileUrl,
                    review.ProfileImageUrl,
                    review.ReviewText,
                    review.OriginalReviewText,
                    review.OriginalLanguage,
                    review.Rating,
                    review.ReviewsCount,
                    review.PhotosCount,
                    review.LocalGuide,
                    review.TimeAgo,
                    review.ReviewTimestampUtc,
                    review.OwnerAnswer,
                    review.OriginalOwnerAnswer,
                    review.OwnerTimeAgo,
                    review.OwnerTimestampUtc,
                    SourceTaskId = sourceTaskId,
                    review.RawJson
                }, tx, cancellationToken: ct));
            count++;
        }

        return count;
    }

    private async Task<TaskCreationResult> CreateTaskAsync(
        string placeId,
        int? reviewCount,
        string? locationName,
        decimal? centerLat,
        decimal? centerLng,
        int? radiusMeters,
        DataForSeoOptions cfg,
        CancellationToken ct)
    {
        var depth = Math.Max(1, reviewCount ?? cfg.Depth);
        var requestItem = new Dictionary<string, object?>
        {
            ["keyword"] = $"place_id:{placeId}",
            ["depth"] = depth
        };
        if (!string.IsNullOrWhiteSpace(cfg.LanguageCode))
            requestItem["language_code"] = cfg.LanguageCode.Trim();

        if (!string.IsNullOrWhiteSpace(locationName))
        {
            requestItem["location_name"] = locationName.Trim();
        }
        else if (centerLat.HasValue && centerLng.HasValue)
        {
            var radius = Math.Max(200, radiusMeters ?? 5000);
            requestItem["location_coordinate"] = FormattableString.Invariant(
                $"{centerLat.Value:F6},{centerLng.Value:F6},{radius}");
        }
        if (!string.IsNullOrWhiteSpace(cfg.PostbackUrl))
            requestItem["postback_url"] = cfg.PostbackUrl.Trim();
        requestItem["tag"] = placeId;

        var requestPayload = new[] { requestItem };

        using var request = CreateRequest(HttpMethod.Post, BuildApiUrl(cfg.BaseUrl, cfg.TaskPostPath), cfg);
        request.Content = JsonContent.Create(requestPayload);

        var client = httpClientFactory.CreateClient();
        using var response = await client.SendAsync(request, ct);
        var body = await response.Content.ReadAsStringAsync(ct);
        if (!response.IsSuccessStatusCode)
            throw new HttpRequestException($"DataForSEO task_post failed with status {(int)response.StatusCode}: {body}");

        using var document = JsonDocument.Parse(body);
        var root = document.RootElement;
        if (!root.TryGetProperty("tasks", out var tasks) || tasks.ValueKind != JsonValueKind.Array || tasks.GetArrayLength() == 0)
            return new TaskCreationResult(null, 0, "No tasks in response.");

        var task = tasks[0];
        var taskStatusCode = task.TryGetProperty("status_code", out var statusCodeNode) && statusCodeNode.TryGetInt32(out var code)
            ? code
            : 0;
        var taskStatusMessage = task.TryGetProperty("status_message", out var statusMessageNode)
            ? statusMessageNode.GetString()
            : null;
        var taskId = task.TryGetProperty("id", out var idNode) ? idNode.GetString() : null;

        return new TaskCreationResult(taskId, taskStatusCode, taskStatusMessage);
    }

    private async Task<Dictionary<string, ReadyTaskInfo>> GetReadyTasksMapAsync(DataForSeoOptions cfg, CancellationToken ct)
    {
        using var request = CreateRequest(HttpMethod.Get, BuildApiUrl(cfg.BaseUrl, cfg.TasksReadyPath), cfg);
        var client = httpClientFactory.CreateClient();
        using var response = await client.SendAsync(request, ct);
        var body = await response.Content.ReadAsStringAsync(ct);
        if (!response.IsSuccessStatusCode)
            throw new HttpRequestException($"DataForSEO tasks_ready failed with status {(int)response.StatusCode}: {body}");

        var map = new Dictionary<string, ReadyTaskInfo>(StringComparer.Ordinal);
        using var document = JsonDocument.Parse(body);
        var root = document.RootElement;
        if (!root.TryGetProperty("status_code", out var statusCodeNode) || !statusCodeNode.TryGetInt32(out var topStatus) || topStatus != 20000)
            return map;
        if (!root.TryGetProperty("tasks", out var tasks) || tasks.ValueKind != JsonValueKind.Array)
            return map;

        foreach (var task in tasks.EnumerateArray())
        {
            var taskStatusCode = task.TryGetProperty("status_code", out var childStatusCodeNode) && childStatusCodeNode.TryGetInt32(out var childStatusCode)
                ? childStatusCode
                : (int?)null;
            var taskStatusMessage = task.TryGetProperty("status_message", out var childStatusMessageNode)
                ? childStatusMessageNode.GetString()
                : null;

            if (!task.TryGetProperty("result", out var resultArray) || resultArray.ValueKind != JsonValueKind.Array)
                continue;

            foreach (var readyItem in resultArray.EnumerateArray())
            {
                var id = GetString(readyItem, "id");
                if (string.IsNullOrWhiteSpace(id))
                    continue;

                map[id] = new ReadyTaskInfo(
                    GetString(readyItem, "endpoint"),
                    taskStatusCode,
                    taskStatusMessage,
                    GetString(readyItem, "tag"));
            }
        }

        return map;
    }

    private async Task<TaskGetSnapshot> GetTaskGetSnapshotAsync(
        HttpClient client,
        string url,
        DataForSeoOptions cfg,
        CancellationToken ct)
    {
        using var request = CreateRequest(HttpMethod.Get, url, cfg);
        using var response = await client.SendAsync(request, ct);
        var body = await response.Content.ReadAsStringAsync(ct);
        if (!response.IsSuccessStatusCode)
            throw new HttpRequestException($"DataForSEO task_get failed with status {(int)response.StatusCode}: {body}");

        return ParseTaskGetSnapshot(body);
    }

    private static TaskGetSnapshot ParseTaskGetSnapshot(string body)
    {
        var reviews = new Dictionary<string, ReviewPayload>(StringComparer.Ordinal);
        using var document = JsonDocument.Parse(body);
        var root = document.RootElement;
        if (!root.TryGetProperty("tasks", out var tasks) || tasks.ValueKind != JsonValueKind.Array || tasks.GetArrayLength() == 0)
            return new TaskGetSnapshot(0, "No tasks in response.", false, reviews);

        var task = tasks[0];
        var taskStatusCode = task.TryGetProperty("status_code", out var statusCodeNode) && statusCodeNode.TryGetInt32(out var code)
            ? code
            : 0;
        var taskStatusMessage = task.TryGetProperty("status_message", out var statusMessageNode)
            ? statusMessageNode.GetString()
            : null;
        var resultCount = task.TryGetProperty("result_count", out var resultCountNode) && resultCountNode.TryGetInt32(out var cnt)
            ? cnt
            : 0;

        if (task.TryGetProperty("result", out var resultArray) && resultArray.ValueKind == JsonValueKind.Array)
        {
            foreach (var result in resultArray.EnumerateArray())
            {
                foreach (var item in EnumerateReviewCandidates(result))
                {
                    var payload = ParseReview(item);
                    if (payload is null)
                        continue;
                    reviews[payload.ReviewId] = payload;
                }
            }
        }

        var isCompleted = taskStatusCode == 20000 && resultCount >= 0;
        return new TaskGetSnapshot(taskStatusCode, taskStatusMessage, isCompleted, reviews);
    }

    private static IEnumerable<JsonElement> EnumerateReviewCandidates(JsonElement result)
    {
        if (result.TryGetProperty("items", out var items) && items.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in items.EnumerateArray())
                yield return item;
        }

        if (result.TryGetProperty("reviews", out var reviews) && reviews.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in reviews.EnumerateArray())
                yield return item;
        }
    }

    private static PostbackSummary ParsePostbackSummary(string payloadJson)
    {
        using var document = JsonDocument.Parse(payloadJson);
        var root = document.RootElement;

        if (!root.TryGetProperty("tasks", out var tasks) || tasks.ValueKind != JsonValueKind.Array || tasks.GetArrayLength() == 0)
            return new PostbackSummary(null, null, "No tasks in callback payload.", null);

        var task = tasks[0];
        var taskId = task.TryGetProperty("id", out var idNode) ? idNode.GetString() : null;
        var statusCode = task.TryGetProperty("status_code", out var statusCodeNode) && statusCodeNode.TryGetInt32(out var code)
            ? code
            : (int?)null;
        var statusMessage = task.TryGetProperty("status_message", out var statusMessageNode)
            ? statusMessageNode.GetString()
            : null;

        string? endpoint = null;
        if (task.TryGetProperty("result", out var resultArray) && resultArray.ValueKind == JsonValueKind.Array && resultArray.GetArrayLength() > 0)
        {
            var first = resultArray[0];
            endpoint = GetString(first, "endpoint");
            taskId ??= GetString(first, "id");
        }

        return new PostbackSummary(taskId, statusCode, statusMessage, endpoint);
    }

    private HttpRequestMessage CreateRequest(HttpMethod method, string url, DataForSeoOptions cfg)
    {
        var request = new HttpRequestMessage(method, url);
        var credentials = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{cfg.Login}:{cfg.Password}"));
        request.Headers.Authorization = new AuthenticationHeaderValue("Basic", credentials);
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        return request;
    }

    private static string TrimTrailingSlash(string? baseUrl)
        => (baseUrl ?? string.Empty).Trim().TrimEnd('/');

    private static string BuildApiUrl(string? baseUrl, string urlOrPath)
    {
        if (Uri.TryCreate(urlOrPath, UriKind.Absolute, out var absolute))
            return absolute.ToString();
        return $"{TrimTrailingSlash(baseUrl)}/{urlOrPath.TrimStart('/')}";
    }

    private static string GetTaskGetPath(DataForSeoOptions cfg, string taskId)
    {
        var format = string.IsNullOrWhiteSpace(cfg.TaskGetPathFormat)
            ? "/v3/business_data/google/reviews/task_get/{0}"
            : cfg.TaskGetPathFormat;
        return string.Format(CultureInfo.InvariantCulture, format, taskId);
    }

    private static ReviewPayload? ParseReview(JsonElement item)
    {
        var reviewId = GetString(item, "review_id");
        if (string.IsNullOrWhiteSpace(reviewId))
            return null;

        return new ReviewPayload(
            reviewId,
            GetString(item, "review_url"),
            GetString(item, "profile_name"),
            GetString(item, "profile_url"),
            GetString(item, "profile_image_url"),
            GetString(item, "review_text"),
            GetString(item, "original_review_text"),
            GetString(item, "original_language"),
            GetDecimal(item, "rating", "value"),
            GetInt(item, "reviews_count"),
            GetInt(item, "photos_count"),
            GetBool(item, "local_guide"),
            GetString(item, "time_ago"),
            ParseTimestamp(GetString(item, "timestamp")),
            GetString(item, "owner_answer"),
            GetString(item, "original_owner_answer"),
            GetString(item, "owner_time_ago"),
            ParseTimestamp(GetString(item, "owner_timestamp")),
            item.GetRawText());
    }

    private static string? GetString(JsonElement obj, string property)
    {
        if (!obj.TryGetProperty(property, out var node) || node.ValueKind == JsonValueKind.Null)
            return null;
        return node.GetString();
    }

    private static int? GetInt(JsonElement obj, string property)
    {
        if (!obj.TryGetProperty(property, out var node) || node.ValueKind == JsonValueKind.Null)
            return null;
        return node.TryGetInt32(out var value) ? value : null;
    }

    private static bool? GetBool(JsonElement obj, string property)
    {
        if (!obj.TryGetProperty(property, out var node) || node.ValueKind == JsonValueKind.Null)
            return null;
        if (node.ValueKind == JsonValueKind.True || node.ValueKind == JsonValueKind.False)
            return node.GetBoolean();
        return null;
    }

    private static decimal? GetDecimal(JsonElement obj, string property, string nestedProperty)
    {
        if (!obj.TryGetProperty(property, out var node) || node.ValueKind != JsonValueKind.Object)
            return null;
        if (!node.TryGetProperty(nestedProperty, out var nested) || nested.ValueKind == JsonValueKind.Null)
            return null;
        return nested.TryGetDecimal(out var value) ? value : null;
    }

    private static DateTime? ParseTimestamp(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;
        if (!DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var dto))
            return null;
        return dto.UtcDateTime;
    }

    private sealed record TaskCreationResult(string? TaskId, int StatusCode, string? StatusMessage);

    private sealed record ReadyTaskInfo(string? Endpoint, int? StatusCode, string? StatusMessage, string? Tag);

    private sealed record TaskTrackingCandidate(long DataForSeoReviewTaskId, string DataForSeoTaskId, string Status);

    private sealed record TaskPopulateTarget(long DataForSeoReviewTaskId, string DataForSeoTaskId, string PlaceId, string? Endpoint);

    private sealed record ReviewPayload(
        string ReviewId,
        string? ReviewUrl,
        string? ProfileName,
        string? ProfileUrl,
        string? ProfileImageUrl,
        string? ReviewText,
        string? OriginalReviewText,
        string? OriginalLanguage,
        decimal? Rating,
        int? ReviewsCount,
        int? PhotosCount,
        bool? LocalGuide,
        string? TimeAgo,
        DateTime? ReviewTimestampUtc,
        string? OwnerAnswer,
        string? OriginalOwnerAnswer,
        string? OwnerTimeAgo,
        DateTime? OwnerTimestampUtc,
        string RawJson);

    private sealed record TaskGetSnapshot(
        int StatusCode,
        string? StatusMessage,
        bool IsCompleted,
        Dictionary<string, ReviewPayload> Reviews);

    private sealed record PostbackSummary(
        string? TaskId,
        int? TaskStatusCode,
        string? TaskStatusMessage,
        string? Endpoint);
}
