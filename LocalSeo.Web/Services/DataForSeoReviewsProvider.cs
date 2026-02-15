using System.Globalization;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
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
    IReviewVelocityService reviewVelocityService,
    ILogger<DataForSeoReviewsProvider> logger) : IReviewsProvider, IDataForSeoTaskTracker
{
    private const string TaskTypeReviews = "reviews";
    private const string TaskTypeMyBusinessInfo = "my_business_info";

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
            await LogTaskFailureAsync(placeId, locationName, TaskTypeReviews, "DataForSEO credentials are not configured.", ct);
            await LogTaskFailureAsync(placeId, locationName, TaskTypeMyBusinessInfo, "DataForSEO credentials are not configured.", ct);
            logger.LogWarning("DataForSEO credentials are not configured; skipping review fetch for place {PlaceId}.", placeId);
            return;
        }

        try
        {
            await CreateAndTrackTaskAsync(
                placeId,
                locationName,
                TaskTypeReviews,
                () => CreateReviewsTaskAsync(placeId, reviewCount, locationName, centerLat, centerLng, radiusMeters, cfg, ct),
                ct);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            await LogTaskFailureAsync(placeId, locationName, TaskTypeReviews, ex.Message, ct);
            logger.LogWarning(ex, "DataForSEO reviews task creation failed for place {PlaceId}.", placeId);
        }

        try
        {
            await CreateAndTrackTaskAsync(
                placeId,
                locationName,
                TaskTypeMyBusinessInfo,
                () => CreateMyBusinessInfoTaskAsync(placeId, locationName, cfg, ct),
                ct);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            await LogTaskFailureAsync(placeId, locationName, TaskTypeMyBusinessInfo, ex.Message, ct);
            logger.LogWarning(ex, "DataForSEO my_business_info task creation failed for place {PlaceId}.", placeId);
        }

        try
        {
            var synced = await RefreshTaskStatusesAsync(ct);
            logger.LogInformation("DataForSEO tasks_ready sync after task_post touched {Count} row(s).", synced);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            logger.LogWarning(ex, "DataForSEO tasks_ready sync failed after task_post for place {PlaceId}.", placeId);
        }
    }

    private async Task CreateAndTrackTaskAsync(
        string placeId,
        string? locationName,
        string taskType,
        Func<Task<TaskCreationResult>> createTask,
        CancellationToken ct)
    {
        var createdTask = await createTask();
        if (string.IsNullOrWhiteSpace(createdTask.TaskId))
        {
            await LogTaskFailureAsync(placeId, locationName, taskType, createdTask.StatusMessage ?? "DataForSEO did not return a task id.", ct);
            logger.LogWarning("DataForSEO did not return a task id for place {PlaceId}. TaskType={TaskType}", placeId, taskType);
            return;
        }

        var status = createdTask.StatusCode is >= 20000 and < 30000 ? "Created" : "Error";
        var endpoint = GetTaskGetPath(options.Value, createdTask.TaskId, taskType);

        await UpsertTaskRowAsync(
            placeId,
            locationName,
            taskType,
            createdTask.TaskId,
            status,
            createdTask.StatusCode,
            createdTask.StatusMessage,
            endpoint,
            status == "Error" ? createdTask.StatusMessage : null,
            ct);

        logger.LogInformation(
            "DataForSEO task created for place {PlaceId}. TaskType={TaskType}, TaskId={TaskId}, StatusCode={StatusCode}, StatusMessage={StatusMessage}",
            placeId,
            taskType,
            createdTask.TaskId,
            createdTask.StatusCode,
            createdTask.StatusMessage);
    }

    private async Task LogTaskFailureAsync(string placeId, string? locationName, string taskType, string errorMessage, CancellationToken ct)
    {
        var syntheticTaskId = $"{taskType}-err-{Guid.NewGuid():N}";
        await UpsertTaskRowAsync(
            placeId,
            locationName,
            taskType,
            syntheticTaskId,
            "Error",
            0,
            errorMessage,
            null,
            errorMessage,
            ct);
    }

    private async Task UpsertTaskRowAsync(
        string placeId,
        string? locationName,
        string taskType,
        string dataForSeoTaskId,
        string status,
        int? taskStatusCode,
        string? taskStatusMessage,
        string? endpoint,
        string? lastError,
        CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(@"
MERGE dbo.DataForSeoReviewTask AS target
USING (SELECT @DataForSeoTaskId AS DataForSeoTaskId) AS source
ON target.DataForSeoTaskId = source.DataForSeoTaskId
WHEN MATCHED THEN UPDATE SET
  TaskType=@TaskType,
  PlaceId=@PlaceId,
  LocationName=@LocationName,
  Status=@Status,
  TaskStatusCode=@TaskStatusCode,
  TaskStatusMessage=@TaskStatusMessage,
  Endpoint=@Endpoint,
  LastCheckedUtc=SYSUTCDATETIME(),
  LastError=@LastError
WHEN NOT MATCHED THEN
  INSERT(
    DataForSeoTaskId,TaskType,PlaceId,LocationName,Status,TaskStatusCode,TaskStatusMessage,Endpoint,CreatedAtUtc,LastCheckedUtc,LastError
  )
  VALUES(
    @DataForSeoTaskId,@TaskType,@PlaceId,@LocationName,@Status,@TaskStatusCode,@TaskStatusMessage,@Endpoint,SYSUTCDATETIME(),SYSUTCDATETIME(),@LastError
  );",
            new
            {
                DataForSeoTaskId = dataForSeoTaskId,
                TaskType = taskType,
                PlaceId = placeId,
                LocationName = locationName,
                Status = status,
                TaskStatusCode = taskStatusCode,
                TaskStatusMessage = taskStatusMessage,
                Endpoint = endpoint,
                LastError = lastError
            }, cancellationToken: ct));
    }

    public async Task<IReadOnlyList<DataForSeoTaskRow>> GetLatestTasksAsync(int take, string? taskType, CancellationToken ct)
    {
        var normalizedTaskType = string.IsNullOrWhiteSpace(taskType) || string.Equals(taskType, "all", StringComparison.OrdinalIgnoreCase)
            ? null
            : NormalizeTaskType(taskType);
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<DataForSeoTaskRow>(new CommandDefinition(@"
SELECT TOP (@Take)
  DataForSeoReviewTaskId,
  DataForSeoTaskId,
  TaskType,
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
WHERE @TaskType IS NULL OR COALESCE(TaskType, 'reviews') = @TaskType
ORDER BY
  CASE WHEN COALESCE(TaskType, 'reviews') = 'my_business_info' THEN 0 ELSE 1 END,
  DataForSeoReviewTaskId DESC;",
            new { Take = Math.Clamp(take, 1, 2000), TaskType = normalizedTaskType }, cancellationToken: ct));
        return rows.ToList();
    }

    public async Task<int> DeleteErrorTasksAsync(string? taskType, CancellationToken ct)
    {
        var normalizedTaskType = string.IsNullOrWhiteSpace(taskType) || string.Equals(taskType, "all", StringComparison.OrdinalIgnoreCase)
            ? null
            : NormalizeTaskType(taskType);

        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.ExecuteAsync(new CommandDefinition(@"
DELETE FROM dbo.DataForSeoReviewTask
WHERE Status='Error'
  AND (@TaskType IS NULL OR COALESCE(TaskType, 'reviews') = @TaskType);",
            new { TaskType = normalizedTaskType }, cancellationToken: ct));
    }

    public async Task<int> RefreshTaskStatusesAsync(CancellationToken ct)
    {
        var cfg = options.Value;
        if (string.IsNullOrWhiteSpace(cfg.Login) || string.IsNullOrWhiteSpace(cfg.Password))
            return 0;

        var readyMapsByType = new Dictionary<string, Dictionary<string, ReadyTaskInfo>>(StringComparer.OrdinalIgnoreCase);
        foreach (var taskType in new[] { TaskTypeReviews, TaskTypeMyBusinessInfo })
        {
            try
            {
                readyMapsByType[taskType] = await GetReadyTasksMapAsync(cfg, taskType, ct);
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                logger.LogWarning(ex, "DataForSEO tasks_ready call failed for task type {TaskType}.", taskType);
                readyMapsByType[taskType] = new Dictionary<string, ReadyTaskInfo>(StringComparer.Ordinal);
            }
        }
        var touched = 0;

        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var candidates = (await conn.QueryAsync<TaskTrackingCandidate>(new CommandDefinition(@"
SELECT DataForSeoReviewTaskId, DataForSeoTaskId, COALESCE(TaskType, 'reviews') AS TaskType, Status
FROM dbo.DataForSeoReviewTask
WHERE Status NOT IN ('Populated','CompletedNoReviews','CompletedNoData')
ORDER BY DataForSeoReviewTaskId DESC;", cancellationToken: ct))).ToList();
        var existingTaskKeys = candidates
            .Select(x => $"{NormalizeTaskType(x.TaskType)}|{x.DataForSeoTaskId}")
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        foreach (var candidate in candidates)
        {
            ct.ThrowIfCancellationRequested();

            var candidateTaskType = NormalizeTaskType(candidate.TaskType);
            if (!readyMapsByType.TryGetValue(candidateTaskType, out var mapForType))
                mapForType = readyMapsByType[TaskTypeReviews];

            if (mapForType.TryGetValue(candidate.DataForSeoTaskId, out var ready))
            {
                var updated = await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.DataForSeoReviewTask
SET
  TaskType=@TaskType,
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
                    TaskType = candidateTaskType,
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

        foreach (var entry in readyMapsByType)
        {
            var taskType = entry.Key;
            foreach (var item in entry.Value)
            {
                ct.ThrowIfCancellationRequested();

                var taskKey = $"{taskType}|{item.Key}";
                if (existingTaskKeys.Contains(taskKey))
                    continue;
                if (string.IsNullOrWhiteSpace(item.Value.Tag))
                    continue;

                touched += await conn.ExecuteAsync(new CommandDefinition(@"
INSERT INTO dbo.DataForSeoReviewTask(
  DataForSeoTaskId, TaskType, PlaceId, LocationName, Status, TaskStatusCode, TaskStatusMessage, Endpoint, CreatedAtUtc, LastCheckedUtc, ReadyAtUtc, LastError
)
SELECT
  @DataForSeoTaskId,
  @TaskType,
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
                    TaskType = taskType,
                    PlaceId = item.Value.Tag,
                    TaskStatusCode = item.Value.StatusCode,
                    TaskStatusMessage = item.Value.StatusMessage,
                    Endpoint = item.Value.Endpoint ?? GetTaskGetPath(cfg, item.Key, taskType)
                }, cancellationToken: ct));
            }
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
SELECT DataForSeoReviewTaskId, DataForSeoTaskId, COALESCE(TaskType, 'reviews') AS TaskType, PlaceId, Endpoint
FROM dbo.DataForSeoReviewTask
WHERE DataForSeoReviewTaskId=@Id;", new { Id = dataForSeoReviewTaskId }, cancellationToken: ct));

        if (task is null)
            return new DataForSeoPopulateResult(false, "Task record not found.", 0);

        var taskType = NormalizeTaskType(task.TaskType);
        var endpoint = string.IsNullOrWhiteSpace(task.Endpoint)
            ? GetTaskGetPath(cfg, task.DataForSeoTaskId, taskType)
            : task.Endpoint;

        var client = httpClientFactory.CreateClient();
        var rawSnapshot = await GetTaskGetSnapshotAsync(client, BuildApiUrl(cfg.BaseUrl, endpoint), cfg, ct);

        if (rawSnapshot.StatusCode >= 40000)
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
                    TaskStatusCode = rawSnapshot.StatusCode,
                    TaskStatusMessage = rawSnapshot.StatusMessage
                }, cancellationToken: ct));

            return new DataForSeoPopulateResult(false, rawSnapshot.StatusMessage ?? "Task failed.", 0);
        }

        if (taskType == TaskTypeMyBusinessInfo)
        {
            var infoSnapshot = ParseMyBusinessInfoSnapshot(rawSnapshot.Body);
            if (!infoSnapshot.HasPayload)
            {
                var status = infoSnapshot.IsCompleted ? "CompletedNoData" : "Pending";
                await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.DataForSeoReviewTask
SET
  Status=@Status,
  TaskStatusCode=@TaskStatusCode,
  TaskStatusMessage=@TaskStatusMessage,
  LastCheckedUtc=SYSUTCDATETIME(),
  LastAttemptedPopulateUtc=SYSUTCDATETIME(),
  LastPopulateReviewCount=0,
  ReadyAtUtc=CASE WHEN @Status='CompletedNoData' THEN COALESCE(ReadyAtUtc, SYSUTCDATETIME()) ELSE ReadyAtUtc END,
  PopulatedAtUtc=CASE WHEN @Status='CompletedNoData' THEN SYSUTCDATETIME() ELSE PopulatedAtUtc END,
  LastError=NULL
WHERE DataForSeoReviewTaskId=@DataForSeoReviewTaskId;",
                    new
                    {
                        task.DataForSeoReviewTaskId,
                        Status = status,
                        TaskStatusCode = infoSnapshot.StatusCode,
                        TaskStatusMessage = infoSnapshot.StatusMessage
                    }, cancellationToken: ct));

                var msg = infoSnapshot.IsCompleted ? "Task completed but had no business info payload." : "Task is not ready yet.";
                return new DataForSeoPopulateResult(infoSnapshot.IsCompleted, msg, 0);
            }

            await using var infoTx = await conn.BeginTransactionAsync(ct);
            var infoUpserted = await UpsertPlaceBusinessInfoAsync(conn, infoTx, task.PlaceId, infoSnapshot.Info, ct);

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
                    TaskStatusCode = infoSnapshot.StatusCode,
                    TaskStatusMessage = infoSnapshot.StatusMessage,
                    LastPopulateReviewCount = infoUpserted
                }, infoTx, cancellationToken: ct));

            await infoTx.CommitAsync(ct);
            return new DataForSeoPopulateResult(true, "Upserted detailed business info.", infoUpserted);
        }

        var reviewSnapshot = ParseTaskGetSnapshot(rawSnapshot.Body);
        if (reviewSnapshot.Reviews.Count == 0)
        {
            var status = reviewSnapshot.IsCompleted ? "CompletedNoReviews" : "Pending";
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
                    TaskStatusCode = reviewSnapshot.StatusCode,
                    TaskStatusMessage = reviewSnapshot.StatusMessage
                }, cancellationToken: ct));

            var msg = reviewSnapshot.IsCompleted ? "Task completed but had no reviews to import." : "Task is not ready yet.";
            return new DataForSeoPopulateResult(reviewSnapshot.IsCompleted, msg, 0);
        }

        await using var tx = await conn.BeginTransactionAsync(ct);
        var upserted = await UpsertReviewsAsync(conn, tx, task.PlaceId, task.DataForSeoTaskId, reviewSnapshot.Reviews.Values, ct);

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
                TaskStatusCode = reviewSnapshot.StatusCode,
                TaskStatusMessage = reviewSnapshot.StatusMessage,
                LastPopulateReviewCount = upserted
            }, tx, cancellationToken: ct));

        await tx.CommitAsync(ct);
        await reviewVelocityService.RecomputePlaceStatsAsync(task.PlaceId, ct);

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

    private async Task<int> UpsertPlaceBusinessInfoAsync(
        Microsoft.Data.SqlClient.SqlConnection conn,
        System.Data.Common.DbTransaction tx,
        string placeId,
        BusinessInfoPayload payload,
        CancellationToken ct)
    {
        var otherCategories = payload.AdditionalCategories.Count > 0
            ? JsonSerializer.Serialize(payload.AdditionalCategories)
            : "[]";
        var placeTopics = payload.PlaceTopics.Count > 0
            ? JsonSerializer.Serialize(payload.PlaceTopics)
            : "[]";

        await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.Place
SET
  Description=COALESCE(@Description, Description),
  PhotoCount=COALESCE(@PhotoCount, PhotoCount),
  OtherCategoriesJson=@OtherCategoriesJson,
  PlaceTopicsJson=@PlaceTopicsJson,
  LastSeenUtc=SYSUTCDATETIME()
WHERE PlaceId=@PlaceId;",
            new
            {
                PlaceId = placeId,
                Description = TruncateText(payload.Description, 750),
                payload.PhotoCount,
                OtherCategoriesJson = otherCategories,
                PlaceTopicsJson = placeTopics
            }, tx, cancellationToken: ct));

        return 1;
    }

    private async Task<TaskCreationResult> CreateReviewsTaskAsync(
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

        using var request = CreateRequest(HttpMethod.Post, BuildApiUrl(cfg.BaseUrl, GetTaskPostPath(cfg, TaskTypeReviews)), cfg);
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

    private async Task<TaskCreationResult> CreateMyBusinessInfoTaskAsync(
        string placeId,
        string? locationName,
        DataForSeoOptions cfg,
        CancellationToken ct)
    {
        var normalizedLocationName = string.IsNullOrWhiteSpace(locationName) ? null : locationName.Trim();
        var requestPayload = new[]
        {
            new MyBusinessInfoTaskPostItem(
                placeId,
                string.IsNullOrWhiteSpace(cfg.LanguageCode) ? null : cfg.LanguageCode.Trim(),
                2)
        };
        var payloadJson = JsonSerializer.Serialize(
            requestPayload,
            new JsonSerializerOptions
            {
                DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
            });
        var postUrl = BuildApiUrl(cfg.BaseUrl, GetTaskPostPath(cfg, TaskTypeMyBusinessInfo));
        var created = await SendMyBusinessInfoTaskPostAsync(cfg, postUrl, payloadJson, "place_id", ct);
        if (created.StatusCode == 40501
            && !string.IsNullOrWhiteSpace(created.StatusMessage)
            && created.StatusMessage.Contains("keyword", StringComparison.OrdinalIgnoreCase))
        {
            // Compatibility fallback: some DataForSEO environments require keyword=place_id:...
            var fallbackPayload = new[]
            {
                new MyBusinessInfoKeywordTaskPostItem(
                    $"place_id:{placeId}",
                    string.IsNullOrWhiteSpace(cfg.LanguageCode) ? null : cfg.LanguageCode.Trim(),
                    2)
            };
            var fallbackJson = JsonSerializer.Serialize(
                fallbackPayload,
                new JsonSerializerOptions
                {
                    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
                });
            logger.LogWarning(
                "DataForSEO my_business_info rejected place_id payload with keyword validation error; retrying with keyword fallback for place {PlaceId}.",
                placeId);
            created = await SendMyBusinessInfoTaskPostAsync(cfg, postUrl, fallbackJson, "keyword", ct);
        }
        if (created.StatusCode == 40501
            && !string.IsNullOrWhiteSpace(created.StatusMessage)
            && created.StatusMessage.Contains("location_name", StringComparison.OrdinalIgnoreCase)
            && !string.IsNullOrWhiteSpace(normalizedLocationName))
        {
            // Compatibility fallback: keyword mode may also require explicit location_name.
            var fallbackWithLocationPayload = new[]
            {
                new MyBusinessInfoKeywordWithLocationTaskPostItem(
                    $"place_id:{placeId}",
                    string.IsNullOrWhiteSpace(cfg.LanguageCode) ? null : cfg.LanguageCode.Trim(),
                    normalizedLocationName,
                    2)
            };
            var fallbackWithLocationJson = JsonSerializer.Serialize(
                fallbackWithLocationPayload,
                new JsonSerializerOptions
                {
                    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
                });
            logger.LogWarning(
                "DataForSEO my_business_info rejected keyword fallback with location_name validation error; retrying with keyword+location_name for place {PlaceId}.",
                placeId);
            created = await SendMyBusinessInfoTaskPostAsync(cfg, postUrl, fallbackWithLocationJson, "keyword+location_name", ct);
        }

        return created;
    }

    private async Task<TaskCreationResult> SendMyBusinessInfoTaskPostAsync(
        DataForSeoOptions cfg,
        string postUrl,
        string payloadJson,
        string payloadMode,
        CancellationToken ct)
    {
        using var request = CreateRequest(HttpMethod.Post, postUrl, cfg);
        request.Content = new StringContent(payloadJson, Encoding.UTF8, "application/json");
        logger.LogInformation(
            "DataForSEO my_business_info task_post request. Url={Url}, Mode={Mode}, Body={Body}",
            postUrl,
            payloadMode,
            payloadJson);

        var client = httpClientFactory.CreateClient();
        using var response = await client.SendAsync(request, ct);
        var body = await response.Content.ReadAsStringAsync(ct);
        logger.LogInformation(
            "DataForSEO my_business_info task_post response. Status={StatusCode}, Mode={Mode}, Body={Body}",
            (int)response.StatusCode,
            payloadMode,
            body);
        if (!response.IsSuccessStatusCode)
            throw new HttpRequestException($"DataForSEO my_business_info task_post failed with status {(int)response.StatusCode}: {body}");

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

    private async Task<Dictionary<string, ReadyTaskInfo>> GetReadyTasksMapAsync(DataForSeoOptions cfg, string taskType, CancellationToken ct)
    {
        using var request = CreateRequest(HttpMethod.Get, BuildApiUrl(cfg.BaseUrl, GetTasksReadyPath(cfg, taskType)), cfg);
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

    private async Task<RawTaskGetSnapshot> GetTaskGetSnapshotAsync(
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

        return ParseRawTaskGetSnapshot(body);
    }

    private static RawTaskGetSnapshot ParseRawTaskGetSnapshot(string body)
    {
        using var document = JsonDocument.Parse(body);
        var root = document.RootElement;
        if (!root.TryGetProperty("tasks", out var tasks) || tasks.ValueKind != JsonValueKind.Array || tasks.GetArrayLength() == 0)
            return new RawTaskGetSnapshot(0, "No tasks in response.", false, body);

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
        var isCompleted = taskStatusCode == 20000 && resultCount >= 0;
        return new RawTaskGetSnapshot(taskStatusCode, taskStatusMessage, isCompleted, body);
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

    private static BusinessInfoTaskSnapshot ParseMyBusinessInfoSnapshot(string body)
    {
        using var document = JsonDocument.Parse(body);
        var root = document.RootElement;
        if (!root.TryGetProperty("tasks", out var tasks) || tasks.ValueKind != JsonValueKind.Array || tasks.GetArrayLength() == 0)
            return new BusinessInfoTaskSnapshot(0, "No tasks in response.", false, false, new BusinessInfoPayload(null, null, [], [], null));

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
        var isCompleted = taskStatusCode == 20000 && resultCount >= 0;

        BusinessInfoPayload? payload = null;
        if (task.TryGetProperty("result", out var resultArray) && resultArray.ValueKind == JsonValueKind.Array)
        {
            foreach (var result in resultArray.EnumerateArray())
            {
                foreach (var item in EnumerateBusinessInfoCandidates(result))
                {
                    var candidate = ParseBusinessInfo(item);
                    if (!candidate.HasValues)
                        continue;
                    payload = candidate;
                    break;
                }

                if (payload is not null)
                    break;
            }
        }

        if (payload is null)
            payload = new BusinessInfoPayload(null, null, [], [], null);

        return new BusinessInfoTaskSnapshot(taskStatusCode, taskStatusMessage, isCompleted, payload.HasValues, payload);
    }

    private static IEnumerable<JsonElement> EnumerateBusinessInfoCandidates(JsonElement result)
    {
        if (result.TryGetProperty("items", out var items))
        {
            if (items.ValueKind == JsonValueKind.Object)
            {
                yield return items;
                yield break;
            }

            if (items.ValueKind == JsonValueKind.Array)
            {
                foreach (var item in items.EnumerateArray())
                {
                    if (item.ValueKind == JsonValueKind.Object)
                        yield return item;
                }

                yield break;
            }
        }

        if (result.ValueKind == JsonValueKind.Object)
            yield return result;
    }

    private static BusinessInfoPayload ParseBusinessInfo(JsonElement item)
    {
        var description = GetString(item, "description");
        if (string.IsNullOrWhiteSpace(description))
            description = GetString(item, "about");

        var additionalCategories = ExtractStringList(item, "additional_categories");
        var placeTopics = ExtractStringList(item, "place_topics");

        return new BusinessInfoPayload(
            TruncateText(description, 750),
            GetInt(item, "total_photos"),
            additionalCategories,
            placeTopics,
            item.GetRawText());
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

    private static IReadOnlyList<string> ExtractStringList(JsonElement obj, string propertyName)
    {
        if (!obj.TryGetProperty(propertyName, out var node) || node.ValueKind == JsonValueKind.Null)
            return [];

        var values = new List<string>();
        if (node.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in node.EnumerateArray())
            {
                var parsed = item.ValueKind switch
                {
                    JsonValueKind.String => item.GetString(),
                    JsonValueKind.Object => GetString(item, "title")
                        ?? GetString(item, "name")
                        ?? GetString(item, "topic")
                        ?? GetString(item, "keyword")
                        ?? GetString(item, "value"),
                    _ => null
                };

                if (!string.IsNullOrWhiteSpace(parsed))
                    values.Add(parsed.Trim());
            }
        }
        else if (node.ValueKind == JsonValueKind.Object)
        {
            // DataForSEO can return place_topics as an object map, e.g. { "SEO": 9, "traffic": 3 }.
            if (string.Equals(propertyName, "place_topics", StringComparison.OrdinalIgnoreCase))
            {
                foreach (var prop in node.EnumerateObject())
                {
                    if (!string.IsNullOrWhiteSpace(prop.Name))
                        values.Add(prop.Name.Trim());
                }
            }
            else
            {
                var parsed = GetString(node, "title")
                    ?? GetString(node, "name")
                    ?? GetString(node, "topic")
                    ?? GetString(node, "keyword")
                    ?? GetString(node, "value");
                if (!string.IsNullOrWhiteSpace(parsed))
                    values.Add(parsed.Trim());
            }
        }
        else if (node.ValueKind == JsonValueKind.String)
        {
            var parsed = node.GetString();
            if (!string.IsNullOrWhiteSpace(parsed))
                values.Add(parsed.Trim());
        }

        return values
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
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

    private static string GetTaskGetPath(DataForSeoOptions cfg, string taskId, string taskType)
    {
        var normalizedTaskType = NormalizeTaskType(taskType);
        var format = normalizedTaskType == TaskTypeMyBusinessInfo
            ? "/v3/business_data/google/my_business_info/task_get/{0}"
            : "/v3/business_data/google/reviews/task_get/{0}";

        return string.Format(CultureInfo.InvariantCulture, format, taskId);
    }

    private static string GetTasksReadyPath(DataForSeoOptions cfg, string taskType)
    {
        var normalizedTaskType = NormalizeTaskType(taskType);
        return normalizedTaskType == TaskTypeMyBusinessInfo
            ? "/v3/business_data/google/my_business_info/tasks_ready"
            : "/v3/business_data/google/reviews/tasks_ready";
    }

    private static string GetTaskPostPath(DataForSeoOptions cfg, string taskType)
    {
        var normalizedTaskType = NormalizeTaskType(taskType);
        return normalizedTaskType == TaskTypeMyBusinessInfo
            ? "/v3/business_data/google/my_business_info/task_post"
            : "/v3/business_data/google/reviews/task_post";
    }

    private static string NormalizeTaskType(string? taskType)
    {
        if (string.Equals(taskType, TaskTypeMyBusinessInfo, StringComparison.OrdinalIgnoreCase))
            return TaskTypeMyBusinessInfo;
        return TaskTypeReviews;
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

    private static string? TruncateText(string? value, int maxLength)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;
        var trimmed = value.Trim();
        return trimmed.Length <= maxLength ? trimmed : trimmed[..maxLength];
    }

    private sealed record TaskCreationResult(string? TaskId, int StatusCode, string? StatusMessage);

    private sealed record ReadyTaskInfo(string? Endpoint, int? StatusCode, string? StatusMessage, string? Tag);

    private sealed record TaskTrackingCandidate(long DataForSeoReviewTaskId, string DataForSeoTaskId, string TaskType, string Status);

    private sealed record TaskPopulateTarget(long DataForSeoReviewTaskId, string DataForSeoTaskId, string TaskType, string PlaceId, string? Endpoint);

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

    private sealed record MyBusinessInfoTaskPostItem(
        [property: JsonPropertyName("place_id")] string PlaceId,
        [property: JsonPropertyName("language_code")] string? LanguageCode,
        [property: JsonPropertyName("priority")] int? Priority);

    private sealed record MyBusinessInfoKeywordTaskPostItem(
        [property: JsonPropertyName("keyword")] string Keyword,
        [property: JsonPropertyName("language_code")] string? LanguageCode,
        [property: JsonPropertyName("priority")] int? Priority);

    private sealed record MyBusinessInfoKeywordWithLocationTaskPostItem(
        [property: JsonPropertyName("keyword")] string Keyword,
        [property: JsonPropertyName("language_code")] string? LanguageCode,
        [property: JsonPropertyName("location_name")] string? LocationName,
        [property: JsonPropertyName("priority")] int? Priority);

    private sealed record RawTaskGetSnapshot(
        int StatusCode,
        string? StatusMessage,
        bool IsCompleted,
        string Body);

    private sealed record BusinessInfoPayload(
        string? Description,
        int? PhotoCount,
        IReadOnlyList<string> AdditionalCategories,
        IReadOnlyList<string> PlaceTopics,
        string? RawJson)
    {
        public bool HasValues =>
            !string.IsNullOrWhiteSpace(Description)
            || PhotoCount.HasValue
            || AdditionalCategories.Count > 0
            || PlaceTopics.Count > 0;
    }

    private sealed record BusinessInfoTaskSnapshot(
        int StatusCode,
        string? StatusMessage,
        bool IsCompleted,
        bool HasPayload,
        BusinessInfoPayload Info);

    private sealed record PostbackSummary(
        string? TaskId,
        int? TaskStatusCode,
        string? TaskStatusMessage,
        string? Endpoint);
}
