using System.Globalization;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Dapper;
using LocalSeo.Web.Data;
using LocalSeo.Web.Models;
using LocalSeo.Web.Options;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Services;

public sealed class DataForSeoReviewsProvider(
    ISqlConnectionFactory connectionFactory,
    IHttpClientFactory httpClientFactory,
    IOptions<DataForSeoOptions> options,
    IReviewVelocityService reviewVelocityService,
    IWebHostEnvironment webHostEnvironment,
    ILogger<DataForSeoReviewsProvider> logger) : IReviewsProvider, IDataForSeoTaskTracker
{
    private const string TaskTypeReviews = "reviews";
    private const string TaskTypeMyBusinessInfo = "my_business_info";
    private const string TaskTypeMyBusinessUpdates = "my_business_updates";
    private const string TaskTypeQuestionsAndAnswers = "questions_and_answers";
    private const string TaskTypeSocialProfiles = "social_profiles";

    public async Task FetchAndStoreReviewsAsync(
        string placeId,
        int? reviewCount,
        string? locationName,
        decimal? centerLat,
        decimal? centerLng,
        int? radiusMeters,
        bool fetchGoogleReviews,
        bool fetchMyBusinessInfo,
        bool fetchGoogleUpdates,
        bool fetchGoogleQuestionsAndAnswers,
        bool fetchGoogleSocialProfiles,
        CancellationToken ct)
    {
        if (!fetchGoogleReviews && !fetchMyBusinessInfo && !fetchGoogleUpdates && !fetchGoogleQuestionsAndAnswers && !fetchGoogleSocialProfiles)
            return;

        var cfg = options.Value;
        if (string.IsNullOrWhiteSpace(cfg.Login) || string.IsNullOrWhiteSpace(cfg.Password))
        {
            if (fetchGoogleReviews)
                await LogTaskFailureAsync(placeId, locationName, TaskTypeReviews, "DataForSEO credentials are not configured.", ct);
            if (fetchMyBusinessInfo)
                await LogTaskFailureAsync(placeId, locationName, TaskTypeMyBusinessInfo, "DataForSEO credentials are not configured.", ct);
            if (fetchGoogleUpdates)
                await LogTaskFailureAsync(placeId, locationName, TaskTypeMyBusinessUpdates, "DataForSEO credentials are not configured.", ct);
            if (fetchGoogleQuestionsAndAnswers)
                await LogTaskFailureAsync(placeId, locationName, TaskTypeQuestionsAndAnswers, "DataForSEO credentials are not configured.", ct);
            if (fetchGoogleSocialProfiles)
                await LogTaskFailureAsync(placeId, locationName, TaskTypeSocialProfiles, "DataForSEO credentials are not configured.", ct);
            logger.LogWarning("DataForSEO credentials are not configured; skipping review fetch for place {PlaceId}.", placeId);
            return;
        }

        if (fetchGoogleReviews)
        {
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
        }

        if (fetchMyBusinessInfo)
        {
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
        }

        if (fetchGoogleUpdates)
        {
            try
            {
                await CreateAndTrackTaskAsync(
                    placeId,
                    locationName,
                    TaskTypeMyBusinessUpdates,
                    () => CreateMyBusinessUpdatesTaskAsync(placeId, locationName, cfg, ct),
                    ct);
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                await LogTaskFailureAsync(placeId, locationName, TaskTypeMyBusinessUpdates, ex.Message, ct);
                logger.LogWarning(ex, "DataForSEO my_business_updates task creation failed for place {PlaceId}.", placeId);
            }
        }

        if (fetchGoogleQuestionsAndAnswers)
        {
            try
            {
                await CreateAndTrackTaskAsync(
                    placeId,
                    locationName,
                    TaskTypeQuestionsAndAnswers,
                    () => CreateQuestionsAndAnswersTaskAsync(placeId, locationName, cfg, ct),
                    ct);
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                await LogTaskFailureAsync(placeId, locationName, TaskTypeQuestionsAndAnswers, ex.Message, ct);
                logger.LogWarning(ex, "DataForSEO questions_and_answers task creation failed for place {PlaceId}.", placeId);
            }
        }

        if (fetchGoogleSocialProfiles)
        {
            try
            {
                await FetchAndStoreSocialProfilesAsync(placeId, locationName, cfg, ct);
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                await LogTaskFailureAsync(placeId, locationName, TaskTypeSocialProfiles, ex.Message, ct);
                logger.LogWarning(ex, "DataForSEO social_profiles call failed for place {PlaceId}.", placeId);
            }
        }

        if (!fetchGoogleReviews && !fetchMyBusinessInfo && !fetchGoogleUpdates && !fetchGoogleQuestionsAndAnswers)
            return;

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

        var status = IsNoDataStatus(createdTask.StatusCode, createdTask.StatusMessage)
            ? "NoData"
            : createdTask.StatusCode is >= 20000 and < 30000 ? "Created" : "Error";
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

    public async Task<IReadOnlyList<DataForSeoTaskRow>> GetLatestTasksAsync(int take, string? taskType, string? status, CancellationToken ct)
    {
        var normalizedTaskType = string.IsNullOrWhiteSpace(taskType) || string.Equals(taskType, "all", StringComparison.OrdinalIgnoreCase)
            ? null
            : NormalizeTaskType(taskType);
        var normalizedStatus = NormalizeTaskStatus(status);
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
  AND (@Status IS NULL OR Status = @Status)
ORDER BY
  DataForSeoReviewTaskId DESC;",
            new { Take = Math.Clamp(take, 1, 2000), TaskType = normalizedTaskType, Status = normalizedStatus }, cancellationToken: ct));
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
        foreach (var taskType in new[] { TaskTypeReviews, TaskTypeMyBusinessInfo, TaskTypeMyBusinessUpdates, TaskTypeQuestionsAndAnswers })
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
WHERE Status NOT IN ('Populated','CompletedNoReviews','CompletedNoData','CompletedNoUpdates','NoData')
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
            var status = IsNoDataStatus(rawSnapshot.StatusCode, rawSnapshot.StatusMessage) ? "NoData" : "Error";
            await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.DataForSeoReviewTask
SET
  Status=@Status,
  TaskStatusCode=@TaskStatusCode,
  TaskStatusMessage=@TaskStatusMessage,
  LastCheckedUtc=SYSUTCDATETIME(),
  LastAttemptedPopulateUtc=SYSUTCDATETIME(),
  LastError=CASE WHEN @Status='Error' THEN @TaskStatusMessage ELSE NULL END
WHERE DataForSeoReviewTaskId=@DataForSeoReviewTaskId;",
                new
                {
                    task.DataForSeoReviewTaskId,
                    Status = status,
                    TaskStatusCode = rawSnapshot.StatusCode,
                    TaskStatusMessage = rawSnapshot.StatusMessage
                }, cancellationToken: ct));

            return new DataForSeoPopulateResult(
                status == "NoData",
                status == "NoData" ? "Task completed with no data (No Search Results)." : rawSnapshot.StatusMessage ?? "Task failed.",
                0);
        }

        if (taskType == TaskTypeMyBusinessInfo)
        {
            var infoSnapshot = ParseMyBusinessInfoSnapshot(rawSnapshot.Body);
            if (!infoSnapshot.HasPayload)
            {
                var status = infoSnapshot.IsCompleted ? "NoData" : "Pending";
                await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.DataForSeoReviewTask
SET
  Status=@Status,
  TaskStatusCode=@TaskStatusCode,
  TaskStatusMessage=@TaskStatusMessage,
  LastCheckedUtc=SYSUTCDATETIME(),
  LastAttemptedPopulateUtc=SYSUTCDATETIME(),
  LastPopulateReviewCount=0,
  ReadyAtUtc=CASE WHEN @Status='NoData' THEN COALESCE(ReadyAtUtc, SYSUTCDATETIME()) ELSE ReadyAtUtc END,
  PopulatedAtUtc=CASE WHEN @Status='NoData' THEN SYSUTCDATETIME() ELSE PopulatedAtUtc END,
  LastError=NULL
WHERE DataForSeoReviewTaskId=@DataForSeoReviewTaskId;",
                    new
                    {
                        task.DataForSeoReviewTaskId,
                        Status = status,
                        TaskStatusCode = infoSnapshot.StatusCode,
                        TaskStatusMessage = infoSnapshot.StatusMessage
                    }, cancellationToken: ct));

                var msg = infoSnapshot.IsCompleted ? "Task completed with no data." : "Task is not ready yet.";
                return new DataForSeoPopulateResult(infoSnapshot.IsCompleted, msg, 0);
            }

            var resolvedInfo = await ResolveBusinessInfoAssetPathsAsync(task.PlaceId, infoSnapshot.Info, ct);

            await using var infoTx = await conn.BeginTransactionAsync(ct);
            var infoUpserted = await UpsertPlaceBusinessInfoAsync(conn, infoTx, task.PlaceId, resolvedInfo, ct);

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

        if (taskType == TaskTypeMyBusinessUpdates)
        {
            var updatesSnapshot = ParseMyBusinessUpdatesSnapshot(rawSnapshot.Body);
            if (updatesSnapshot.Updates.Count == 0)
            {
                var status = updatesSnapshot.IsCompleted ? "NoData" : "Pending";
                await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.DataForSeoReviewTask
SET
  Status=@Status,
  TaskStatusCode=@TaskStatusCode,
  TaskStatusMessage=@TaskStatusMessage,
  LastCheckedUtc=SYSUTCDATETIME(),
  LastAttemptedPopulateUtc=SYSUTCDATETIME(),
  LastPopulateReviewCount=0,
  ReadyAtUtc=CASE WHEN @Status='NoData' THEN COALESCE(ReadyAtUtc, SYSUTCDATETIME()) ELSE ReadyAtUtc END,
  PopulatedAtUtc=CASE WHEN @Status='NoData' THEN SYSUTCDATETIME() ELSE PopulatedAtUtc END,
  LastError=NULL
WHERE DataForSeoReviewTaskId=@DataForSeoReviewTaskId;",
                    new
                    {
                        task.DataForSeoReviewTaskId,
                        Status = status,
                        TaskStatusCode = updatesSnapshot.StatusCode,
                        TaskStatusMessage = updatesSnapshot.StatusMessage
                    }, cancellationToken: ct));

                var msg = updatesSnapshot.IsCompleted ? "Task completed with no data." : "Task is not ready yet.";
                return new DataForSeoPopulateResult(updatesSnapshot.IsCompleted, msg, 0);
            }

            await using var updatesTx = await conn.BeginTransactionAsync(ct);
            var upsertedUpdates = await UpsertUpdatesAsync(conn, updatesTx, task.PlaceId, task.DataForSeoTaskId, updatesSnapshot.Updates.Values, ct);

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
                    TaskStatusCode = updatesSnapshot.StatusCode,
                    TaskStatusMessage = updatesSnapshot.StatusMessage,
                    LastPopulateReviewCount = upsertedUpdates
                }, updatesTx, cancellationToken: ct));

            await updatesTx.CommitAsync(ct);
            return new DataForSeoPopulateResult(true, $"Upserted {upsertedUpdates} updates.", upsertedUpdates);
        }

        if (taskType == TaskTypeQuestionsAndAnswers)
        {
            var qaSnapshot = ParseQuestionsAndAnswersSnapshot(rawSnapshot.Body);
            if (qaSnapshot.Items.Count == 0)
            {
                var status = qaSnapshot.IsCompleted ? "NoData" : "Pending";
                await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.DataForSeoReviewTask
SET
  Status=@Status,
  TaskStatusCode=@TaskStatusCode,
  TaskStatusMessage=@TaskStatusMessage,
  LastCheckedUtc=SYSUTCDATETIME(),
  LastAttemptedPopulateUtc=SYSUTCDATETIME(),
  LastPopulateReviewCount=0,
  ReadyAtUtc=CASE WHEN @Status='NoData' THEN COALESCE(ReadyAtUtc, SYSUTCDATETIME()) ELSE ReadyAtUtc END,
  PopulatedAtUtc=CASE WHEN @Status='NoData' THEN SYSUTCDATETIME() ELSE PopulatedAtUtc END,
  LastError=NULL
WHERE DataForSeoReviewTaskId=@DataForSeoReviewTaskId;",
                    new
                    {
                        task.DataForSeoReviewTaskId,
                        Status = status,
                        TaskStatusCode = qaSnapshot.StatusCode,
                        TaskStatusMessage = qaSnapshot.StatusMessage
                    }, cancellationToken: ct));

                var msg = qaSnapshot.IsCompleted ? "Task completed with no data." : "Task is not ready yet.";
                return new DataForSeoPopulateResult(qaSnapshot.IsCompleted, msg, 0);
            }

            await using var qaTx = await conn.BeginTransactionAsync(ct);
            var upsertedQa = await UpsertQuestionsAndAnswersAsync(conn, qaTx, task.PlaceId, task.DataForSeoTaskId, qaSnapshot.Items.Values, ct);

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
                    TaskStatusCode = qaSnapshot.StatusCode,
                    TaskStatusMessage = qaSnapshot.StatusMessage,
                    LastPopulateReviewCount = upsertedQa
                }, qaTx, cancellationToken: ct));

            await qaTx.CommitAsync(ct);
            return new DataForSeoPopulateResult(true, $"Upserted {upsertedQa} question/answer row(s).", upsertedQa);
        }

        var reviewSnapshot = ParseTaskGetSnapshot(rawSnapshot.Body);
        if (reviewSnapshot.Reviews.Count == 0)
        {
            var status = reviewSnapshot.IsCompleted ? "NoData" : "Pending";
            await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.DataForSeoReviewTask
SET
  Status=@Status,
  TaskStatusCode=@TaskStatusCode,
  TaskStatusMessage=@TaskStatusMessage,
  LastCheckedUtc=SYSUTCDATETIME(),
  LastAttemptedPopulateUtc=SYSUTCDATETIME(),
  LastPopulateReviewCount=0,
  ReadyAtUtc=CASE WHEN @Status='NoData' THEN COALESCE(ReadyAtUtc, SYSUTCDATETIME()) ELSE ReadyAtUtc END,
  PopulatedAtUtc=CASE WHEN @Status='NoData' THEN SYSUTCDATETIME() ELSE PopulatedAtUtc END,
  LastError=NULL
WHERE DataForSeoReviewTaskId=@DataForSeoReviewTaskId;",
                new
                {
                    task.DataForSeoReviewTaskId,
                    Status = status,
                    TaskStatusCode = reviewSnapshot.StatusCode,
                    TaskStatusMessage = reviewSnapshot.StatusMessage
                }, cancellationToken: ct));

            var msg = reviewSnapshot.IsCompleted ? "Task completed with no data." : "Task is not ready yet.";
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

    public async Task<DataForSeoBulkPopulateResult> PopulateReadyTasksAsync(string? taskType, CancellationToken ct)
    {
        var normalizedTaskType = string.IsNullOrWhiteSpace(taskType) || string.Equals(taskType, "all", StringComparison.OrdinalIgnoreCase)
            ? null
            : NormalizeTaskType(taskType);

        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var readyTaskIds = (await conn.QueryAsync<long>(new CommandDefinition(@"
SELECT DataForSeoReviewTaskId
FROM dbo.DataForSeoReviewTask
WHERE Status='Ready'
  AND (@TaskType IS NULL OR COALESCE(TaskType, 'reviews') = @TaskType)
ORDER BY DataForSeoReviewTaskId;", new { TaskType = normalizedTaskType }, cancellationToken: ct))).ToList();

        var attempted = readyTaskIds.Count;
        var succeeded = 0;
        var failed = 0;
        var reviewsUpserted = 0;

        foreach (var taskId in readyTaskIds)
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                var result = await PopulateTaskAsync(taskId, ct);
                if (result.Success)
                {
                    succeeded++;
                    reviewsUpserted += Math.Max(0, result.ReviewsUpserted);
                }
                else
                {
                    failed++;
                }
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                failed++;
                logger.LogWarning(ex, "Bulk populate failed for DataForSEO task row id {TaskRowId}.", taskId);
            }
        }

        return new DataForSeoBulkPopulateResult(attempted, succeeded, failed, reviewsUpserted);
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
            : statusCode is >= 20000 and < 30000 ? "Ready" : IsNoDataStatus(statusCode, statusMessage) ? "NoData" : "Error";
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

    private async Task<int> UpsertUpdatesAsync(
        Microsoft.Data.SqlClient.SqlConnection conn,
        System.Data.Common.DbTransaction tx,
        string placeId,
        string sourceTaskId,
        IEnumerable<UpdatePayload> updates,
        CancellationToken ct)
    {
        var count = 0;
        foreach (var update in updates)
        {
            await conn.ExecuteAsync(new CommandDefinition(@"
MERGE dbo.PlaceUpdate AS target
USING (SELECT @PlaceId AS PlaceId, @UpdateKey AS UpdateKey) AS source
ON target.PlaceId = source.PlaceId AND target.UpdateKey = source.UpdateKey
WHEN MATCHED THEN UPDATE SET
  PostText=@PostText,
  Url=@Url,
  ImagesUrlJson=@ImagesUrlJson,
  PostDateUtc=@PostDateUtc,
  LinksJson=@LinksJson,
  SourceTaskId=@SourceTaskId,
  RawJson=@RawJson,
  LastSeenUtc=SYSUTCDATETIME()
WHEN NOT MATCHED THEN
  INSERT(
    PlaceId,UpdateKey,PostText,Url,ImagesUrlJson,PostDateUtc,LinksJson,SourceTaskId,RawJson,FirstSeenUtc,LastSeenUtc
  )
  VALUES(
    @PlaceId,@UpdateKey,@PostText,@Url,@ImagesUrlJson,@PostDateUtc,@LinksJson,@SourceTaskId,@RawJson,SYSUTCDATETIME(),SYSUTCDATETIME()
  );",
                new
                {
                    PlaceId = placeId,
                    update.UpdateKey,
                    update.PostText,
                    update.Url,
                    update.ImagesUrlJson,
                    update.PostDateUtc,
                    update.LinksJson,
                    SourceTaskId = sourceTaskId,
                    update.RawJson
                }, tx, cancellationToken: ct));
            count++;
        }

        return count;
    }

    private async Task<int> UpsertQuestionsAndAnswersAsync(
        Microsoft.Data.SqlClient.SqlConnection conn,
        System.Data.Common.DbTransaction tx,
        string placeId,
        string sourceTaskId,
        IEnumerable<QuestionAnswerPayload> items,
        CancellationToken ct)
    {
        var count = 0;
        foreach (var item in items)
        {
            await conn.ExecuteAsync(new CommandDefinition(@"
MERGE dbo.PlaceQuestionAnswer AS target
USING (SELECT @PlaceId AS PlaceId, @QaKey AS QaKey) AS source
ON target.PlaceId = source.PlaceId AND target.QaKey = source.QaKey
WHEN MATCHED THEN UPDATE SET
  QuestionText=@QuestionText,
  QuestionTimestampUtc=@QuestionTimestampUtc,
  QuestionProfileName=@QuestionProfileName,
  AnswerText=@AnswerText,
  AnswerTimestampUtc=@AnswerTimestampUtc,
  AnswerProfileName=@AnswerProfileName,
  SourceTaskId=@SourceTaskId,
  RawJson=@RawJson,
  LastSeenUtc=SYSUTCDATETIME()
WHEN NOT MATCHED THEN
  INSERT(
    PlaceId,QaKey,QuestionText,QuestionTimestampUtc,QuestionProfileName,AnswerText,AnswerTimestampUtc,AnswerProfileName,SourceTaskId,RawJson,FirstSeenUtc,LastSeenUtc
  )
  VALUES(
    @PlaceId,@QaKey,@QuestionText,@QuestionTimestampUtc,@QuestionProfileName,@AnswerText,@AnswerTimestampUtc,@AnswerProfileName,@SourceTaskId,@RawJson,SYSUTCDATETIME(),SYSUTCDATETIME()
  );",
                new
                {
                    PlaceId = placeId,
                    item.QaKey,
                    item.QuestionText,
                    item.QuestionTimestampUtc,
                    item.QuestionProfileName,
                    item.AnswerText,
                    item.AnswerTimestampUtc,
                    item.AnswerProfileName,
                    SourceTaskId = sourceTaskId,
                    item.RawJson
                }, tx, cancellationToken: ct));
            count++;
        }

        await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE p
SET p.QuestionAnswerCount = qa.Cnt
FROM dbo.Place p
OUTER APPLY (
    SELECT COUNT(1) AS Cnt
    FROM dbo.PlaceQuestionAnswer x
    WHERE x.PlaceId = p.PlaceId
) qa
WHERE p.PlaceId=@PlaceId;",
            new { PlaceId = placeId }, tx, cancellationToken: ct));

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
  LogoUrl=COALESCE(@LogoUrl, LogoUrl),
  MainPhotoUrl=COALESCE(@MainPhotoUrl, MainPhotoUrl),
  OtherCategoriesJson=@OtherCategoriesJson,
  PlaceTopicsJson=@PlaceTopicsJson,
  LastSeenUtc=SYSUTCDATETIME()
WHERE PlaceId=@PlaceId;",
            new
            {
                PlaceId = placeId,
                Description = TruncateText(payload.Description, 750),
                payload.PhotoCount,
                payload.LogoUrl,
                payload.MainPhotoUrl,
                OtherCategoriesJson = otherCategories,
                PlaceTopicsJson = placeTopics
            }, tx, cancellationToken: ct));

        return 1;
    }

    private async Task<BusinessInfoPayload> ResolveBusinessInfoAssetPathsAsync(
        string placeId,
        BusinessInfoPayload payload,
        CancellationToken ct)
    {
        var currentAssets = await GetCurrentPlaceAssetsAsync(placeId, ct);
        var payloadLogoUrl = payload.LogoUrl ?? ExtractImageUrlFromRawJson(payload.RawJson, preferLogo: true);
        var payloadMainPhotoUrl = payload.MainPhotoUrl ?? ExtractImageUrlFromRawJson(payload.RawJson, preferLogo: false);

        var logoSourceUrl = payloadLogoUrl ?? currentAssets?.LogoUrl;
        var mainPhotoSourceUrl = payloadMainPhotoUrl ?? currentAssets?.MainPhotoUrl;

        var resolvedLogoUrl = await DownloadPlaceImageAsync(placeId, logoSourceUrl, "logo", "site-assets/place-logo", ct);
        var resolvedMainPhotoUrl = await DownloadPlaceImageAsync(placeId, mainPhotoSourceUrl, "main-photo", "site-assets/place-main-photo", ct);

        return payload with
        {
            LogoUrl = CoalesceAssetUrl(resolvedLogoUrl, payloadLogoUrl, currentAssets?.LogoUrl),
            MainPhotoUrl = CoalesceAssetUrl(resolvedMainPhotoUrl, payloadMainPhotoUrl, currentAssets?.MainPhotoUrl)
        };
    }

    private async Task<PlaceAssetUrls?> GetCurrentPlaceAssetsAsync(string placeId, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleOrDefaultAsync<PlaceAssetUrls>(new CommandDefinition(@"
SELECT TOP 1
  LogoUrl,
  MainPhotoUrl
FROM dbo.Place
WHERE PlaceId = @PlaceId;", new { PlaceId = placeId }, cancellationToken: ct));
    }

    private async Task<string?> DownloadPlaceImageAsync(
        string placeId,
        string? sourceUrl,
        string kind,
        string relativeDirectory,
        CancellationToken ct)
    {
        var normalizedSourceUrl = NormalizeAbsoluteHttpUrl(sourceUrl);
        if (string.IsNullOrWhiteSpace(normalizedSourceUrl))
            return null;

        var webRootPath = webHostEnvironment.WebRootPath;
        if (string.IsNullOrWhiteSpace(webRootPath) && !string.IsNullOrWhiteSpace(webHostEnvironment.ContentRootPath))
            webRootPath = Path.Combine(webHostEnvironment.ContentRootPath, "wwwroot");
        if (string.IsNullOrWhiteSpace(webRootPath))
            return null;

        var fullDirectory = Path.Combine(
            webRootPath,
            relativeDirectory.Replace('/', Path.DirectorySeparatorChar));
        Directory.CreateDirectory(fullDirectory);

        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, normalizedSourceUrl);
            request.Headers.TryAddWithoutValidation("User-Agent", "LocalSeo.Web/1.0");
            request.Headers.TryAddWithoutValidation("Accept", "image/*,*/*;q=0.8");
            var client = httpClientFactory.CreateClient();
            using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, ct);
            if (!response.IsSuccessStatusCode)
                return null;

            await using var sourceStream = await response.Content.ReadAsStreamAsync(ct);
            using var memory = new MemoryStream();
            await sourceStream.CopyToAsync(memory, ct);
            if (memory.Length == 0)
                return null;

            var extension = GetImageFileExtension(
                response.Content.Headers.ContentType?.MediaType,
                normalizedSourceUrl);
            var fileName = BuildAssetFileName(placeId, kind, normalizedSourceUrl, extension);
            var fullPath = Path.Combine(fullDirectory, fileName);

            await File.WriteAllBytesAsync(fullPath, memory.ToArray(), ct);
            var relativeUrl = "/" + relativeDirectory.Trim('/').Replace("\\", "/") + "/" + fileName;
            return relativeUrl;
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            logger.LogWarning(
                ex,
                "Failed to download place {Kind} image for place {PlaceId} from {Url}.",
                kind,
                placeId,
                normalizedSourceUrl);
            return null;
        }
    }

    private static string? CoalesceAssetUrl(string? downloadedUrl, string? payloadUrl, string? currentUrl)
    {
        if (!string.IsNullOrWhiteSpace(downloadedUrl))
            return downloadedUrl;

        var payloadLocal = NormalizeLocalAssetUrl(payloadUrl);
        if (!string.IsNullOrWhiteSpace(payloadLocal))
            return payloadLocal;

        var currentLocal = NormalizeLocalAssetUrl(currentUrl);
        if (!string.IsNullOrWhiteSpace(currentLocal))
            return currentLocal;

        return NormalizeAbsoluteHttpUrl(payloadUrl)
            ?? NormalizeAbsoluteHttpUrl(currentUrl);
    }

    private static string? NormalizeLocalAssetUrl(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        var normalized = value.Trim().Replace('\\', '/');
        if (normalized.StartsWith("~/", StringComparison.Ordinal))
            normalized = "/" + normalized[2..];

        if (normalized.StartsWith("/", StringComparison.Ordinal))
            return normalized;

        if (normalized.StartsWith("site-assets/", StringComparison.OrdinalIgnoreCase))
            return "/" + normalized;

        if (normalized.StartsWith("wwwroot/", StringComparison.OrdinalIgnoreCase))
            return "/" + normalized["wwwroot/".Length..];

        return null;
    }

    private static string BuildAssetFileName(string placeId, string kind, string sourceUrl, string extension)
    {
        var safePlaceId = SanitizeFilePart(placeId);
        using var sha = SHA256.Create();
        var hashBytes = sha.ComputeHash(Encoding.UTF8.GetBytes(sourceUrl));
        var hash = Convert.ToHexString(hashBytes).ToLowerInvariant();
        return $"{safePlaceId}-{kind}-{hash[..16]}{extension}";
    }

    private static string SanitizeFilePart(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return "place";

        var chars = value
            .Trim()
            .Select(ch => char.IsLetterOrDigit(ch) ? ch : '-')
            .ToArray();
        var normalized = new string(chars).Trim('-');
        return string.IsNullOrWhiteSpace(normalized) ? "place" : normalized.ToLowerInvariant();
    }

    private static string GetImageFileExtension(string? mediaType, string sourceUrl)
    {
        if (!string.IsNullOrWhiteSpace(mediaType))
        {
            var normalizedMediaType = mediaType.Trim().ToLowerInvariant();
            if (normalizedMediaType.Contains("jpeg"))
                return ".jpg";
            if (normalizedMediaType.Contains("png"))
                return ".png";
            if (normalizedMediaType.Contains("webp"))
                return ".webp";
            if (normalizedMediaType.Contains("gif"))
                return ".gif";
            if (normalizedMediaType.Contains("bmp"))
                return ".bmp";
            if (normalizedMediaType.Contains("svg"))
                return ".svg";
            if (normalizedMediaType.Contains("avif"))
                return ".avif";
        }

        if (Uri.TryCreate(sourceUrl, UriKind.Absolute, out var uri))
        {
            var ext = Path.GetExtension(uri.AbsolutePath);
            if (!string.IsNullOrWhiteSpace(ext) && ext.Length <= 10)
                return ext.ToLowerInvariant();
        }

        return ".jpg";
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
                null)
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
                    null)
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
            && !string.IsNullOrWhiteSpace(normalizedLocationName)
            && IsLocationNameFollowupCandidate(created.StatusMessage))
        {
            // Compatibility fallback: some API responses use
            // "Invalid Field: 'location_name'." for missing/required location_name.
            var fallbackWithLocationPayload = new[]
            {
                new MyBusinessInfoKeywordWithLocationTaskPostItem(
                    $"place_id:{placeId}",
                    string.IsNullOrWhiteSpace(cfg.LanguageCode) ? null : cfg.LanguageCode.Trim(),
                    normalizedLocationName,
                    null)
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

    private async Task<TaskCreationResult> CreateMyBusinessUpdatesTaskAsync(
        string placeId,
        string? locationName,
        DataForSeoOptions cfg,
        CancellationToken ct)
    {
        var requestItem = new Dictionary<string, object?>
        {
            ["keyword"] = $"place_id:{placeId}"
        };
        if (!string.IsNullOrWhiteSpace(cfg.LanguageCode))
            requestItem["language_code"] = cfg.LanguageCode.Trim();
        if (!string.IsNullOrWhiteSpace(locationName))
            requestItem["location_name"] = locationName.Trim();
        if (!string.IsNullOrWhiteSpace(cfg.PostbackUrl))
            requestItem["postback_url"] = cfg.PostbackUrl.Trim();
        requestItem["tag"] = placeId;

        var requestPayload = new[] { requestItem };
        using var request = CreateRequest(HttpMethod.Post, BuildApiUrl(cfg.BaseUrl, GetTaskPostPath(cfg, TaskTypeMyBusinessUpdates)), cfg);
        request.Content = JsonContent.Create(requestPayload);

        var client = httpClientFactory.CreateClient();
        using var response = await client.SendAsync(request, ct);
        var body = await response.Content.ReadAsStringAsync(ct);
        if (!response.IsSuccessStatusCode)
            throw new HttpRequestException($"DataForSEO my_business_updates task_post failed with status {(int)response.StatusCode}: {body}");

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

    private async Task<TaskCreationResult> CreateQuestionsAndAnswersTaskAsync(
        string placeId,
        string? locationName,
        DataForSeoOptions cfg,
        CancellationToken ct)
    {
        var requestItem = new Dictionary<string, object?>
        {
            ["keyword"] = $"place_id:{placeId}"
        };
        if (!string.IsNullOrWhiteSpace(cfg.LanguageCode))
            requestItem["language_code"] = cfg.LanguageCode.Trim();
        if (!string.IsNullOrWhiteSpace(locationName))
            requestItem["location_name"] = locationName.Trim();
        if (!string.IsNullOrWhiteSpace(cfg.PostbackUrl))
            requestItem["postback_url"] = cfg.PostbackUrl.Trim();
        requestItem["tag"] = placeId;

        var requestPayload = new[] { requestItem };
        using var request = CreateRequest(HttpMethod.Post, BuildApiUrl(cfg.BaseUrl, GetTaskPostPath(cfg, TaskTypeQuestionsAndAnswers)), cfg);
        request.Content = JsonContent.Create(requestPayload);

        var client = httpClientFactory.CreateClient();
        using var response = await client.SendAsync(request, ct);
        var body = await response.Content.ReadAsStringAsync(ct);
        if (!response.IsSuccessStatusCode)
            throw new HttpRequestException($"DataForSEO questions_and_answers task_post failed with status {(int)response.StatusCode}: {body}");

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

    private async Task FetchAndStoreSocialProfilesAsync(
        string placeId,
        string? locationName,
        DataForSeoOptions cfg,
        CancellationToken ct)
    {
        var seed = await GetSocialProfilesSeedAsync(placeId, ct);
        var keyword = BuildSocialProfilesKeyword(seed, locationName);
        if (string.IsNullOrWhiteSpace(keyword))
            throw new InvalidOperationException("Unable to determine a keyword for social profiles lookup.");

        var requestItem = new Dictionary<string, object?>
        {
            ["keyword"] = keyword,
            ["depth"] = Math.Clamp(cfg.SocialProfilesDepth, 1, 20)
        };

        var effectiveLocationName = FirstNonEmpty(locationName, seed.SearchLocationName, cfg.SocialProfilesLocationName);
        if (!string.IsNullOrWhiteSpace(effectiveLocationName))
            requestItem["location_name"] = effectiveLocationName.Trim();
        if (!string.IsNullOrWhiteSpace(cfg.SocialProfilesLanguageName))
            requestItem["language_name"] = cfg.SocialProfilesLanguageName.Trim();

        var endpoint = BuildApiUrl(cfg.BaseUrl, cfg.SocialProfilesOrganicPath);
        using var request = CreateRequest(HttpMethod.Post, endpoint, cfg);
        request.Content = JsonContent.Create(new[] { requestItem });

        var client = httpClientFactory.CreateClient();
        using var response = await client.SendAsync(request, ct);
        var body = await response.Content.ReadAsStringAsync(ct);
        if (!response.IsSuccessStatusCode)
            throw new HttpRequestException($"DataForSEO social_profiles request failed with status {(int)response.StatusCode}: {body}");

        using var document = JsonDocument.Parse(body);
        var root = document.RootElement;
        if (!root.TryGetProperty("tasks", out var tasks) || tasks.ValueKind != JsonValueKind.Array || tasks.GetArrayLength() == 0)
            throw new InvalidOperationException("DataForSEO social_profiles response did not include any tasks.");

        var task = tasks[0];
        var taskStatusCode = task.TryGetProperty("status_code", out var statusCodeNode) && statusCodeNode.TryGetInt32(out var code)
            ? code
            : 0;
        var taskStatusMessage = task.TryGetProperty("status_message", out var statusMessageNode)
            ? statusMessageNode.GetString()
            : null;
        var taskId = task.TryGetProperty("id", out var idNode) ? idNode.GetString() : null;

        var socialUrls = ExtractSocialUrlsFromSerpTask(task);
        await UpsertSocialProfilesAsync(placeId, socialUrls, ct);

        var status = taskStatusCode is >= 20000 and < 30000
            ? socialUrls.HasAny ? "Populated" : "NoData"
            : "Error";
        var statusMessage = taskStatusMessage;
        if (status == "NoData" && string.IsNullOrWhiteSpace(statusMessage))
            statusMessage = "No social profile URLs found in SERP results.";

        await UpsertTaskRowAsync(
            placeId,
            seed.SearchLocationName,
            TaskTypeSocialProfiles,
            string.IsNullOrWhiteSpace(taskId) ? $"{TaskTypeSocialProfiles}-live-{Guid.NewGuid():N}" : taskId!,
            status,
            taskStatusCode,
            statusMessage,
            endpoint,
            status == "Error" ? statusMessage : null,
            ct);
    }

    private async Task<SocialProfilesSeedRow> GetSocialProfilesSeedAsync(string placeId, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var row = await conn.QuerySingleOrDefaultAsync<SocialProfilesSeedRow>(new CommandDefinition(@"
SELECT TOP 1
  PlaceId,
  DisplayName,
  NationalPhoneNumber,
  WebsiteUri,
  SearchLocationName
FROM dbo.Place
WHERE PlaceId=@PlaceId;", new { PlaceId = placeId }, cancellationToken: ct));

        if (row is null)
            throw new InvalidOperationException($"Place '{placeId}' was not found.");

        return row;
    }

    private async Task UpsertSocialProfilesAsync(string placeId, SocialUrlSet socialUrls, CancellationToken ct)
    {
        var socialPayload = socialUrls.HasAny
            ? JsonSerializer.Serialize(socialUrls.AsDictionary())
            : null;

        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.Place
SET
  FacebookUrl = COALESCE(@FacebookUrl, FacebookUrl),
  InstagramUrl = COALESCE(@InstagramUrl, InstagramUrl),
  LinkedInUrl = COALESCE(@LinkedInUrl, LinkedInUrl),
  XUrl = COALESCE(@XUrl, XUrl),
  YouTubeUrl = COALESCE(@YouTubeUrl, YouTubeUrl),
  TikTokUrl = COALESCE(@TikTokUrl, TikTokUrl),
  PinterestUrl = COALESCE(@PinterestUrl, PinterestUrl),
  BlueskyUrl = COALESCE(@BlueskyUrl, BlueskyUrl),
  SocialProfilesJson = COALESCE(@SocialProfilesJson, SocialProfilesJson),
  LastSeenUtc = SYSUTCDATETIME()
WHERE PlaceId = @PlaceId;", new
        {
            PlaceId = placeId,
            socialUrls.FacebookUrl,
            socialUrls.InstagramUrl,
            socialUrls.LinkedInUrl,
            socialUrls.XUrl,
            socialUrls.YouTubeUrl,
            socialUrls.TikTokUrl,
            socialUrls.PinterestUrl,
            socialUrls.BlueskyUrl,
            SocialProfilesJson = socialPayload
        }, cancellationToken: ct));
    }

    private static string BuildSocialProfilesKeyword(SocialProfilesSeedRow seed, string? requestedLocationName)
    {
        var displayName = NormalizeText(seed.DisplayName, 300);
        var fallbackName = ExtractDomainToken(seed.WebsiteUri);
        var baseName = FirstNonEmpty(displayName, fallbackName, seed.PlaceId);
        var phoneNumber = NormalizePhoneForSearch(seed.NationalPhoneNumber);
        var town = NormalizeTownForSearch(FirstNonEmpty(requestedLocationName, seed.SearchLocationName));

        if (string.IsNullOrWhiteSpace(baseName)
            || string.IsNullOrWhiteSpace(phoneNumber)
            || string.IsNullOrWhiteSpace(town))
        {
            return string.Empty;
        }

        return $"{baseName} {phoneNumber} {town}";
    }

    private static SocialUrlSet ExtractSocialUrlsFromSerpTask(JsonElement task)
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        if (!task.TryGetProperty("result", out var resultArray) || resultArray.ValueKind != JsonValueKind.Array)
            return SocialUrlSet.Empty;

        foreach (var result in resultArray.EnumerateArray())
            ExtractSocialUrlsFromNode(result, map, 0);

        return new SocialUrlSet(
            GetFromMap(map, "facebook"),
            GetFromMap(map, "instagram"),
            GetFromMap(map, "linkedin"),
            GetFromMap(map, "x"),
            GetFromMap(map, "youtube"),
            GetFromMap(map, "tiktok"),
            GetFromMap(map, "pinterest"),
            GetFromMap(map, "bluesky"));
    }

    private static void ExtractSocialUrlsFromNode(JsonElement node, IDictionary<string, string> map, int depth)
    {
        if (depth > 8 || map.Count >= 8)
            return;

        switch (node.ValueKind)
        {
            case JsonValueKind.String:
                TryCaptureSocialUrl(node.GetString(), map);
                return;
            case JsonValueKind.Array:
                foreach (var item in node.EnumerateArray())
                    ExtractSocialUrlsFromNode(item, map, depth + 1);
                return;
            case JsonValueKind.Object:
                foreach (var prop in node.EnumerateObject())
                    ExtractSocialUrlsFromNode(prop.Value, map, depth + 1);
                return;
        }
    }

    private static void TryCaptureSocialUrl(string? candidate, IDictionary<string, string> map)
    {
        var normalized = NormalizeAbsoluteHttpUrl(candidate);
        if (string.IsNullOrWhiteSpace(normalized) || !Uri.TryCreate(normalized, UriKind.Absolute, out var uri))
            return;

        var platform = ResolveSocialPlatform(uri);
        if (platform is null || map.ContainsKey(platform))
            return;

        map[platform] = uri.AbsoluteUri.TrimEnd('/');
    }

    private static string? ResolveSocialPlatform(Uri uri)
    {
        var host = uri.Host.Trim().ToLowerInvariant();
        if (host.StartsWith("www.", StringComparison.Ordinal))
            host = host[4..];

        if (host.EndsWith("facebook.com", StringComparison.Ordinal))
            return "facebook";
        if (host.EndsWith("instagram.com", StringComparison.Ordinal))
            return "instagram";
        if (host.EndsWith("linkedin.com", StringComparison.Ordinal))
            return "linkedin";
        if (host.EndsWith("x.com", StringComparison.Ordinal) || host.EndsWith("twitter.com", StringComparison.Ordinal))
            return "x";
        if (host.EndsWith("youtube.com", StringComparison.Ordinal) || host.EndsWith("youtu.be", StringComparison.Ordinal))
            return "youtube";
        if (host.EndsWith("tiktok.com", StringComparison.Ordinal))
            return "tiktok";
        if (host.EndsWith("pinterest.com", StringComparison.Ordinal))
            return "pinterest";
        if (host.EndsWith("bsky.app", StringComparison.Ordinal) || host.EndsWith("bluesky.social", StringComparison.Ordinal))
            return "bluesky";

        return null;
    }

    private static string? GetFromMap(IReadOnlyDictionary<string, string> map, string key)
        => map.TryGetValue(key, out var value) ? value : null;

    private static string? FirstNonEmpty(params string?[] values)
        => values.FirstOrDefault(x => !string.IsNullOrWhiteSpace(x))?.Trim();

    private static string? NormalizeText(string? value, int maxLength)
    {
        var trimmed = (value ?? string.Empty).Trim();
        if (trimmed.Length == 0)
            return null;
        return trimmed.Length <= maxLength ? trimmed : trimmed[..maxLength];
    }

    private static string? ExtractDomainToken(string? websiteUri)
    {
        var normalized = NormalizeAbsoluteHttpUrl(websiteUri);
        if (string.IsNullOrWhiteSpace(normalized) || !Uri.TryCreate(normalized, UriKind.Absolute, out var uri))
            return null;

        var host = uri.Host.Trim().ToLowerInvariant();
        if (host.StartsWith("www.", StringComparison.Ordinal))
            host = host[4..];
        if (host.Length == 0)
            return null;

        var token = host.Split('.', StringSplitOptions.RemoveEmptyEntries).FirstOrDefault();
        return NormalizeText(token, 120);
    }

    private static string? NormalizeTownForSearch(string? locationName)
    {
        var normalized = NormalizeText(locationName, 200);
        if (string.IsNullOrWhiteSpace(normalized))
            return null;

        var town = normalized;
        var commaIndex = town.IndexOf(',', StringComparison.Ordinal);
        if (commaIndex >= 0)
            town = town[..commaIndex];

        var cleanedChars = town
            .Where(c => char.IsLetterOrDigit(c) || c == '\'' || c == '-' || char.IsWhiteSpace(c))
            .ToArray();
        if (cleanedChars.Length == 0)
            return null;

        var compact = string.Join(' ', new string(cleanedChars)
            .Split(' ', StringSplitOptions.RemoveEmptyEntries));
        return NormalizeText(compact, 120);
    }

    private static string? NormalizePhoneForSearch(string? phoneNumber)
    {
        if (string.IsNullOrWhiteSpace(phoneNumber))
            return null;

        var groups = new List<string>();
        var current = new StringBuilder();
        foreach (var ch in phoneNumber)
        {
            if (char.IsDigit(ch))
            {
                current.Append(ch);
                continue;
            }

            if (current.Length > 0)
            {
                groups.Add(current.ToString());
                current.Clear();
            }
        }

        if (current.Length > 0)
            groups.Add(current.ToString());
        if (groups.Count == 0)
            return null;

        return string.Join(' ', groups);
    }

    private static bool IsLocationNameFollowupCandidate(string? statusMessage)
    {
        if (string.IsNullOrWhiteSpace(statusMessage))
            return false;

        var message = statusMessage.Trim();
        return message.Contains("location_name", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsNoDataStatus(int? statusCode, string? statusMessage)
    {
        if (!string.IsNullOrWhiteSpace(statusMessage)
            && statusMessage.Contains("No Search Results", StringComparison.OrdinalIgnoreCase))
            return true;

        return false;
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
            return new BusinessInfoTaskSnapshot(0, "No tasks in response.", false, false, new BusinessInfoPayload(null, null, [], [], null, null, null));

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
            payload = new BusinessInfoPayload(null, null, [], [], null, null, null);

        return new BusinessInfoTaskSnapshot(taskStatusCode, taskStatusMessage, isCompleted, payload.HasValues, payload);
    }

    private static MyBusinessUpdatesTaskSnapshot ParseMyBusinessUpdatesSnapshot(string body)
    {
        var updates = new Dictionary<string, UpdatePayload>(StringComparer.Ordinal);
        using var document = JsonDocument.Parse(body);
        var root = document.RootElement;
        if (!root.TryGetProperty("tasks", out var tasks) || tasks.ValueKind != JsonValueKind.Array || tasks.GetArrayLength() == 0)
            return new MyBusinessUpdatesTaskSnapshot(0, "No tasks in response.", false, updates);

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
                foreach (var item in EnumerateMyBusinessUpdatesCandidates(result))
                {
                    var payload = ParseMyBusinessUpdate(item);
                    if (payload is null)
                        continue;
                    updates[payload.UpdateKey] = payload;
                }
            }
        }

        var isCompleted = taskStatusCode == 20000 && resultCount >= 0;
        return new MyBusinessUpdatesTaskSnapshot(taskStatusCode, taskStatusMessage, isCompleted, updates);
    }

    private static QuestionsAndAnswersTaskSnapshot ParseQuestionsAndAnswersSnapshot(string body)
    {
        var items = new Dictionary<string, QuestionAnswerPayload>(StringComparer.Ordinal);
        using var document = JsonDocument.Parse(body);
        var root = document.RootElement;
        if (!root.TryGetProperty("tasks", out var tasks) || tasks.ValueKind != JsonValueKind.Array || tasks.GetArrayLength() == 0)
            return new QuestionsAndAnswersTaskSnapshot(0, "No tasks in response.", false, items);

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
                foreach (var question in EnumerateQuestionsAndAnswersCandidates(result))
                {
                    foreach (var payload in ParseQuestionAnswerPayloads(question))
                    {
                        items[payload.QaKey] = payload;
                    }
                }
            }
        }

        var isCompleted = taskStatusCode == 20000 && resultCount >= 0;
        return new QuestionsAndAnswersTaskSnapshot(taskStatusCode, taskStatusMessage, isCompleted, items);
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

    private static IEnumerable<JsonElement> EnumerateMyBusinessUpdatesCandidates(JsonElement result)
    {
        if (!result.TryGetProperty("items", out var items))
            yield break;

        if (items.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in items.EnumerateArray())
            {
                if (item.ValueKind == JsonValueKind.Object)
                    yield return item;
            }
            yield break;
        }

        if (items.ValueKind == JsonValueKind.Object)
        {
            foreach (var nestedName in new[] { "updates", "posts", "items" })
            {
                if (!items.TryGetProperty(nestedName, out var nestedNode) || nestedNode.ValueKind != JsonValueKind.Array)
                    continue;

                foreach (var item in nestedNode.EnumerateArray())
                {
                    if (item.ValueKind == JsonValueKind.Object)
                        yield return item;
                }
                yield break;
            }

            foreach (var property in items.EnumerateObject())
            {
                if (property.Value.ValueKind != JsonValueKind.Array)
                    continue;

                foreach (var item in property.Value.EnumerateArray())
                {
                    if (item.ValueKind == JsonValueKind.Object)
                        yield return item;
                }
            }

            yield return items;
        }
    }

    private static IEnumerable<JsonElement> EnumerateQuestionsAndAnswersCandidates(JsonElement result)
    {
        if (!result.TryGetProperty("items", out var items))
            yield break;

        if (items.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in items.EnumerateArray())
            {
                if (item.ValueKind == JsonValueKind.Object)
                    yield return item;
            }
            yield break;
        }

        if (items.ValueKind == JsonValueKind.Object)
        {
            foreach (var nestedName in new[] { "questions", "items" })
            {
                if (!items.TryGetProperty(nestedName, out var nestedNode) || nestedNode.ValueKind != JsonValueKind.Array)
                    continue;

                foreach (var item in nestedNode.EnumerateArray())
                {
                    if (item.ValueKind == JsonValueKind.Object)
                        yield return item;
                }
                yield break;
            }

            foreach (var property in items.EnumerateObject())
            {
                if (property.Value.ValueKind != JsonValueKind.Array)
                    continue;

                foreach (var item in property.Value.EnumerateArray())
                {
                    if (item.ValueKind == JsonValueKind.Object)
                        yield return item;
                }
            }

            yield return items;
        }
    }

    private static IEnumerable<QuestionAnswerPayload> ParseQuestionAnswerPayloads(JsonElement question)
    {
        var questionText = GetString(question, "question_text") ?? GetString(question, "text");
        var questionTimestampUtc = ParseTimestampFromProperty(question, "timestamp");
        var questionProfileName = GetString(question, "profile_name");
        var questionIdentity = GetString(question, "question_id")
            ?? GetString(question, "id")
            ?? string.Empty;

        var answers = new List<(string? AnswerText, DateTime? AnswerTimestampUtc, string? AnswerProfileName, string RawJson)>();
        if (question.TryGetProperty("items", out var answersNode) && answersNode.ValueKind == JsonValueKind.Array)
        {
            foreach (var answer in answersNode.EnumerateArray())
            {
                if (answer.ValueKind != JsonValueKind.Object)
                    continue;

                answers.Add((
                    GetString(answer, "answer_text") ?? GetString(answer, "text"),
                    ParseTimestampFromProperty(answer, "timestamp"),
                    GetString(answer, "profile_name"),
                    answer.GetRawText()));
            }
        }

        if (answers.Count == 0)
        {
            var seedNoAnswer = $"{questionIdentity}|{questionText}|{questionTimestampUtc:O}|{questionProfileName}|no-answer";
            if (string.IsNullOrWhiteSpace(seedNoAnswer.Replace("|", string.Empty, StringComparison.Ordinal)))
                yield break;

            yield return new QuestionAnswerPayload(
                BuildUpdateKey(seedNoAnswer),
                questionText,
                questionTimestampUtc,
                questionProfileName,
                null,
                null,
                null,
                question.GetRawText());
            yield break;
        }

        foreach (var answer in answers)
        {
            var seed = $"{questionIdentity}|{questionText}|{questionTimestampUtc:O}|{questionProfileName}|{answer.AnswerText}|{answer.AnswerTimestampUtc:O}|{answer.AnswerProfileName}";
            if (string.IsNullOrWhiteSpace(seed.Replace("|", string.Empty, StringComparison.Ordinal)))
                continue;

            yield return new QuestionAnswerPayload(
                BuildUpdateKey(seed),
                questionText,
                questionTimestampUtc,
                questionProfileName,
                answer.AnswerText,
                answer.AnswerTimestampUtc,
                answer.AnswerProfileName,
                answer.RawJson);
        }
    }

    private static BusinessInfoPayload ParseBusinessInfo(JsonElement item)
    {
        var description = GetString(item, "description");
        if (string.IsNullOrWhiteSpace(description))
            description = GetString(item, "about");

        var additionalCategories = ExtractStringList(item, "additional_categories");
        var placeTopics = ExtractStringList(item, "place_topics");
        var logoUrl = ExtractFirstImageUrl(item,
            "logo_url",
            "logo_image_url",
            "logo",
            "profile_image_url",
            "profile_image");
        var mainPhotoUrl = ExtractFirstImageUrl(item,
            "main_photo_url",
            "main_photo",
            "cover_photo_url",
            "cover_photo",
            "main_image_url",
            "main_image",
            "featured_image",
            "photo_url",
            "image_url",
            "photo",
            "image",
            "photos",
            "images",
            "images_url",
            "photos_data",
            "media");

        return new BusinessInfoPayload(
            TruncateText(description, 750),
            GetInt(item, "total_photos"),
            additionalCategories,
            placeTopics,
            logoUrl,
            mainPhotoUrl,
            item.GetRawText());
    }

    private static string? ExtractImageUrlFromRawJson(string? rawJson, bool preferLogo)
    {
        if (string.IsNullOrWhiteSpace(rawJson))
            return null;

        try
        {
            using var document = JsonDocument.Parse(rawJson);
            var root = document.RootElement;
            var propertyCandidates = preferLogo
                ? new[]
                {
                    "logo_url", "logo_image_url", "logo", "profile_image_url", "profile_image", "brand_logo", "icon_url", "icon"
                }
                : new[]
                {
                    "main_photo_url", "main_photo", "cover_photo_url", "cover_photo", "main_image_url", "main_image",
                    "featured_image", "photo_url", "image_url", "photo", "image", "photos", "images", "images_url", "photos_data", "media"
                };
            return ExtractFirstImageUrl(root, propertyCandidates) ?? ExtractImageUrlFromNode(root, 0);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static string? ExtractFirstImageUrl(JsonElement item, params string[] propertyCandidates)
    {
        if (item.ValueKind != JsonValueKind.Object)
            return ExtractImageUrlFromNode(item, 0);

        foreach (var propertyName in propertyCandidates)
        {
            if (!item.TryGetProperty(propertyName, out var node) || node.ValueKind == JsonValueKind.Null)
                continue;

            var url = ExtractImageUrlFromNode(node, 0);
            if (!string.IsNullOrWhiteSpace(url))
                return url;
        }

        return null;
    }

    private static string? ExtractImageUrlFromNode(JsonElement node, int depth)
    {
        if (depth > 5)
            return null;

        if (node.ValueKind == JsonValueKind.String)
            return NormalizeAbsoluteHttpUrl(node.GetString());

        if (node.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in node.EnumerateArray())
            {
                var nested = ExtractImageUrlFromNode(item, depth + 1);
                if (!string.IsNullOrWhiteSpace(nested))
                    return nested;
            }

            return null;
        }

        if (node.ValueKind != JsonValueKind.Object)
            return null;

        foreach (var key in new[]
        {
            "url",
            "image_url",
            "logo_url",
            "main_photo_url",
            "main_image_url",
            "thumbnail_url",
            "icon_url",
            "photo_url",
            "src",
            "value"
        })
        {
            var value = GetString(node, key);
            var normalized = NormalizeAbsoluteHttpUrl(value);
            if (!string.IsNullOrWhiteSpace(normalized))
                return normalized;
        }

        foreach (var key in new[]
        {
            "image",
            "logo",
            "photo",
            "main_photo",
            "cover_photo",
            "main_image",
            "featured_image",
            "images",
            "photos",
            "thumbnails",
            "media",
            "photos_data",
            "items",
            "gallery"
        })
        {
            if (!node.TryGetProperty(key, out var nestedNode) || nestedNode.ValueKind == JsonValueKind.Null)
                continue;

            var nested = ExtractImageUrlFromNode(nestedNode, depth + 1);
            if (!string.IsNullOrWhiteSpace(nested))
                return nested;
        }

        foreach (var prop in node.EnumerateObject())
        {
            if (prop.Value.ValueKind is JsonValueKind.Object or JsonValueKind.Array)
            {
                var name = prop.Name.ToLowerInvariant();
                if (name.Contains("image", StringComparison.Ordinal)
                    || name.Contains("photo", StringComparison.Ordinal)
                    || name.Contains("logo", StringComparison.Ordinal)
                    || name.Contains("cover", StringComparison.Ordinal)
                    || name.Contains("icon", StringComparison.Ordinal)
                    || name.Contains("picture", StringComparison.Ordinal)
                    || name.Contains("media", StringComparison.Ordinal))
                {
                    var nested = ExtractImageUrlFromNode(prop.Value, depth + 1);
                    if (!string.IsNullOrWhiteSpace(nested))
                        return nested;
                }
            }
            else if (prop.Value.ValueKind == JsonValueKind.String)
            {
                var name = prop.Name.ToLowerInvariant();
                if (name.Contains("image", StringComparison.Ordinal)
                    || name.Contains("photo", StringComparison.Ordinal)
                    || name.Contains("logo", StringComparison.Ordinal)
                    || name.Contains("cover", StringComparison.Ordinal)
                    || name.Contains("icon", StringComparison.Ordinal)
                    || name.Contains("picture", StringComparison.Ordinal))
                {
                    var normalized = NormalizeAbsoluteHttpUrl(prop.Value.GetString());
                    if (!string.IsNullOrWhiteSpace(normalized))
                        return normalized;
                }
            }
        }

        return null;
    }

    private static UpdatePayload? ParseMyBusinessUpdate(JsonElement item)
    {
        var postText = GetString(item, "post_text")
            ?? GetString(item, "description")
            ?? GetString(item, "text");
        var url = GetString(item, "url");
        var postDateUtc = ParseTimestampFromProperty(item, "timestamp")
            ?? ParseTimestampFromProperty(item, "post_date")
            ?? ParseTimestampFromProperty(item, "posted_at")
            ?? ParseTimestampFromProperty(item, "date")
            ?? ParseTimestampFromProperty(item, "date_posted");
        var imageUrls = ExtractStringList(item, "images_url");
        var links = ExtractLinks(item);

        var identitySeed = GetString(item, "update_id")
            ?? GetString(item, "post_id")
            ?? GetString(item, "cid")
            ?? GetString(item, "id");

        if (string.IsNullOrWhiteSpace(identitySeed))
        {
            var seedParts = $"{postDateUtc:O}|{url}|{postText}";
            if (string.IsNullOrWhiteSpace(seedParts.Replace("|", string.Empty, StringComparison.Ordinal)))
                return null;
            identitySeed = seedParts;
        }

        var updateKey = BuildUpdateKey(identitySeed);
        if (string.IsNullOrWhiteSpace(updateKey))
            return null;

        return new UpdatePayload(
            updateKey,
            postText,
            url,
            imageUrls.Count == 0 ? "[]" : JsonSerializer.Serialize(imageUrls),
            postDateUtc,
            links.Count == 0 ? "[]" : JsonSerializer.Serialize(links),
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
                        ?? GetString(item, "url")
                        ?? GetString(item, "image_url")
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
                    ?? GetString(node, "url")
                    ?? GetString(node, "image_url")
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

    private static IReadOnlyList<UpdateLinkPayload> ExtractLinks(JsonElement obj)
    {
        if (!obj.TryGetProperty("links", out var node) || node.ValueKind == JsonValueKind.Null)
            return [];

        if (node.ValueKind != JsonValueKind.Array)
            return [];

        var links = new List<UpdateLinkPayload>();
        foreach (var item in node.EnumerateArray())
        {
            if (item.ValueKind != JsonValueKind.Object)
                continue;

            var type = GetString(item, "type");
            var title = GetString(item, "title");
            var url = GetString(item, "url");
            if (string.IsNullOrWhiteSpace(type) && string.IsNullOrWhiteSpace(title) && string.IsNullOrWhiteSpace(url))
                continue;

            links.Add(new UpdateLinkPayload(
                string.IsNullOrWhiteSpace(type) ? null : type.Trim(),
                string.IsNullOrWhiteSpace(title) ? null : title.Trim(),
                string.IsNullOrWhiteSpace(url) ? null : url.Trim()));
        }

        return links;
    }

    private static string BuildUpdateKey(string value)
    {
        var normalized = value.Trim();
        if (string.IsNullOrWhiteSpace(normalized))
            return string.Empty;

        using var sha = SHA256.Create();
        var hash = sha.ComputeHash(Encoding.UTF8.GetBytes(normalized));
        return Convert.ToHexString(hash).ToLowerInvariant();
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
        var format = normalizedTaskType switch
        {
            TaskTypeMyBusinessInfo => cfg.MyBusinessInfoTaskGetPathFormat,
            TaskTypeMyBusinessUpdates => cfg.MyBusinessUpdatesTaskGetPathFormat,
            TaskTypeQuestionsAndAnswers => cfg.QuestionsAndAnswersTaskGetPathFormat,
            _ => cfg.TaskGetPathFormat
        };

        return string.Format(CultureInfo.InvariantCulture, format, taskId);
    }

    private static string GetTasksReadyPath(DataForSeoOptions cfg, string taskType)
    {
        var normalizedTaskType = NormalizeTaskType(taskType);
        return normalizedTaskType switch
        {
            TaskTypeMyBusinessInfo => cfg.MyBusinessInfoTasksReadyPath,
            TaskTypeMyBusinessUpdates => cfg.MyBusinessUpdatesTasksReadyPath,
            TaskTypeQuestionsAndAnswers => cfg.QuestionsAndAnswersTasksReadyPath,
            _ => cfg.TasksReadyPath
        };
    }

    private static string GetTaskPostPath(DataForSeoOptions cfg, string taskType)
    {
        var normalizedTaskType = NormalizeTaskType(taskType);
        return normalizedTaskType switch
        {
            TaskTypeMyBusinessInfo => cfg.MyBusinessInfoTaskPostPath,
            TaskTypeMyBusinessUpdates => cfg.MyBusinessUpdatesTaskPostPath,
            TaskTypeQuestionsAndAnswers => cfg.QuestionsAndAnswersTaskPostPath,
            _ => cfg.TaskPostPath
        };
    }

    private static string NormalizeTaskType(string? taskType)
    {
        if (string.Equals(taskType, TaskTypeMyBusinessInfo, StringComparison.OrdinalIgnoreCase))
            return TaskTypeMyBusinessInfo;
        if (string.Equals(taskType, TaskTypeMyBusinessUpdates, StringComparison.OrdinalIgnoreCase))
            return TaskTypeMyBusinessUpdates;
        if (string.Equals(taskType, TaskTypeQuestionsAndAnswers, StringComparison.OrdinalIgnoreCase))
            return TaskTypeQuestionsAndAnswers;
        if (string.Equals(taskType, TaskTypeSocialProfiles, StringComparison.OrdinalIgnoreCase))
            return TaskTypeSocialProfiles;
        return TaskTypeReviews;
    }

    private static string? NormalizeTaskStatus(string? status)
    {
        if (string.IsNullOrWhiteSpace(status) || string.Equals(status, "all", StringComparison.OrdinalIgnoreCase))
            return null;

        var normalized = status.Trim();
        if (normalized.Length > 40)
            normalized = normalized[..40];
        return normalized;
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

        return node.ValueKind switch
        {
            JsonValueKind.String => node.GetString(),
            JsonValueKind.Number => node.ToString(),
            JsonValueKind.True => "true",
            JsonValueKind.False => "false",
            _ => null
        };
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

    private static string? NormalizeAbsoluteHttpUrl(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        var normalized = value.Trim();
        if (normalized.StartsWith("//", StringComparison.Ordinal))
            normalized = "https:" + normalized;

        if (!Uri.TryCreate(normalized, UriKind.Absolute, out var uri))
            return null;
        if (!string.Equals(uri.Scheme, Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase)
            && !string.Equals(uri.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        return uri.AbsoluteUri;
    }

    private static DateTime? ParseTimestamp(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        var trimmed = value.Trim();
        if (long.TryParse(trimmed, NumberStyles.Integer, CultureInfo.InvariantCulture, out var epoch))
        {
            // Only treat clear epoch formats as unix timestamps.
            if (trimmed.Length == 13)
                return DateTimeOffset.FromUnixTimeMilliseconds(epoch).UtcDateTime;
            if (trimmed.Length == 10)
                return DateTimeOffset.FromUnixTimeSeconds(epoch).UtcDateTime;
        }
        if (!DateTimeOffset.TryParse(trimmed, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var dto))
            return null;
        return dto.UtcDateTime;
    }

    private static DateTime? ParseTimestampFromProperty(JsonElement obj, string propertyName)
    {
        if (!obj.TryGetProperty(propertyName, out var node) || node.ValueKind == JsonValueKind.Null)
            return null;

        if (node.ValueKind == JsonValueKind.String || node.ValueKind == JsonValueKind.Number)
            return ParseTimestamp(node.ToString());

        if (node.ValueKind != JsonValueKind.Object)
            return null;

        foreach (var nestedName in new[] { "timestamp", "post_date", "date", "datetime", "posted_at", "time", "date_posted" })
        {
            if (!node.TryGetProperty(nestedName, out var nested) || nested.ValueKind == JsonValueKind.Null)
                continue;

            if (nested.ValueKind == JsonValueKind.String || nested.ValueKind == JsonValueKind.Number)
                return ParseTimestamp(nested.ToString());
        }

        return null;
    }

    private static string? TruncateText(string? value, int maxLength)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;
        var trimmed = value.Trim();
        return trimmed.Length <= maxLength ? trimmed : trimmed[..maxLength];
    }

    private sealed record SocialProfilesSeedRow(
        string PlaceId,
        string? DisplayName,
        string? NationalPhoneNumber,
        string? WebsiteUri,
        string? SearchLocationName);

    private sealed record SocialUrlSet(
        string? FacebookUrl,
        string? InstagramUrl,
        string? LinkedInUrl,
        string? XUrl,
        string? YouTubeUrl,
        string? TikTokUrl,
        string? PinterestUrl,
        string? BlueskyUrl)
    {
        public static SocialUrlSet Empty { get; } = new(null, null, null, null, null, null, null, null);

        public bool HasAny =>
            !string.IsNullOrWhiteSpace(FacebookUrl)
            || !string.IsNullOrWhiteSpace(InstagramUrl)
            || !string.IsNullOrWhiteSpace(LinkedInUrl)
            || !string.IsNullOrWhiteSpace(XUrl)
            || !string.IsNullOrWhiteSpace(YouTubeUrl)
            || !string.IsNullOrWhiteSpace(TikTokUrl)
            || !string.IsNullOrWhiteSpace(PinterestUrl)
            || !string.IsNullOrWhiteSpace(BlueskyUrl);

        public IReadOnlyDictionary<string, string> AsDictionary()
        {
            var dict = new Dictionary<string, string>(StringComparer.Ordinal);
            if (!string.IsNullOrWhiteSpace(FacebookUrl)) dict["facebook"] = FacebookUrl;
            if (!string.IsNullOrWhiteSpace(InstagramUrl)) dict["instagram"] = InstagramUrl;
            if (!string.IsNullOrWhiteSpace(LinkedInUrl)) dict["linkedin"] = LinkedInUrl;
            if (!string.IsNullOrWhiteSpace(XUrl)) dict["x"] = XUrl;
            if (!string.IsNullOrWhiteSpace(YouTubeUrl)) dict["youtube"] = YouTubeUrl;
            if (!string.IsNullOrWhiteSpace(TikTokUrl)) dict["tiktok"] = TikTokUrl;
            if (!string.IsNullOrWhiteSpace(PinterestUrl)) dict["pinterest"] = PinterestUrl;
            if (!string.IsNullOrWhiteSpace(BlueskyUrl)) dict["bluesky"] = BlueskyUrl;
            return dict;
        }
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

    private sealed record UpdatePayload(
        string UpdateKey,
        string? PostText,
        string? Url,
        string ImagesUrlJson,
        DateTime? PostDateUtc,
        string LinksJson,
        string RawJson);

    private sealed record MyBusinessUpdatesTaskSnapshot(
        int StatusCode,
        string? StatusMessage,
        bool IsCompleted,
        Dictionary<string, UpdatePayload> Updates);

    private sealed record QuestionAnswerPayload(
        string QaKey,
        string? QuestionText,
        DateTime? QuestionTimestampUtc,
        string? QuestionProfileName,
        string? AnswerText,
        DateTime? AnswerTimestampUtc,
        string? AnswerProfileName,
        string RawJson);

    private sealed record QuestionsAndAnswersTaskSnapshot(
        int StatusCode,
        string? StatusMessage,
        bool IsCompleted,
        Dictionary<string, QuestionAnswerPayload> Items);

    private sealed record UpdateLinkPayload(
        string? Type,
        string? Title,
        string? Url);

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

    private sealed record PlaceAssetUrls(
        string? LogoUrl,
        string? MainPhotoUrl);

    private sealed record BusinessInfoPayload(
        string? Description,
        int? PhotoCount,
        IReadOnlyList<string> AdditionalCategories,
        IReadOnlyList<string> PlaceTopics,
        string? LogoUrl,
        string? MainPhotoUrl,
        string? RawJson)
    {
        public bool HasValues =>
            !string.IsNullOrWhiteSpace(Description)
            || PhotoCount.HasValue
            || AdditionalCategories.Count > 0
            || PlaceTopics.Count > 0
            || !string.IsNullOrWhiteSpace(LogoUrl)
            || !string.IsNullOrWhiteSpace(MainPhotoUrl);
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
