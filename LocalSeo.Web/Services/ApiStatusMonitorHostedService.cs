namespace LocalSeo.Web.Services;

public sealed class ApiStatusMonitorHostedService(
    IServiceScopeFactory scopeFactory,
    ILogger<ApiStatusMonitorHostedService> logger) : BackgroundService
{
    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(30);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            using var warmScope = scopeFactory.CreateScope();
            var service = warmScope.ServiceProvider.GetRequiredService<IApiStatusService>();
            await service.SeedDefinitionsAsync(stoppingToken);
            await service.WarmLatestCacheAsync(stoppingToken);
        }
        catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
        {
            logger.LogWarning(ex, "API status monitor warmup failed.");
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var scope = scopeFactory.CreateScope();
                var service = scope.ServiceProvider.GetRequiredService<IApiStatusService>();
                var refreshed = await service.RefreshStaleChecksAsync(forceAll: false, stoppingToken);
                if (refreshed > 0)
                    logger.LogDebug("API status monitor refreshed {Count} checks.", refreshed);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "API status monitor iteration failed.");
            }

            try
            {
                await Task.Delay(PollInterval, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
    }
}

