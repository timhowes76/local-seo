using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace LocalSeo.Web.Services;

public sealed class SqlConnectionHealthCheck(IConfiguration configuration) : IHealthCheck
{
    private const int ConnectTimeoutSeconds = 5;
    private const int CommandTimeoutSeconds = 3;

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        var connectionString = configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrWhiteSpace(connectionString))
            connectionString = configuration["ConnectionStrings:DefaultConnection"];

        if (string.IsNullOrWhiteSpace(connectionString))
            return HealthCheckResult.Unhealthy("Missing connection string");

        try
        {
            var builder = new SqlConnectionStringBuilder(connectionString)
            {
                ConnectTimeout = ConnectTimeoutSeconds
            };

            await using var connection = new SqlConnection(builder.ConnectionString);
            await connection.OpenAsync(cancellationToken);

            await using var command = connection.CreateCommand();
            command.CommandText = "SELECT 1";
            command.CommandTimeout = CommandTimeoutSeconds;

            await command.ExecuteScalarAsync(cancellationToken);
            return HealthCheckResult.Healthy();
        }
        catch
        {
            return HealthCheckResult.Unhealthy("Database unreachable");
        }
    }
}
