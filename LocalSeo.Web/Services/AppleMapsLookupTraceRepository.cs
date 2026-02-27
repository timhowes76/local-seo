using Dapper;
using LocalSeo.Web.Data;
using Microsoft.Data.SqlClient;

namespace LocalSeo.Web.Services;

public sealed record AppleMapsLookupTraceWriteModel(
    string PlaceId,
    int QueryIndex,
    string QueryText,
    string RequestUrl,
    int? HttpStatusCode,
    string? ResponseJson,
    string? ErrorMessage);

public interface IAppleMapsLookupTraceRepository
{
    Task InsertAsync(AppleMapsLookupTraceWriteModel model, CancellationToken ct);
}

public sealed class AppleMapsLookupTraceRepository(ISqlConnectionFactory connectionFactory) : IAppleMapsLookupTraceRepository
{
    public async Task InsertAsync(AppleMapsLookupTraceWriteModel model, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(model);

        var placeId = Truncate((model.PlaceId ?? string.Empty).Trim(), 255);
        var queryText = Truncate((model.QueryText ?? string.Empty).Trim(), 500);
        var requestUrl = Truncate((model.RequestUrl ?? string.Empty).Trim(), 2000);
        if (string.IsNullOrWhiteSpace(placeId) || string.IsNullOrWhiteSpace(queryText) || string.IsNullOrWhiteSpace(requestUrl))
            return;

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(@"
INSERT INTO dbo.AppleMapsLookupTrace(
  PlaceId,
  QueryIndex,
  QueryText,
  RequestUrl,
  HttpStatusCode,
  ResponseJson,
  ErrorMessage
)
VALUES(
  @PlaceId,
  @QueryIndex,
  @QueryText,
  @RequestUrl,
  @HttpStatusCode,
  @ResponseJson,
  @ErrorMessage
);",
            new
            {
                PlaceId = placeId,
                QueryIndex = Math.Max(1, model.QueryIndex),
                QueryText = queryText,
                RequestUrl = requestUrl,
                HttpStatusCode = model.HttpStatusCode is > 0 ? model.HttpStatusCode : null,
                ResponseJson = string.IsNullOrWhiteSpace(model.ResponseJson) ? null : model.ResponseJson,
                ErrorMessage = Truncate(model.ErrorMessage, 2000)
            },
            cancellationToken: ct));
    }

    private static string? Truncate(string? value, int maxLength)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;
        var trimmed = value.Trim();
        return trimmed.Length <= maxLength ? trimmed : trimmed[..maxLength];
    }
}
