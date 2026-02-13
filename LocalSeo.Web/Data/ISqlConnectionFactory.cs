using System.Data;

namespace LocalSeo.Web.Data;

public interface ISqlConnectionFactory
{
    Task<IDbConnection> OpenConnectionAsync(CancellationToken ct);
}
