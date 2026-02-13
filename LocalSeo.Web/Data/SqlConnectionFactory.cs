using System.Data;
using Microsoft.Data.SqlClient;

namespace LocalSeo.Web.Data;

public sealed class SqlConnectionFactory(IConfiguration configuration) : ISqlConnectionFactory
{
    public async Task<IDbConnection> OpenConnectionAsync(CancellationToken ct)
    {
        var conn = new SqlConnection(configuration.GetConnectionString("Sql"));
        await conn.OpenAsync(ct);
        return conn;
    }
}
