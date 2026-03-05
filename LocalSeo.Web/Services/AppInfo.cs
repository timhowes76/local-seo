using System.Reflection;

namespace LocalSeo.Web.Services;

public interface IAppInfo
{
    string Version { get; }
}

public sealed class AppInfo : IAppInfo
{
    public AppInfo()
    {
        Version = ResolveVersion();
    }

    public string Version { get; }

    private static string ResolveVersion()
    {
        var assembly = Assembly.GetEntryAssembly() ?? Assembly.GetExecutingAssembly();
        var informationalVersion = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
        var resolved = string.IsNullOrWhiteSpace(informationalVersion)
            ? assembly.GetName().Version?.ToString()
            : informationalVersion;

        if (string.IsNullOrWhiteSpace(resolved))
            return "unknown";

        var normalized = resolved.Trim();
        const int maxDisplayLength = 50;
        return normalized.Length <= maxDisplayLength
            ? normalized
            : normalized[..maxDisplayLength];
    }
}
