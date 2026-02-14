namespace LocalSeo.Web.Options;

public sealed class DataForSeoOptions
{
    public string BaseUrl { get; set; } = "https://api.dataforseo.com";
    public string Login { get; set; } = string.Empty;
    public string Password { get; set; } = string.Empty;
    public string PostbackUrl { get; set; } = string.Empty;
    public string TaskPostPath { get; set; } = "/v3/business_data/google/reviews/task_post";
    public string TaskGetPathFormat { get; set; } = "/v3/business_data/google/reviews/task_get/{0}";
    public string TasksReadyPath { get; set; } = "/v3/business_data/google/reviews/tasks_ready";
    public string LanguageCode { get; set; } = "en";
    public int Depth { get; set; } = 100;
    public string SortBy { get; set; } = "newest";
    public int MaxPollAttempts { get; set; } = 10;
    public int PollDelayMs { get; set; } = 1000;
}
