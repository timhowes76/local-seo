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
    public string MyBusinessInfoTaskPostPath { get; set; } = "/v3/business_data/google/my_business_info/task_post";
    public string MyBusinessInfoTaskGetPathFormat { get; set; } = "/v3/business_data/google/my_business_info/task_get/{0}";
    public string MyBusinessInfoTasksReadyPath { get; set; } = "/v3/business_data/google/my_business_info/tasks_ready";
    public string MyBusinessUpdatesTaskPostPath { get; set; } = "/v3/business_data/google/my_business_updates/task_post";
    public string MyBusinessUpdatesTaskGetPathFormat { get; set; } = "/v3/business_data/google/my_business_updates/task_get/{0}";
    public string MyBusinessUpdatesTasksReadyPath { get; set; } = "/v3/business_data/google/my_business_updates/tasks_ready";
    public string QuestionsAndAnswersTaskPostPath { get; set; } = "/v3/business_data/google/questions_and_answers/task_post";
    public string QuestionsAndAnswersTaskGetPathFormat { get; set; } = "/v3/business_data/google/questions_and_answers/task_get/{0}";
    public string QuestionsAndAnswersTasksReadyPath { get; set; } = "/v3/business_data/google/questions_and_answers/tasks_ready";
    public string SearchVolumePath { get; set; } = "/v3/keywords_data/google_ads/search_volume/live";
    public string SearchVolumeLocationName { get; set; } = "United Kingdom";
    public string SearchVolumeLanguageName { get; set; } = "English";
    public int SearchVolumeLocationCode { get; set; } = 2826;
    public int SearchVolumeLanguageCode { get; set; } = 1000;
    public bool SearchVolumeSearchPartners { get; set; }
    public string LanguageCode { get; set; } = "en";
    public int Depth { get; set; } = 100;
    public string SortBy { get; set; } = "newest";
    public int MaxPollAttempts { get; set; } = 10;
    public int PollDelayMs { get; set; } = 1000;
}
