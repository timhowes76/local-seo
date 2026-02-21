namespace LocalSeo.Web.Models;

public sealed class ProfileEditRequestModel
{
    public string FirstName { get; set; } = string.Empty;
    public string LastName { get; set; } = string.Empty;
    public bool UseGravatar { get; set; }
    public bool IsDarkMode { get; set; }
}

public sealed class ProfileEditViewModel
{
    public ProfileEditRequestModel Profile { get; init; } = new();
    public string? Message { get; init; }
}
