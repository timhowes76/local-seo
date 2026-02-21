namespace LocalSeo.Web.Services;

public sealed record PasswordPolicyValidationResult(
    bool IsValid,
    IReadOnlyList<string> MissingRequirements);

public static class PasswordPolicyEvaluator
{
    public static PasswordPolicyValidationResult Validate(string? password, PasswordPolicyRules rules)
    {
        var value = password ?? string.Empty;
        var missing = new List<string>();

        if (value.Length < Math.Max(8, rules.MinimumLength))
            missing.Add($"at least {Math.Max(8, rules.MinimumLength)} characters");

        if (rules.RequiresNumber && !value.Any(char.IsDigit))
            missing.Add("at least one number");

        if (rules.RequiresCapitalLetter && !value.Any(char.IsUpper))
            missing.Add("at least one capital letter");

        if (rules.RequiresSpecialCharacter && !value.Any(ch => !char.IsLetterOrDigit(ch)))
            missing.Add("at least one special character");

        return new PasswordPolicyValidationResult(missing.Count == 0, missing);
    }

    public static string BuildGuidanceMessage(PasswordPolicyValidationResult validation)
    {
        if (validation.IsValid || validation.MissingRequirements.Count == 0)
            return "Password does not meet security requirements.";

        return $"Password must include {string.Join(", ", validation.MissingRequirements)}.";
    }
}
