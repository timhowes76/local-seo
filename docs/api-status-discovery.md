# API Status Discovery (Excluding DataForSEO)

## Scope
- Date: 2026-02-25
- Repository: `local-seo`
- Goal: identify external API integrations to include in the generic API Status system.

## Discovery method
- Searched HTTP client registrations (`AddHttpClient(`) and direct `HttpClient` usage.
- Searched code and config for external URL domains (`https://`), API option keys, and integration services.
- Reviewed `Program.cs`, `Options/*`, and service classes that make outbound API calls.

## Discovered integrations (for generated checks)

| Key | DisplayName | Category | Base URL / Domains | Evidence (paths/classes) |
|---|---|---|---|---|
| `openai` | OpenAI | AI | `api.openai.com` | `LocalSeo.Web/Options/OpenAiOptions.cs`, `LocalSeo.Web/Services/OpenAi/KeyphraseSuggestionService.cs`, `LocalSeo.Web/Program.cs` |
| `companieshouse` | Companies House | Government / Company Data | `api.company-information.service.gov.uk`, `document-api.company-information.service.gov.uk` | `LocalSeo.Web/Options/CompaniesHouseOptions.cs`, `LocalSeo.Web/Services/CompaniesHouseService.cs`, `LocalSeo.Web/Services/CompaniesHouseAccountsSyncService.cs`, `LocalSeo.Web/Program.cs` |
| `google` | Google APIs (Places, Geocoding, OAuth, GBP) | Maps / OAuth / Business Profile | `places.googleapis.com`, `maps.googleapis.com`, `accounts.google.com`, `oauth2.googleapis.com`, `mybusinessbusinessinformation.googleapis.com`, `www.googleapis.com` | `LocalSeo.Web/Options/GoogleOptions.cs`, `LocalSeo.Web/Services/GooglePlacesClient.cs`, `LocalSeo.Web/Services/GoogleBusinessProfileOAuthService.cs`, `LocalSeo.Web/Services/GoogleBusinessProfileCategoryService.cs`, `LocalSeo.Web/Program.cs` |
| `sendgrid` | SendGrid | Email Delivery | `api.sendgrid.com` | `LocalSeo.Web/Options/SendGridOptions.cs`, `LocalSeo.Web/Services/EmailSender.cs`, `LocalSeo.Web/Services/EmailDeliveryStatusSyncService.cs`, `LocalSeo.Web/Services/TransactionalEmailSender.cs`, `LocalSeo.Web/Program.cs` |
| `zoho` | Zoho CRM / OAuth | CRM | `accounts.zoho.com`, `www.zohoapis.com` | `LocalSeo.Web/Options/ZohoOAuthOptions.cs`, `LocalSeo.Web/Services/ZohoOAuthService.cs`, `LocalSeo.Web/Services/ZohoTokenService.cs`, `LocalSeo.Web/Services/ZohoCrmClient.cs`, `LocalSeo.Web/Controllers/ZohoIntegrationsController.cs`, `LocalSeo.Web/Program.cs` |

## Explicit exclusions

- **DataForSEO excluded by requirement**:
  - `LocalSeo.Web/Options/DataForSeoOptions.cs`
  - `LocalSeo.Web/Services/DataForSeoReviewsProvider.cs`
  - `LocalSeo.Web/Services/DataForSeoAccountStatusService.cs`
  - Existing Home/DataForSEO widget + modal in `LocalSeo.Web/Views/Home/Index.cshtml`.

- **Internal app URLs excluded**:
  - local/dev app URLs (`localhost`, `*.ngrok-free.dev`) used for callbacks/site links.

- **Telemetry/CDN/frontend asset endpoints excluded** (not core API dependencies for server-side functional flows):
  - static asset/CDN/font/package endpoints (`cdn.jsdelivr.net`, `fonts.googleapis.com`, `registry.npmjs.org`, etc.).

- **Non-core social/profile links excluded**:
  - social profile URL fields and UI links (`facebook.com`, `instagram.com`, etc.) are data content fields, not backend API integrations.

