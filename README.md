# Local SEO Ingestion Tool (.NET 10)

ASP.NET Core MVC app for internal staff to run Google Places Text Search queries and persist snapshots into SQL Server 2019.

## Features
- OTP login restricted to `@kontrolit.net`.
- 6-digit code emailed via SendGrid (salted+hashed in DB only).
- Search form for seed keyword + location (+ optional geo bias).
- Ingestion pipeline with retries/backoff for Google transient errors.
- SQL bootstrap on startup (creates all required tables/indexes if missing).
- `/runs` and `/runs/{id}` pages for historical snapshots.
- Reviews provider abstraction with `None`, `SerpApi` placeholder, and `DataForSeo` implementation.
- DataForSEO reviews are upserted by `(PlaceId, ReviewId)` so repeat runs only add new reviews.
- DataForSEO task tracking in `/admin/dataforseo-tasks` with status refresh and manual populate.
- DataForSEO postbacks are accepted at `/api/dataforseo/postback` (supports gzip payloads).
- Structured logging with Serilog.

## Tech
- .NET 10 ASP.NET Core MVC
- SQL Server 2019
- Dapper + Microsoft.Data.SqlClient
- Cookie auth + antiforgery

## Configuration
Set via `appsettings.json` and/or environment variables:
- `ConnectionStrings__Sql`
- `Google__ApiKey`
- `Google__ClientId`
- `Google__ClientSecret`
- `Google__RedirectBaseUrl`
- `Google__BusinessProfileOAuthRefreshToken` (optional in config; local secure store is populated after `/admin/google/connect`)
- `SendGrid__ApiKey`
- `SendGrid__FromEmail`
- `SendGrid__FromName`
- `Auth__AllowedDomain` (default `kontrolit.net`)
- `Auth__CodeTtlMinutes` (default `10`)
- `Auth__MaxAttempts` (default `5`)
- `Auth__MaxSendsPerHour` (default `3`)
- `Places__DefaultRadiusMeters` (default `5000`)
- `Places__DefaultResultLimit` (default `20`)
- `Places__ReviewsProvider` (`None`, `SerpApi`, or `DataForSeo`)
- `DataForSeo__BaseUrl` (default `https://api.dataforseo.com`)
- `DataForSeo__Login`
- `DataForSeo__Password`
- `DataForSeo__PostbackUrl` (e.g. `https://your-domain/api/dataforseo/postback?id=$id&tag=$tag`)
- `DataForSeo__TaskPostPath` (default `/v3/business_data/google/reviews/task_post`)
- `DataForSeo__TaskGetPathFormat` (default `/v3/business_data/google/reviews/task_get/{0}`)
- `DataForSeo__TasksReadyPath` (default `/v3/business_data/google/reviews/tasks_ready`)
- `DataForSeo__LanguageCode` (default `en`)
- `DataForSeo__Depth` (default `100`)
- `DataForSeo__SortBy` (default `newest`)
- `DataForSeo__MaxPollAttempts` (default `10`)
- `DataForSeo__PollDelayMs` (default `1000`)
- `OpenAi__ApiBaseUrl` (default `https://api.openai.com/v1/responses`)
- `OpenAi__ApiKey` (optional fallback; prefer Admin > Settings > Search)
- `OpenAi__DefaultModel` (default `gpt-4.1-mini`)
- `OpenAi__TimeoutSeconds` (default `20`)
- `ZohoOAuth__AccountsBaseUrl` (EU: `https://accounts.zoho.eu`, US: `https://accounts.zoho.com`)
- `ZohoOAuth__CrmApiBaseUrl` (EU: `https://www.zohoapis.eu/crm/v2`, US: `https://www.zohoapis.com/crm/v2`)
- `ZohoOAuth__ClientId`
- `ZohoOAuth__ClientSecret` (store in secrets manager / env var for production)
- `ZohoOAuth__RedirectUri` (e.g. `https://your-domain/zoho/oauth/callback`)
- `ZohoOAuth__Scopes` (default `ZohoCRM.modules.leads.ALL,ZohoCRM.settings.modules.READ`)

### Google OAuth setup checklist (GBP Categories sync)
- Authorized redirect URI:
  `https://briskly-viceless-kayleen.ngrok-free.dev/admin/google/oauth/callback`
- Authorized JavaScript origin (if needed):
  `https://briskly-viceless-kayleen.ngrok-free.dev`

### Zoho CRM OAuth setup checklist
- Use a Zoho server-based OAuth client with redirect URI:
  `https://briskly-viceless-kayleen.ngrok-free.dev/zoho/oauth/callback`
- For local HTTPS development, trust the ASP.NET Core dev certificate once:
  ```bash
  dotnet dev-certs https --trust
  ```
- Start the one-time consent flow as an authenticated staff user:
  `GET /integrations/zoho/connect`
- Callback endpoints supported by the app:
  - `GET /zoho/oauth/callback`
  - `GET /integrations/zoho/callback`
- Smoke test endpoint after connect:
  `GET /integrations/zoho/ping`

### Zoho client secret rotation
- Create a new client secret in Zoho and update only `ZohoOAuth__ClientSecret` in your secret store/environment.
- Restart the app instances so new config is loaded.
- Existing refresh tokens stay valid unless revoked; if revoked, rerun `/integrations/zoho/connect`.

### OpenAI keyphrase suggestions setup
- Development: store the fallback key with user-secrets instead of committing it:
  ```bash
  dotnet user-secrets set "OpenAi:ApiKey" "<your-api-key>" --project LocalSeo.Web
  ```
- Production: configure and rotate key via `Admin > Settings > Search` (`OpenAI API Key` field is encrypted at rest and masked in UI).

## Run
1. Create SQL DB and set `ConnectionStrings__Sql`.
2. Set Google + SendGrid API keys in environment variables.
3. Run app:
   ```bash
   dotnet restore
   dotnet run --project LocalSeo.Web
   ```
4. Open `/login`.

> On startup, schema bootstrap creates these tables if missing: `SearchRun`, `Place`, `PlaceSnapshot`, `PlaceReview`, `DataForSeoReviewTask`, `LoginCode`, `LoginThrottle`.

## OTP security rules implemented
- Allowed domain enforcement.
- TTL 10 min.
- Max verify attempts 5/code.
- Max sends 3/hour per email.
- Salted hash storage only.
- Mark code used after successful login.
- Cookie: HttpOnly + Secure + SameSite=Lax.

## Sample SQL queries (velocity/trend)

### Latest rank delta between last two sightings per place
```sql
WITH ranked AS (
  SELECT
    s.PlaceId,
    p.DisplayName,
    s.RankPosition,
    s.CapturedAtUtc,
    ROW_NUMBER() OVER (PARTITION BY s.PlaceId ORDER BY s.CapturedAtUtc DESC) AS rn
  FROM dbo.PlaceSnapshot s
  JOIN dbo.Place p ON p.PlaceId = s.PlaceId
)
SELECT
  cur.PlaceId,
  cur.DisplayName,
  prev.RankPosition AS PrevRank,
  cur.RankPosition AS CurrentRank,
  prev.RankPosition - cur.RankPosition AS RankImprovement
FROM ranked cur
JOIN ranked prev ON prev.PlaceId = cur.PlaceId AND prev.rn = 2
WHERE cur.rn = 1
ORDER BY RankImprovement DESC;
```

### Average rank by day for a place
```sql
SELECT
  CAST(CapturedAtUtc AS date) AS SnapshotDate,
  AVG(CAST(RankPosition AS decimal(10,2))) AS AvgRank
FROM dbo.PlaceSnapshot
WHERE PlaceId = @PlaceId
GROUP BY CAST(CapturedAtUtc AS date)
ORDER BY SnapshotDate;
```
