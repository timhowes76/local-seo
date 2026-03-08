# Local SEO App

Internal ASP.NET Core MVC application for sales and admin teams to collect, enrich, review, and report on local SEO data for Google Business Profiles and related lead-gen workflows.

## Current Scope

The codebase is no longer just a simple Google Places ingestion tool. It now includes:

- Search runs that capture map-pack snapshots into `SearchRun` and `PlaceSnapshot`.
- Current-state place pages with reviews, updates, Q&A, photos, social profiles, Apple/Bing links, and summary metrics.
- A persisted SEO audit/actions system with editable rules and per-place audit results.
- Review and update velocity analysis.
- DataForSEO task submission, task tracking, manual population, and admin log pages.
- Zoho lead sync and status tracking.
- Companies House enrichment for financial data.
- Report generation and keyphrase traffic views.
- Admin settings, announcements, API status checks, and operational logs.

## Tech Stack

- .NET 10
- ASP.NET Core MVC
- Razor views
- SQL Server 2019
- Dapper + `Microsoft.Data.SqlClient`
- Serilog
- xUnit + Moq for tests

Runtime persistence is Dapper-based. The app does not use EF Core `DbContext` for normal data access.

## Architecture Notes

- Controllers are thin; business logic lives in services.
- SQL schema bootstrap runs at startup via [DbBootstrapper](./LocalSeo.Web/Data/DbBootstrapper.cs).
- The place detail experience is mostly current-state.
- Run snapshots preserve rank/rating snapshot history, but many detail views and audits use the latest stored place data.
- SEO audit results are tied to `PlaceId` and recalculated against current stored data.
- SQL migration scripts are stored under [LocalSeo.Web/Data/Migrations](./LocalSeo.Web/Data/Migrations).

## Main Areas

### Search / Runs / Places

- `/search` creates or reruns searches.
- `/runs` shows captured runs.
- `/runs/{id}` shows run snapshots, audit score, review/update indicators, and comparison links.
- `/places/{id}` shows current place details, reviews, updates, Q&A, financial data, audit actions, and history.

### SEO Audit / Actions

- Audit rules are stored in SQL and editable in admin.
- Results are persisted in `SeoAuditResult`.
- The place page has an `Actions` tab with score, actions needed, and already-good items.
- Current rules cover description, categories, reviews, review recency/velocity, responses, website, Q&A, updates, photos, hours, and rating-related checks.

### DataForSEO

- Supports task creation and population for:
  - reviews
  - my business info
  - updates
  - questions and answers
  - social profiles
- Admin log pages include task filtering, task details, refresh/populate actions, and error cleanup.
- Current route for task logs: `/admin/logs/dataforseo-tasks`

### CRM / Enrichment

- Zoho CRM lead sync
- Companies House company + officer/account enrichment
- Apple Maps / Bing link enrichment

### Admin / Ops

- Admin settings pages
- Announcements
- Login/email logs
- App/API health monitoring
- Audit rule management

## Repository Layout

- [LocalSeo.Web](./LocalSeo.Web) - main web app
- [LocalSeo.Web.Tests](./LocalSeo.Web.Tests) - automated tests
- [docs](./docs) - supporting docs/assets

## Configuration

The app currently loads configuration primarily from [LocalSeo.Web/appsettings.json](./LocalSeo.Web/appsettings.json), with additional runtime/admin-managed settings stored in SQL.

Important configuration areas include:

- `ConnectionStrings:Sql`
- `Google`
- `SendGrid`
- `Places`
- `DataForSeo`
- `OpenAi`
- `ZohoOAuth`
- `CompaniesHouse`
- `Integrations:AzureMaps`
- `Integrations:AppleMaps`
- `Reports`
- `Brand`

Do not commit real credentials. Use local/environment-specific values outside source control where possible.

## Local Development

1. Create or point to a SQL Server 2019 database.
2. Set `ConnectionStrings:Sql` and required integration keys in `LocalSeo.Web/appsettings.json`.
3. Restore and run:

```bash
dotnet restore
dotnet run --project LocalSeo.Web
```

4. Open `/login`.

Startup will:

- ensure/update schema
- seed required data definitions
- warm API status cache

If build or run fails because `LocalSeo.Web.exe` is locked, stop the existing running process first and rerun the command.

## Tests

Build:

```bash
dotnet build local-seo.sln
```

Run tests:

```bash
dotnet test LocalSeo.Web.Tests/LocalSeo.Web.Tests.csproj
```

## Operational Notes

- DataForSEO calls cost money; avoid unnecessary reruns.
- The app stores raw third-party values where needed, then derives reporting/audit behaviour in application logic.
- Website/audit classification is now centralized, and social-profile-only URLs do not count as a proper business website for reporting.
