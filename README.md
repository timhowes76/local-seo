# Local SEO Ingestion Tool (.NET 10)

ASP.NET Core MVC app for internal staff to run Google Places Text Search queries and persist snapshots into SQL Server 2019.

## Features
- OTP login restricted to `@kontrolit.net`.
- 6-digit code emailed via SendGrid (salted+hashed in DB only).
- Search form for seed keyword + location (+ optional geo bias).
- Ingestion pipeline with retries/backoff for Google transient errors.
- SQL bootstrap on startup (creates all required tables/indexes if missing).
- `/runs` and `/runs/{id}` pages for historical snapshots.
- Reviews provider abstraction with `None` and `SerpApi` placeholder.
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
- `SendGrid__ApiKey`
- `SendGrid__FromEmail`
- `SendGrid__FromName`
- `Auth__AllowedDomain` (default `kontrolit.net`)
- `Auth__CodeTtlMinutes` (default `10`)
- `Auth__MaxAttempts` (default `5`)
- `Auth__MaxSendsPerHour` (default `3`)
- `Places__DefaultRadiusMeters` (default `5000`)
- `Places__DefaultResultLimit` (default `20`)
- `Places__ReviewsProvider` (`None` or `SerpApi`)

## Run
1. Create SQL DB and set `ConnectionStrings__Sql`.
2. Set Google + SendGrid API keys in environment variables.
3. Run app:
   ```bash
   dotnet restore
   dotnet run --project LocalSeo.Web
   ```
4. Open `/login`.

> On startup, schema bootstrap creates these tables if missing: `SearchRun`, `Place`, `PlaceSnapshot`, `LoginCode`, `LoginThrottle`.

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
