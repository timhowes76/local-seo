using Dapper;

namespace LocalSeo.Web.Data;

public sealed class DbBootstrapper(ISqlConnectionFactory connectionFactory, ILogger<DbBootstrapper> logger)
{
    public async Task EnsureSchemaAsync(CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var sql = @"
IF OBJECT_ID('dbo.SearchRun','U') IS NULL
BEGIN
  CREATE TABLE dbo.SearchRun(
    SearchRunId bigint IDENTITY(1,1) PRIMARY KEY,
    SeedKeyword nvarchar(200) NOT NULL,
    LocationName nvarchar(200) NOT NULL,
    CenterLat decimal(9,6) NULL,
    CenterLng decimal(9,6) NULL,
    RadiusMeters int NULL,
    ResultLimit int NOT NULL,
    RanAtUtc datetime2(0) NOT NULL CONSTRAINT DF_SearchRun_RanAtUtc DEFAULT SYSUTCDATETIME()
  );
END;
IF OBJECT_ID('dbo.Place','U') IS NULL
BEGIN
  CREATE TABLE dbo.Place(
    PlaceId nvarchar(128) NOT NULL PRIMARY KEY,
    DisplayName nvarchar(300) NULL,
    PrimaryType nvarchar(80) NULL,
    PrimaryCategory nvarchar(200) NULL,
    TypesCsv nvarchar(1500) NULL,
    FormattedAddress nvarchar(500) NULL,
    Lat decimal(9,6) NULL,
    Lng decimal(9,6) NULL,
    NationalPhoneNumber nvarchar(50) NULL,
    WebsiteUri nvarchar(500) NULL,
    Description nvarchar(750) NULL,
    PhotoCount int NULL,
    IsServiceAreaBusiness bit NULL,
    BusinessStatus nvarchar(50) NULL,
    SearchLocationName nvarchar(200) NULL,
    RegularOpeningHoursJson nvarchar(max) NULL,
    OpeningDate datetime2(0) NULL,
    SocialProfilesJson nvarchar(max) NULL,
    ServiceAreasJson nvarchar(max) NULL,
    OtherCategoriesJson nvarchar(max) NULL,
    PlaceTopicsJson nvarchar(max) NULL,
    LastSeenUtc datetime2(0) NOT NULL CONSTRAINT DF_Place_LastSeenUtc DEFAULT SYSUTCDATETIME()
  );
END;
IF COL_LENGTH('dbo.Place', 'PrimaryCategory') IS NULL
  ALTER TABLE dbo.Place ADD PrimaryCategory nvarchar(200) NULL;
IF COL_LENGTH('dbo.Place', 'NationalPhoneNumber') IS NULL
  ALTER TABLE dbo.Place ADD NationalPhoneNumber nvarchar(50) NULL;
IF COL_LENGTH('dbo.Place', 'WebsiteUri') IS NULL
  ALTER TABLE dbo.Place ADD WebsiteUri nvarchar(500) NULL;
IF COL_LENGTH('dbo.Place', 'Description') IS NULL
  ALTER TABLE dbo.Place ADD Description nvarchar(750) NULL;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id=OBJECT_ID('dbo.Place') AND name='Description' AND max_length <> 1500)
BEGIN
  UPDATE dbo.Place
  SET Description = LEFT(Description, 750)
  WHERE Description IS NOT NULL
    AND LEN(Description) > 750;
  ALTER TABLE dbo.Place ALTER COLUMN Description nvarchar(750) NULL;
END;
IF COL_LENGTH('dbo.Place', 'PhotoCount') IS NULL
  ALTER TABLE dbo.Place ADD PhotoCount int NULL;
IF COL_LENGTH('dbo.Place', 'IsServiceAreaBusiness') IS NULL
  ALTER TABLE dbo.Place ADD IsServiceAreaBusiness bit NULL;
IF COL_LENGTH('dbo.Place', 'BusinessStatus') IS NULL
  ALTER TABLE dbo.Place ADD BusinessStatus nvarchar(50) NULL;
IF COL_LENGTH('dbo.Place', 'SearchLocationName') IS NULL
  ALTER TABLE dbo.Place ADD SearchLocationName nvarchar(200) NULL;
IF COL_LENGTH('dbo.Place', 'RegularOpeningHoursJson') IS NULL
  ALTER TABLE dbo.Place ADD RegularOpeningHoursJson nvarchar(max) NULL;
IF COL_LENGTH('dbo.Place', 'OpeningDate') IS NULL
  ALTER TABLE dbo.Place ADD OpeningDate datetime2(0) NULL;
IF COL_LENGTH('dbo.Place', 'SocialProfilesJson') IS NULL
  ALTER TABLE dbo.Place ADD SocialProfilesJson nvarchar(max) NULL;
IF COL_LENGTH('dbo.Place', 'ServiceAreasJson') IS NULL
  ALTER TABLE dbo.Place ADD ServiceAreasJson nvarchar(max) NULL;
IF COL_LENGTH('dbo.Place', 'OtherCategoriesJson') IS NULL
  ALTER TABLE dbo.Place ADD OtherCategoriesJson nvarchar(max) NULL;
IF COL_LENGTH('dbo.Place', 'PlaceTopicsJson') IS NULL
  ALTER TABLE dbo.Place ADD PlaceTopicsJson nvarchar(max) NULL;
IF OBJECT_ID('dbo.PlaceSnapshot','U') IS NULL
BEGIN
  CREATE TABLE dbo.PlaceSnapshot(
    PlaceSnapshotId bigint IDENTITY(1,1) PRIMARY KEY,
    SearchRunId bigint NOT NULL FOREIGN KEY REFERENCES dbo.SearchRun(SearchRunId),
    PlaceId nvarchar(128) NOT NULL FOREIGN KEY REFERENCES dbo.Place(PlaceId),
    RankPosition int NOT NULL,
    Rating decimal(3,2) NULL,
    UserRatingCount int NULL,
    CapturedAtUtc datetime2(0) NOT NULL CONSTRAINT DF_PlaceSnapshot_CapturedAtUtc DEFAULT SYSUTCDATETIME()
  );
END;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_PlaceSnapshot_Place_CapturedAt' AND object_id=OBJECT_ID('dbo.PlaceSnapshot'))
  CREATE INDEX IX_PlaceSnapshot_Place_CapturedAt ON dbo.PlaceSnapshot(PlaceId, CapturedAtUtc DESC);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_PlaceSnapshot_SearchRun_Rank' AND object_id=OBJECT_ID('dbo.PlaceSnapshot'))
  CREATE INDEX IX_PlaceSnapshot_SearchRun_Rank ON dbo.PlaceSnapshot(SearchRunId, RankPosition);
IF OBJECT_ID('dbo.PlaceReview','U') IS NULL
BEGIN
  CREATE TABLE dbo.PlaceReview(
    PlaceReviewId bigint IDENTITY(1,1) PRIMARY KEY,
    PlaceId nvarchar(128) NOT NULL FOREIGN KEY REFERENCES dbo.Place(PlaceId),
    ReviewId nvarchar(200) NOT NULL,
    ReviewUrl nvarchar(1500) NULL,
    ProfileName nvarchar(300) NULL,
    ProfileUrl nvarchar(1500) NULL,
    ProfileImageUrl nvarchar(1500) NULL,
    ReviewText nvarchar(max) NULL,
    OriginalReviewText nvarchar(max) NULL,
    OriginalLanguage nvarchar(30) NULL,
    Rating decimal(3,2) NULL,
    ReviewsCount int NULL,
    PhotosCount int NULL,
    LocalGuide bit NULL,
    TimeAgo nvarchar(100) NULL,
    ReviewTimestampUtc datetime2(0) NULL,
    OwnerAnswer nvarchar(max) NULL,
    OriginalOwnerAnswer nvarchar(max) NULL,
    OwnerTimeAgo nvarchar(100) NULL,
    OwnerTimestampUtc datetime2(0) NULL,
    SourceTaskId nvarchar(64) NULL,
    RawJson nvarchar(max) NULL,
    FirstSeenUtc datetime2(0) NOT NULL CONSTRAINT DF_PlaceReview_FirstSeenUtc DEFAULT SYSUTCDATETIME(),
    LastSeenUtc datetime2(0) NOT NULL CONSTRAINT DF_PlaceReview_LastSeenUtc DEFAULT SYSUTCDATETIME()
  );
END;
IF COL_LENGTH('dbo.PlaceReview', 'ReviewUrl') IS NULL
  ALTER TABLE dbo.PlaceReview ADD ReviewUrl nvarchar(1500) NULL;
IF COL_LENGTH('dbo.PlaceReview', 'ProfileName') IS NULL
  ALTER TABLE dbo.PlaceReview ADD ProfileName nvarchar(300) NULL;
IF COL_LENGTH('dbo.PlaceReview', 'ProfileUrl') IS NULL
  ALTER TABLE dbo.PlaceReview ADD ProfileUrl nvarchar(1500) NULL;
IF COL_LENGTH('dbo.PlaceReview', 'ProfileImageUrl') IS NULL
  ALTER TABLE dbo.PlaceReview ADD ProfileImageUrl nvarchar(1500) NULL;
IF COL_LENGTH('dbo.PlaceReview', 'ReviewText') IS NULL
  ALTER TABLE dbo.PlaceReview ADD ReviewText nvarchar(max) NULL;
IF COL_LENGTH('dbo.PlaceReview', 'OriginalReviewText') IS NULL
  ALTER TABLE dbo.PlaceReview ADD OriginalReviewText nvarchar(max) NULL;
IF COL_LENGTH('dbo.PlaceReview', 'OriginalLanguage') IS NULL
  ALTER TABLE dbo.PlaceReview ADD OriginalLanguage nvarchar(30) NULL;
IF COL_LENGTH('dbo.PlaceReview', 'Rating') IS NULL
  ALTER TABLE dbo.PlaceReview ADD Rating decimal(3,2) NULL;
IF COL_LENGTH('dbo.PlaceReview', 'ReviewsCount') IS NULL
  ALTER TABLE dbo.PlaceReview ADD ReviewsCount int NULL;
IF COL_LENGTH('dbo.PlaceReview', 'PhotosCount') IS NULL
  ALTER TABLE dbo.PlaceReview ADD PhotosCount int NULL;
IF COL_LENGTH('dbo.PlaceReview', 'LocalGuide') IS NULL
  ALTER TABLE dbo.PlaceReview ADD LocalGuide bit NULL;
IF COL_LENGTH('dbo.PlaceReview', 'TimeAgo') IS NULL
  ALTER TABLE dbo.PlaceReview ADD TimeAgo nvarchar(100) NULL;
IF COL_LENGTH('dbo.PlaceReview', 'ReviewTimestampUtc') IS NULL
  ALTER TABLE dbo.PlaceReview ADD ReviewTimestampUtc datetime2(0) NULL;
IF COL_LENGTH('dbo.PlaceReview', 'OwnerAnswer') IS NULL
  ALTER TABLE dbo.PlaceReview ADD OwnerAnswer nvarchar(max) NULL;
IF COL_LENGTH('dbo.PlaceReview', 'OriginalOwnerAnswer') IS NULL
  ALTER TABLE dbo.PlaceReview ADD OriginalOwnerAnswer nvarchar(max) NULL;
IF COL_LENGTH('dbo.PlaceReview', 'OwnerTimeAgo') IS NULL
  ALTER TABLE dbo.PlaceReview ADD OwnerTimeAgo nvarchar(100) NULL;
IF COL_LENGTH('dbo.PlaceReview', 'OwnerTimestampUtc') IS NULL
  ALTER TABLE dbo.PlaceReview ADD OwnerTimestampUtc datetime2(0) NULL;
IF COL_LENGTH('dbo.PlaceReview', 'SourceTaskId') IS NULL
  ALTER TABLE dbo.PlaceReview ADD SourceTaskId nvarchar(64) NULL;
IF COL_LENGTH('dbo.PlaceReview', 'RawJson') IS NULL
  ALTER TABLE dbo.PlaceReview ADD RawJson nvarchar(max) NULL;
IF COL_LENGTH('dbo.PlaceReview', 'FirstSeenUtc') IS NULL
  ALTER TABLE dbo.PlaceReview ADD FirstSeenUtc datetime2(0) NOT NULL CONSTRAINT DF_PlaceReview_FirstSeenUtc_Alt DEFAULT SYSUTCDATETIME();
IF COL_LENGTH('dbo.PlaceReview', 'LastSeenUtc') IS NULL
  ALTER TABLE dbo.PlaceReview ADD LastSeenUtc datetime2(0) NOT NULL CONSTRAINT DF_PlaceReview_LastSeenUtc_Alt DEFAULT SYSUTCDATETIME();
IF EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_PlaceReview_Place_Review' AND object_id=OBJECT_ID('dbo.PlaceReview'))
  DROP INDEX UX_PlaceReview_Place_Review ON dbo.PlaceReview;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_PlaceReview_Place_Review_NotNull' AND object_id=OBJECT_ID('dbo.PlaceReview'))
  CREATE UNIQUE INDEX UX_PlaceReview_Place_Review_NotNull ON dbo.PlaceReview(PlaceId, ReviewId) WHERE ReviewId IS NOT NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_PlaceReview_Place_LastSeen' AND object_id=OBJECT_ID('dbo.PlaceReview'))
  CREATE INDEX IX_PlaceReview_Place_LastSeen ON dbo.PlaceReview(PlaceId, LastSeenUtc DESC);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_PlaceReview_Place_ReviewTimestampUtc' AND object_id=OBJECT_ID('dbo.PlaceReview'))
  CREATE INDEX IX_PlaceReview_Place_ReviewTimestampUtc ON dbo.PlaceReview(PlaceId, ReviewTimestampUtc DESC) INCLUDE (Rating, OwnerTimestampUtc);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_PlaceReview_Place_OwnerTimestampUtc' AND object_id=OBJECT_ID('dbo.PlaceReview'))
  CREATE INDEX IX_PlaceReview_Place_OwnerTimestampUtc ON dbo.PlaceReview(PlaceId, OwnerTimestampUtc DESC) INCLUDE (ReviewTimestampUtc);
IF OBJECT_ID('dbo.PlaceReviewVelocityStats','U') IS NULL
BEGIN
  CREATE TABLE dbo.PlaceReviewVelocityStats(
    PlaceId nvarchar(128) NOT NULL PRIMARY KEY FOREIGN KEY REFERENCES dbo.Place(PlaceId),
    AsOfUtc datetime2(0) NOT NULL,
    ReviewsLast90 int NOT NULL CONSTRAINT DF_PlaceReviewVelocityStats_ReviewsLast90 DEFAULT(0),
    ReviewsLast180 int NOT NULL CONSTRAINT DF_PlaceReviewVelocityStats_ReviewsLast180 DEFAULT(0),
    ReviewsLast270 int NOT NULL CONSTRAINT DF_PlaceReviewVelocityStats_ReviewsLast270 DEFAULT(0),
    ReviewsLast365 int NOT NULL CONSTRAINT DF_PlaceReviewVelocityStats_ReviewsLast365 DEFAULT(0),
    AvgPerMonth12m decimal(6,2) NULL,
    Prev90 int NOT NULL CONSTRAINT DF_PlaceReviewVelocityStats_Prev90 DEFAULT(0),
    Trend90Pct decimal(7,2) NULL,
    DaysSinceLastReview int NULL,
    LastReviewTimestampUtc datetime2(0) NULL,
    LongestGapDays12m int NULL,
    RespondedPct12m decimal(7,2) NULL,
    AvgOwnerResponseHours12m decimal(8,2) NULL,
    MomentumScore int NULL,
    StatusLabel varchar(20) NOT NULL CONSTRAINT DF_PlaceReviewVelocityStats_StatusLabel DEFAULT('NoReviews')
  );
END;
EXEC('CREATE OR ALTER PROCEDURE dbo.usp_RecomputePlaceReviewVelocityStats @PlaceId nvarchar(128) AS
BEGIN
  SET NOCOUNT ON;
  DECLARE @AsOfUtc datetime2(0) = SYSUTCDATETIME();
  DECLARE @WindowStart12m datetime2(0) = DATEADD(day,-365,@AsOfUtc);
  DECLARE @ReviewsLast90 int = (SELECT COUNT(1) FROM dbo.PlaceReview WHERE PlaceId=@PlaceId AND ReviewTimestampUtc >= DATEADD(day,-90,@AsOfUtc));
  DECLARE @ReviewsLast180 int = (SELECT COUNT(1) FROM dbo.PlaceReview WHERE PlaceId=@PlaceId AND ReviewTimestampUtc >= DATEADD(day,-180,@AsOfUtc));
  DECLARE @ReviewsLast270 int = (SELECT COUNT(1) FROM dbo.PlaceReview WHERE PlaceId=@PlaceId AND ReviewTimestampUtc >= DATEADD(day,-270,@AsOfUtc));
  DECLARE @ReviewsLast365 int = (SELECT COUNT(1) FROM dbo.PlaceReview WHERE PlaceId=@PlaceId AND ReviewTimestampUtc >= @WindowStart12m);
  DECLARE @Prev90 int = (SELECT COUNT(1) FROM dbo.PlaceReview WHERE PlaceId=@PlaceId AND ReviewTimestampUtc >= DATEADD(day,-180,@AsOfUtc) AND ReviewTimestampUtc < DATEADD(day,-90,@AsOfUtc));
  DECLARE @LastReviewTimestampUtc datetime2(0) = (SELECT MAX(ReviewTimestampUtc) FROM dbo.PlaceReview WHERE PlaceId=@PlaceId);
  DECLARE @DaysSinceLastReview int = CASE WHEN @LastReviewTimestampUtc IS NULL THEN NULL ELSE DATEDIFF(day,@LastReviewTimestampUtc,@AsOfUtc) END;
  DECLARE @Trend90Pct decimal(7,2) = CASE WHEN @Prev90 = 0 AND @ReviewsLast90 = 0 THEN 0 WHEN @Prev90 = 0 AND @ReviewsLast90 > 0 THEN 100 ELSE CAST(((@ReviewsLast90 - @Prev90) * 100.0) / NULLIF(@Prev90,0) AS decimal(7,2)) END;

  DECLARE @Total12m int = (SELECT COUNT(1) FROM dbo.PlaceReview WHERE PlaceId=@PlaceId AND ReviewTimestampUtc >= @WindowStart12m);
  DECLARE @Replied12m int = (SELECT COUNT(1) FROM dbo.PlaceReview WHERE PlaceId=@PlaceId AND ReviewTimestampUtc >= @WindowStart12m AND OwnerTimestampUtc IS NOT NULL AND OwnerTimestampUtc >= ReviewTimestampUtc);
  DECLARE @RespondedPct12m decimal(7,2) = CASE WHEN @Total12m = 0 THEN NULL ELSE CAST((@Replied12m * 100.0) / NULLIF(@Total12m,0) AS decimal(7,2)) END;
  DECLARE @AvgOwnerResponseHours12m decimal(8,2) = (
    SELECT CAST(AVG(DATEDIFF(minute, ReviewTimestampUtc, OwnerTimestampUtc) / 60.0) AS decimal(8,2))
    FROM dbo.PlaceReview
    WHERE PlaceId=@PlaceId
      AND ReviewTimestampUtc >= @WindowStart12m
      AND OwnerTimestampUtc IS NOT NULL
      AND OwnerTimestampUtc >= ReviewTimestampUtc
  );

  DECLARE @LongestGapDays12m int = NULL;
  ;WITH review_points AS (
    SELECT CAST(@WindowStart12m AS datetime2(0)) AS PointUtc
    UNION ALL
    SELECT ReviewTimestampUtc FROM dbo.PlaceReview WHERE PlaceId=@PlaceId AND ReviewTimestampUtc >= @WindowStart12m
    UNION ALL
    SELECT @AsOfUtc
  ), gap_points AS (
    SELECT PointUtc, LEAD(PointUtc) OVER (ORDER BY PointUtc) AS NextPointUtc FROM review_points
  )
  SELECT @LongestGapDays12m = MAX(CASE WHEN NextPointUtc IS NULL THEN 0 ELSE DATEDIFF(day, PointUtc, NextPointUtc) END)
  FROM gap_points;

  DECLARE @RecencyScore decimal(9,2) = CASE
    WHEN @DaysSinceLastReview IS NULL THEN 0
    WHEN @DaysSinceLastReview <= 7 THEN 100
    WHEN @DaysSinceLastReview <= 30 THEN 100 - ((@DaysSinceLastReview - 7) * (40.0 / 23.0))
    WHEN @DaysSinceLastReview <= 60 THEN 60 - ((@DaysSinceLastReview - 30) * (30.0 / 30.0))
    ELSE 0 END;

  DECLARE @MeanMonthly decimal(9,4), @StdMonthly decimal(9,4), @ConsistencyScore decimal(9,2);
  ;WITH months AS (
    SELECT 0 AS n, DATEFROMPARTS(YEAR(DATEADD(month,-11,@AsOfUtc)), MONTH(DATEADD(month,-11,@AsOfUtc)), 1) AS MonthStart
    UNION ALL
    SELECT n + 1, DATEADD(month,1,MonthStart) FROM months WHERE n < 11
  ), bucket AS (
    SELECT m.MonthStart, COUNT(r.PlaceReviewId) AS Cnt
    FROM months m
    LEFT JOIN dbo.PlaceReview r ON r.PlaceId=@PlaceId AND r.ReviewTimestampUtc >= m.MonthStart AND r.ReviewTimestampUtc < DATEADD(month,1,m.MonthStart)
    GROUP BY m.MonthStart
  )
  SELECT @MeanMonthly = AVG(CAST(Cnt AS decimal(9,4))), @StdMonthly = STDEV(CAST(Cnt AS decimal(9,4))) FROM bucket OPTION (MAXRECURSION 20);

  SET @ConsistencyScore = CASE WHEN ISNULL(@MeanMonthly,0) = 0 THEN 0 ELSE CASE WHEN (100 - ((ISNULL(@StdMonthly,0)/NULLIF(@MeanMonthly,0))*100.0)) < 0 THEN 0 WHEN (100 - ((ISNULL(@StdMonthly,0)/NULLIF(@MeanMonthly,0))*100.0)) > 100 THEN 100 ELSE (100 - ((ISNULL(@StdMonthly,0)/NULLIF(@MeanMonthly,0))*100.0)) END END;

  DECLARE @GrowthScore decimal(9,2);
  DECLARE @TrendClamped decimal(9,2) = CASE WHEN @Trend90Pct < -100 THEN -100 WHEN @Trend90Pct > 200 THEN 200 ELSE @Trend90Pct END;
  SET @GrowthScore = CASE
    WHEN @TrendClamped <= 0 THEN (@TrendClamped + 100) * 0.5
    WHEN @TrendClamped <= 100 THEN 50 + (@TrendClamped * 0.25)
    ELSE 75 + ((@TrendClamped - 100) * 0.25)
  END;

  DECLARE @MomentumScore int = CAST(ROUND((0.45 * @RecencyScore) + (0.25 * @ConsistencyScore) + (0.30 * @GrowthScore),0) AS int);
  IF @MomentumScore < 0 SET @MomentumScore = 0;
  IF @MomentumScore > 100 SET @MomentumScore = 100;

  DECLARE @StatusLabel varchar(20) = CASE
    WHEN @LastReviewTimestampUtc IS NULL THEN ''NoReviews''
    WHEN @DaysSinceLastReview > 60 THEN ''Stalled''
    WHEN @Trend90Pct <= -30 THEN ''Slowing''
    WHEN @Trend90Pct >= 30 AND @DaysSinceLastReview <= 30 THEN ''Accelerating''
    ELSE ''Healthy'' END;

  MERGE dbo.PlaceReviewVelocityStats AS target
  USING (SELECT @PlaceId AS PlaceId) AS source
  ON target.PlaceId = source.PlaceId
  WHEN MATCHED THEN UPDATE SET
    AsOfUtc=@AsOfUtc,
    ReviewsLast90=@ReviewsLast90,
    ReviewsLast180=@ReviewsLast180,
    ReviewsLast270=@ReviewsLast270,
    ReviewsLast365=@ReviewsLast365,
    AvgPerMonth12m=CAST(@ReviewsLast365 / 12.0 AS decimal(6,2)),
    Prev90=@Prev90,
    Trend90Pct=@Trend90Pct,
    DaysSinceLastReview=@DaysSinceLastReview,
    LastReviewTimestampUtc=@LastReviewTimestampUtc,
    LongestGapDays12m=@LongestGapDays12m,
    RespondedPct12m=@RespondedPct12m,
    AvgOwnerResponseHours12m=@AvgOwnerResponseHours12m,
    MomentumScore=@MomentumScore,
    StatusLabel=@StatusLabel
  WHEN NOT MATCHED THEN INSERT(
    PlaceId,AsOfUtc,ReviewsLast90,ReviewsLast180,ReviewsLast270,ReviewsLast365,AvgPerMonth12m,Prev90,Trend90Pct,DaysSinceLastReview,LastReviewTimestampUtc,LongestGapDays12m,RespondedPct12m,AvgOwnerResponseHours12m,MomentumScore,StatusLabel
  ) VALUES(
    @PlaceId,@AsOfUtc,@ReviewsLast90,@ReviewsLast180,@ReviewsLast270,@ReviewsLast365,CAST(@ReviewsLast365 / 12.0 AS decimal(6,2)),@Prev90,@Trend90Pct,@DaysSinceLastReview,@LastReviewTimestampUtc,@LongestGapDays12m,@RespondedPct12m,@AvgOwnerResponseHours12m,@MomentumScore,@StatusLabel
  );
END');
IF OBJECT_ID('dbo.DataForSeoReviewTask','U') IS NULL
BEGIN
  CREATE TABLE dbo.DataForSeoReviewTask(
    DataForSeoReviewTaskId bigint IDENTITY(1,1) PRIMARY KEY,
    DataForSeoTaskId nvarchar(64) NOT NULL,
    TaskType nvarchar(40) NOT NULL CONSTRAINT DF_DataForSeoReviewTask_TaskType DEFAULT 'reviews',
    PlaceId nvarchar(128) NOT NULL FOREIGN KEY REFERENCES dbo.Place(PlaceId),
    LocationName nvarchar(200) NULL,
    Status nvarchar(40) NOT NULL,
    TaskStatusCode int NULL,
    TaskStatusMessage nvarchar(500) NULL,
    Endpoint nvarchar(500) NULL,
    CreatedAtUtc datetime2(0) NOT NULL CONSTRAINT DF_DataForSeoReviewTask_CreatedAtUtc DEFAULT SYSUTCDATETIME(),
    LastCheckedUtc datetime2(0) NULL,
    ReadyAtUtc datetime2(0) NULL,
    PopulatedAtUtc datetime2(0) NULL,
    LastAttemptedPopulateUtc datetime2(0) NULL,
    LastPopulateReviewCount int NULL,
    CallbackReceivedAtUtc datetime2(0) NULL,
    CallbackTaskId nvarchar(64) NULL,
    LastError nvarchar(2000) NULL
  );
END;
IF COL_LENGTH('dbo.DataForSeoReviewTask', 'TaskType') IS NULL
  ALTER TABLE dbo.DataForSeoReviewTask ADD TaskType nvarchar(40) NOT NULL CONSTRAINT DF_DataForSeoReviewTask_TaskType_Alt DEFAULT 'reviews';
IF COL_LENGTH('dbo.DataForSeoReviewTask', 'LocationName') IS NULL
  ALTER TABLE dbo.DataForSeoReviewTask ADD LocationName nvarchar(200) NULL;
IF COL_LENGTH('dbo.DataForSeoReviewTask', 'Status') IS NULL
  ALTER TABLE dbo.DataForSeoReviewTask ADD Status nvarchar(40) NOT NULL CONSTRAINT DF_DataForSeoReviewTask_Status DEFAULT 'Created';
IF COL_LENGTH('dbo.DataForSeoReviewTask', 'TaskStatusCode') IS NULL
  ALTER TABLE dbo.DataForSeoReviewTask ADD TaskStatusCode int NULL;
IF COL_LENGTH('dbo.DataForSeoReviewTask', 'TaskStatusMessage') IS NULL
  ALTER TABLE dbo.DataForSeoReviewTask ADD TaskStatusMessage nvarchar(500) NULL;
IF COL_LENGTH('dbo.DataForSeoReviewTask', 'Endpoint') IS NULL
  ALTER TABLE dbo.DataForSeoReviewTask ADD Endpoint nvarchar(500) NULL;
IF COL_LENGTH('dbo.DataForSeoReviewTask', 'CreatedAtUtc') IS NULL
  ALTER TABLE dbo.DataForSeoReviewTask ADD CreatedAtUtc datetime2(0) NOT NULL CONSTRAINT DF_DataForSeoReviewTask_CreatedAtUtc_Alt DEFAULT SYSUTCDATETIME();
IF COL_LENGTH('dbo.DataForSeoReviewTask', 'LastCheckedUtc') IS NULL
  ALTER TABLE dbo.DataForSeoReviewTask ADD LastCheckedUtc datetime2(0) NULL;
IF COL_LENGTH('dbo.DataForSeoReviewTask', 'ReadyAtUtc') IS NULL
  ALTER TABLE dbo.DataForSeoReviewTask ADD ReadyAtUtc datetime2(0) NULL;
IF COL_LENGTH('dbo.DataForSeoReviewTask', 'PopulatedAtUtc') IS NULL
  ALTER TABLE dbo.DataForSeoReviewTask ADD PopulatedAtUtc datetime2(0) NULL;
IF COL_LENGTH('dbo.DataForSeoReviewTask', 'LastAttemptedPopulateUtc') IS NULL
  ALTER TABLE dbo.DataForSeoReviewTask ADD LastAttemptedPopulateUtc datetime2(0) NULL;
IF COL_LENGTH('dbo.DataForSeoReviewTask', 'LastPopulateReviewCount') IS NULL
  ALTER TABLE dbo.DataForSeoReviewTask ADD LastPopulateReviewCount int NULL;
IF COL_LENGTH('dbo.DataForSeoReviewTask', 'CallbackReceivedAtUtc') IS NULL
  ALTER TABLE dbo.DataForSeoReviewTask ADD CallbackReceivedAtUtc datetime2(0) NULL;
IF COL_LENGTH('dbo.DataForSeoReviewTask', 'CallbackTaskId') IS NULL
  ALTER TABLE dbo.DataForSeoReviewTask ADD CallbackTaskId nvarchar(64) NULL;
IF COL_LENGTH('dbo.DataForSeoReviewTask', 'LastError') IS NULL
  ALTER TABLE dbo.DataForSeoReviewTask ADD LastError nvarchar(2000) NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_DataForSeoReviewTask_TaskId' AND object_id=OBJECT_ID('dbo.DataForSeoReviewTask'))
  CREATE UNIQUE INDEX UX_DataForSeoReviewTask_TaskId ON dbo.DataForSeoReviewTask(DataForSeoTaskId);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_DataForSeoReviewTask_Status_Created' AND object_id=OBJECT_ID('dbo.DataForSeoReviewTask'))
  CREATE INDEX IX_DataForSeoReviewTask_Status_Created ON dbo.DataForSeoReviewTask(Status, CreatedAtUtc DESC);
IF OBJECT_ID('dbo.LoginCode','U') IS NULL
BEGIN
  CREATE TABLE dbo.LoginCode(
    LoginCodeId bigint IDENTITY(1,1) PRIMARY KEY,
    Email nvarchar(320) NOT NULL,
    CodeHash varbinary(64) NOT NULL,
    Salt varbinary(32) NOT NULL,
    ExpiresAtUtc datetime2(0) NOT NULL,
    CreatedAtUtc datetime2(0) NOT NULL CONSTRAINT DF_LoginCode_CreatedAtUtc DEFAULT SYSUTCDATETIME(),
    FailedAttempts int NOT NULL CONSTRAINT DF_LoginCode_FailedAttempts DEFAULT(0),
    IsUsed bit NOT NULL CONSTRAINT DF_LoginCode_IsUsed DEFAULT(0)
  );
END;
IF OBJECT_ID('dbo.LoginThrottle','U') IS NULL
BEGIN
  CREATE TABLE dbo.LoginThrottle(
    Email nvarchar(320) NOT NULL PRIMARY KEY,
    WindowStartUtc datetime2(0) NOT NULL,
    SendCount int NOT NULL
  );
END;";
        await conn.ExecuteAsync(new CommandDefinition(sql, cancellationToken: ct));
        logger.LogInformation("Schema bootstrap completed.");
    }
}
