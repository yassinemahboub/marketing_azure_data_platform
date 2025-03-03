USE facebook_ads

-- CREATE EXTERNAL FACT TABLE

CREATE EXTERNAL TABLE dbo.fact_metrics (
	[date] date,
	[ad_id] nvarchar(4000),
	[adset_id] nvarchar(4000),
	[campaign_id] nvarchar(4000),
	[account_id] nvarchar(4000),
	[placement] nvarchar(4000),
	[platform] nvarchar(4000),
	[spend] real,
	[impressions] int,
	[clicks] int,
	[frequency] real,
	[leads] int,
	[ad_engagement] int,
	[video_views] int,
	[thruplay_video_views] int,
	[load_date] datetime2(7),
	[source] nvarchar(4000)
	)
	WITH (
	LOCATION = 'facebook_ads/fact_facebook_ads_metrics/**',
	DATA_SOURCE = silver,
	FILE_FORMAT = ParquetFormat
	)
GO

-- CREATE DIMENSIONAL AD TABLE

CREATE EXTERNAL TABLE dbo.dim_ad (
	[ad_created_time] datetime2(7),
	[ad_id] nvarchar(4000),
	[ad_name] nvarchar(4000),
	[ad_content] nvarchar(4000),
	[load_date] datetime2(7),
	[source] nvarchar(4000)
	)
	WITH (
	LOCATION = 'facebook_ads/dim_ad/**',
	DATA_SOURCE = silver,
	FILE_FORMAT = ParquetFormat
	)
GO

-- CREATE DIMENSIONAL ADSET TABLE

CREATE EXTERNAL TABLE dbo.dim_adset (
	[adset_id] nvarchar(4000),
	[adset_name] nvarchar(4000),
	[adset_status] nvarchar(4000),
	[adset_daily_budget] real,
	[adset_created_timestamp] datetime2(7),
	[adset_start_timestamp] datetime2(7),
	[adset_end_timestamp] datetime2(7),
	[load_date] datetime2(7),
	[source] nvarchar(4000)
	)
	WITH (
	LOCATION = 'facebook_ads/dim_adset/**',
	DATA_SOURCE = silver,
	FILE_FORMAT = ParquetFormat
	)
GO

-- CREATE DIMENSIONAL CAMPAIGN TABLE

CREATE EXTERNAL TABLE dbo.dim_campaign  (
	[campaign_id] nvarchar(4000),
	[campaign_name] nvarchar(4000),
	[campaign_objective] nvarchar(4000),
	[campaign_status] nvarchar(4000),
	[campaign_created_time] datetime2(7),
	[load_date] datetime2(7),
	[source] nvarchar(4000)
	)
	WITH (
	LOCATION = 'facebook_ads/dim_campaign/**',
	DATA_SOURCE = silver,
	FILE_FORMAT = ParquetFormat
	)
GO


-- CREATE DIMENSIONAL PLATFORM TABLE

CREATE EXTERNAL TABLE dbo.dim_platform (
	[device_platform] nvarchar(4000),
	[platform_position] nvarchar(4000),
	[publisher_platform] nvarchar(4000),
	[load_date] datetime2(7),
	[source] nvarchar(4000)
	)
	WITH (
	LOCATION = 'facebook_ads/dim_platform/**',
	DATA_SOURCE = silver,
	FILE_FORMAT = ParquetFormat
	)
GO



