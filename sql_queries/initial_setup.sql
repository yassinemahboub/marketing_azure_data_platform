-- CREATE DATA BASE
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'facebook_ads')
BEGIN
    CREATE DATABASE facebook_ads;
    PRINT 'Database "facebook_ads" created successfully.';
END
ELSE
BEGIN
    PRINT 'Database "facebook_ads" already exists.';
END
GO


-- USE FACEBOOK ADS DATA BASE

USE facebook_ads
GO

-- CREATE EXTERNAL DATA SOURCE

CREATE EXTERNAL DATA SOURCE silver 
WITH (
    LOCATION = 'abfss://learning@learningstorage1093.dfs.core.windows.net/silver/'
);

CREATE EXTERNAL DATA SOURCE gold
WITH (
    LOCATION = 'abfss://learning@learningstorage1093.dfs.core.windows.net/gold/'
);

-- CREATE EXTERNAL FILE FORMAT

CREATE EXTERNAL FILE FORMAT ParquetFormat
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);





