CREATE TABLE public.stg_bikedata (
	bikeid INTEGER
	,activity_timestamp TIMESTAMP
	,activity_type CHAR(6) ENCODE ZSTD
	,station_id INTEGER ENCODE AZ64
	,station_latitude DECIMAL(10,8) ENCODE AZ64
	,station_longitude DECIMAL(10,8) ENCODE AZ64
	,station_name VARCHAR(64) ENCODE ZSTD
) DISTSTYLE KEY DISTKEY (bikeid) SORTKEY (bikeid, activity_timestamp);

CREATE TABLE public.bikedata (
	bikeid INTEGER
	,eff_timestamp TIMESTAMP ENCODE AZ64
	,end_timestamp TIMESTAMP
	,status VARCHAR(64) ENCODE ZSTD
	,station_id INTEGER ENCODE AZ64
	,station_latitude DECIMAL(10,8) ENCODE AZ64
	,station_longitude DECIMAL(10,8) ENCODE AZ64
	,station_name VARCHAR(64) ENCODE ZSTD
) DISTSTYLE KEY DISTKEY (bikeid) SORTKEY (bikeid, end_timestamp, eff_timestamp);
