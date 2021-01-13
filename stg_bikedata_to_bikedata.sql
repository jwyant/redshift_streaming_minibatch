START TRANSACTION;

UPDATE public.bikedata t SET end_timestamp = stg.activity_timestamp
FROM (SELECT
	a.bikeid
	,b.end_timestamp
	,a.activity_timestamp
	,ROW_NUMBER() OVER (PARTITION BY a.bikeid ORDER BY a.activity_timestamp) AS rnum
	FROM public.stg_bikedata a
	LEFT JOIN public.bikedata b
	  ON a.bikeid = b.bikeid
	 AND b.eff_timestamp <= a.activity_timestamp
	 AND b.end_timestamp > a.activity_timestamp
	WHERE (a.processed IS NULL OR a.processed = False)
) stg
WHERE stg.rnum = 1 AND t.bikeid = stg.bikeid AND t.end_timestamp = stg.end_timestamp;

INSERT INTO public.bikedata
SELECT
bikeid
,eff_timestamp
,end_timestamp
,status
,station_id
,station_longitude
,station_latitude
,station_name
FROM (SELECT
	a.bikeid
	,a.activity_timestamp as eff_timestamp
	,COALESCE(b.activity_timestamp, '9999-12-31 23:59:59') as end_timestamp
	,a.activity_type as status
	,COALESCE(a.station_id, b.station_id) as station_id
	,COALESCE(a.station_latitude, b.station_latitude) as station_latitude
	,COALESCE(a.station_longitude, b.station_longitude) as station_longitude
	,COALESCE(a.station_name, b.station_name) as station_name
	,ROW_NUMBER() OVER (PARTITION BY a.bikeid, a.activity_timestamp ORDER BY b.activity_timestamp ASC) as rnum

	FROM public.stg_bikedata a

	LEFT JOIN public.stg_bikedata b
	   ON a.bikeid = b.bikeid
	  AND a.activity_timestamp < b.activity_timestamp
	  AND (b.processed IS NULL OR b.processed = False)

	WHERE (a.processed IS NULL OR a.processed = False)

) WHERE rnum = 1 OR rnum IS NULL;

UPDATE stg_bikedata SET processed = True
FROM (
	SELECT s.* FROM stg_bikedata s
	LEFT JOIN bikedata t
	on s.bikeid = t.bikeid 
	and s.activity_timestamp = t.eff_timestamp
	WHERE t.eff_timestamp IS NOT NULL
) alreadythere
WHERE alreadythere.bikeid = stg_bikedata.bikeid 
  AND alreadythere.activity_timestamp = stg_bikedata.activity_timestamp;

END TRANSACTION;