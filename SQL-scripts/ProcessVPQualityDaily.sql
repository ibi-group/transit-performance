
---run this script in the transit-performance database
--USE transit_performance
--GO

IF OBJECT_ID('dbo.ProcessVPQualityDaily','P') IS NOT NULL
	DROP PROCEDURE dbo.ProcessVPQualityDaily
GO


SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE PROCEDURE dbo.ProcessVPQualityDaily

--Script Version: Master - 1.1.0.0 - generic-all-agencies - 1

--This procedure calculates daily location quality metrics.

	@service_date							DATE

AS


BEGIN
    SET NOCOUNT ON; 

-----------------------------------------------------------------------------------------------------------------------------------------------------------------

	--Set parameters
	
	DECLARE @service_date_process DATE 
		SET @service_date_process = @service_date
	
	DECLARE @process_start_time INT
		SET @process_start_time = dbo.fnConvertDateTimeToEpoch(CAST(@service_date_process AS datetime)) + (3*60*60) -- + (3*60*60)  -- Set to 3am then adjust from transit-dev server time to CommTrans local time

	DECLARE @process_end_time INT
		SET @process_end_time = @process_start_time + (24*60*60)

	DECLARE @file_gap_threshold	INT
		SET @file_gap_threshold = 300

	DECLARE @vehicle_count_threshold INT
		SET @vehicle_count_threshold = -10

	DECLARE @distance_from_centroid_feet_threshold INT
		SET @distance_from_centroid_feet_threshold = (5280*100)
	
	DECLARE @file_time_gap_threshold INT
		SET @file_time_gap_threshold = 300

	DECLARE @vehicle_gap_threshold INT
		SET @vehicle_gap_threshold = 300

	DECLARE @vehicle_movement_threshold INT
		SET @vehicle_movement_threshold = 300

	DECLARE @vehicle_speed_threshold_commuter INT 
		SET @vehicle_speed_threshold_commuter = 70

	DECLARE @vehicle_speed_threshold_local INT 
		SET @vehicle_speed_threshold_local = 40


-----------------------------------------------------------------------------------------------------------------------------------------------------------------

	--Create dbo.daily_vehicle_position table and store daily positions from dbo.gtfsrt_vehicleposition_denormalized

	IF OBJECT_ID('dbo.daily_vehicle_position', 'U') IS NOT NULL 
		DROP TABLE dbo.daily_vehicle_position

	CREATE TABLE dbo.daily_vehicle_position 
	(
		record_id					INT IDENTITY(1, 1) PRIMARY KEY
		,service_date				DATE			NOT NULL
		,file_time					INT				NOT NULL
		,file_time_dt				DATETIME		NOT NULL
		,trip_start_time			VARCHAR(8)		
		,trip_schedule_relationship	VARCHAR(255)	NOT NULL
		,route_id					VARCHAR(255)	NOT NULL
		,trip_id					VARCHAR(255)
		,direction_id				INT	
		,stop_id					VARCHAR(255)
		,stop_sequence				INT
		,vehicle_id					VARCHAR(255)	NOT NULL
		,vehicle_label				VARCHAR(255)
		,current_status				VARCHAR(255)	NOT NULL
		,vehicle_timestamp			INT				NOT NULL
		,latitude					FLOAT			NOT NULL
		,longitude					FLOAT			NOT NULL
	)

	INSERT INTO dbo.daily_vehicle_position
	(
		service_date
		,file_time
		,file_time_dt
		,trip_start_time
		,trip_schedule_relationship
		,route_id
		,trip_id
		,direction_id
		,stop_id
		,stop_sequence
		,vehicle_id
		,vehicle_label
		,current_status
		,vehicle_timestamp
		,latitude
		,longitude
	)
	
	SELECT	
		CONVERT(DATE, trip_start_date)
		,gtfsrt.header_timestamp
		,dbo.fnConvertEpochToDateTime(gtfsrt.header_timestamp)
		,gtfsrt.trip_start_time
		,gtfsrt.trip_schedule_relationship
		,gtfsrt.route_id
		,gtfsrt.trip_id
		,gtfsrt.direction_id
		,gtfsrt.stop_id
		,gtfsrt.current_stop_sequence
		,gtfsrt.vehicle_id
		,gtfsrt.vehicle_label
		,gtfsrt.current_status
		,gtfsrt.vehicle_timestamp
		,gtfsrt.latitude
		,gtfsrt.longitude
	FROM	dbo.gtfsrt_vehicleposition_denormalized gtfsrt
	WHERE	gtfsrt.header_timestamp >= @process_start_time AND
			gtfsrt.header_timestamp < @process_end_time
	--WHERE CONVERT(DATE, trip_start_date) = @service_date_process 

	UPDATE daily_vehicle_position 
	SET direction_id = t.direction_id
	FROM gtfs.trips t
	WHERE
			dbo.daily_vehicle_position.trip_id = t.trip_id
		AND 
			dbo.daily_vehicle_position.direction_id IS NULL



-----------------------------------------------------------------------------------------------------------------------------------------------------------------
	--FILE METRICS

	--Create dbo.daily_vehicle_position_file_summary table.
	--This table identifies potential issues with any of the day's GTFS realtime vehicle position files.
	--Files are uniquely identified by their header_timestamp, and are flagged for the following issues:
		--1) file_gap: Large gap between updates (using the difference between header_timestamps)
		--2) vehicle_count: Large number of missing vehicles (comparing the number of vehicles in each file to the number of scheduled trips at the time of the header_timestamp)

	IF OBJECT_ID('tempdb..#daily_vehicle_position_file_disaggregate', 'U') IS NOT NULL 
		DROP TABLE #daily_vehicle_position_file_disaggregate

	CREATE TABLE #daily_vehicle_position_file_disaggregate
	(
		record_id						INT	IDENTITY(1, 1)	PRIMARY KEY
		,service_date					DATE
		,file_time						INT
		,previous_file_time				INT
		,seconds_since_previous_file	INT
		,count_vehicles_in_file			INT
		,count_scheduled_trips			INT
		,file_gap_flag					BIT	DEFAULT 0
		,vehicle_count_flag				BIT	DEFAULT 0
		,overall_file_quality_flag		BIT	DEFAULT 0
	)

	INSERT INTO #daily_vehicle_position_file_disaggregate
	(
		service_date
		,file_time
		,previous_file_time
		,seconds_since_previous_file
		,count_vehicles_in_file
	)

	SELECT
		dvp.service_date
		,dvp.file_time
		,LAG(dvp.file_time, 1) OVER (ORDER BY dvp.file_time)
		,dvp.file_time - LAG(dvp.file_time, 1) OVER (ORDER BY dvp.file_time)
		,COUNT(DISTINCT dvp.vehicle_id)
	FROM	dbo.daily_vehicle_position dvp
	GROUP BY	dvp.service_date
				,dvp.file_time
	ORDER BY	dvp.service_date
				,dvp.file_time

	UPDATE	#daily_vehicle_position_file_disaggregate
	SET		file_gap_flag = 1
	WHERE	seconds_since_previous_file > @file_gap_threshold

	UPDATE	#daily_vehicle_position_file_disaggregate
	SET		count_scheduled_trips = st.count_scheduled_trips
	FROM	#daily_vehicle_position_file_disaggregate vp
	LEFT JOIN	
	(
		SELECT 
				vp.file_time
				,count(distinct t.trip_id) as count_scheduled_trips
		FROM	#daily_vehicle_position_file_disaggregate vp
		LEFT JOIN	
			(
				SELECT DISTINCT
					service_date
					,trip_id
					,dbo.fnConvertDateTimeToEpoch(DATEADD(s, trip_start_time_sec, CAST(service_date AS datetime))) AS trip_start_time
					,dbo.fnConvertDateTimeToEpoch(DATEADD(s, trip_end_time_sec, CAST(service_date AS datetime))) AS trip_end_time
				FROM	dbo.daily_stop_times_sec 
			) t
		ON
				vp.file_time >= t.trip_start_time
			AND	
				vp.file_time <= t.trip_end_time
		GROUP BY	vp.file_time	
	) st
	ON
		vp.file_time = st.file_time

	UPDATE	#daily_vehicle_position_file_disaggregate
	SET		vehicle_count_flag = 1
	WHERE	count_vehicles_in_file - count_scheduled_trips < @vehicle_count_threshold

	UPDATE	#daily_vehicle_position_file_disaggregate
	SET		overall_file_quality_flag = 1
	WHERE	
			file_gap_flag <> 0
		OR
			vehicle_count_flag <> 0

	--Write final daily_vehicle_position_file_summary to database
		
	IF OBJECT_ID('dbo.daily_vehicle_position_file_summary') IS NOT NULL
		DROP TABLE dbo.daily_vehicle_position_file_summary

	CREATE TABLE dbo.daily_vehicle_position_file_summary 
	(
		file_time_dt	DATETIME
		,file_issue		VARCHAR(255)
	)

	INSERT INTO dbo.daily_vehicle_position_file_summary 
	(
		file_time_dt
		,file_issue
	)

	SELECT
		dbo.fnConvertEpochToDateTime(dvpfd.file_time)
		,CASE
			WHEN	dvpfd.file_gap_flag <> 0 AND
					dvpfd.vehicle_count_flag <> 1
						THEN CONCAT(
								CONVERT(VARCHAR(255), seconds_since_previous_file)
								,' seconds passed since the previous file update ')
			WHEN	dvpfd.file_gap_flag <> 1 AND
					dvpfd.vehicle_count_flag <> 0
						THEN CONCAT(
								'Only '
								,CONVERT(VARCHAR(255), count_vehicles_in_file)
								,' vehicles are present in the file while there are '
								,CONVERT(VARCHAR(255), count_scheduled_trips)
								,' scheduled trips for the time of this file update')
			ELSE	CONCAT(
						CONVERT(VARCHAR(255), seconds_since_previous_file)
						,' seconds passed since the previous file update AND only '
						,CONVERT(VARCHAR(255), count_vehicles_in_file)
						,' vehicles are present in the file while there are '
						,CONVERT(VARCHAR(255), count_scheduled_trips)
						,' scheduled trips for the time of this file update')
			END AS	file_issue
	FROM	#daily_vehicle_position_file_disaggregate dvpfd
	WHERE	dvpfd.overall_file_quality_flag <> 0


-----------------------------------------------------------------------------------------------------------------------------------------------------------------

	--Create temporary daily_vehile_position_disaggregate table.
	--All disagreggate vehicle position metrics are calculated using this temporary table.
	--After all calculations are performed a final daily_vehicle_position_disaggregate table is written to the database that includes only the fields of interest.

	IF OBJECT_ID('tempdb..#daily_vehicle_position_disaggregate', 'U') IS NOT NULL 
		DROP TABLE #daily_vehicle_position_disaggregate

	CREATE TABLE #daily_vehicle_position_disaggregate 
	(
		record_id								INT	
		,service_date							DATE			NOT NULL
		,file_time								INT				NOT NULL
		,file_time_dt							DATETIME		NOT NULL
		,trip_start_time						VARCHAR(8)		
		,trip_schedule_relationship				VARCHAR(255)	NOT NULL
		,route_id								VARCHAR(255)	NOT NULL
		,trip_id								VARCHAR(255)
		,direction_id							INT	
		,stop_id								VARCHAR(255)
		,stop_sequence							INT
		,vehicle_id								VARCHAR(255)	NOT NULL
		,vehicle_label							VARCHAR(255)
		,current_status							VARCHAR(255)	NOT NULL
		,vehicle_timestamp						INT				NOT NULL
		,latitude								FLOAT			NOT NULL
		,longitude								FLOAT			NOT NULL
		,avg_latitude							FLOAT
		,avg_longitude							FLOAT
		,distance_from_centroid_feet			INT
		,previous_file_time						INT
		,files_missing_since_previous_file_time	INT
		,file_time_gap							INT
		,file_time_vehicle_timestamp_lag		INT																	
		,first_vehicle_timestamp_of_position	INT
		,time_since_last_movement				INT
		,first_stop_sequence					INT
		,last_stop_sequence						INT
		,previous_latitude						FLOAT 
		,previous_longitude						FLOAT 
		,previous_vehicle_timestamp				INT 
		,vehicle_timestamp_gap_hour				FLOAT 
		,distance_traveled_since_last_file_mi	FLOAT 
		,vehicle_mph							FLOAT
		,file_quality_flag						BIT				DEFAULT 0
		,location_missing_flag					BIT				DEFAULT 0
		,location_quality_flag					BIT				DEFAULT 0
		,missing_from_file_flag					BIT				DEFAULT 0
		,file_time_gap_flag						BIT				DEFAULT 0
		,vehicle_update_flag					BIT				DEFAULT 0
		,vehicle_update_flag_lead				BIT				DEFAULT 0		
		,vehicle_movement_flag					BIT				DEFAULT 0
		,vehicle_movement_flag_lead				BIT				DEFAULT 0
		,vehicle_speed_flag						BIT				DEFAULT 0
	)

	INSERT INTO #daily_vehicle_position_disaggregate
	(
		record_id
		,service_date
		,file_time
		,file_time_dt
		,trip_start_time
		,trip_schedule_relationship
		,route_id
		,trip_id
		,direction_id
		,stop_id
		,stop_sequence
		,vehicle_id
		,vehicle_label
		,current_status
		,vehicle_timestamp
		,latitude
		,longitude
		,avg_latitude
		,avg_longitude	
		,previous_file_time
		,file_time_gap
		,file_time_vehicle_timestamp_lag
		,previous_latitude
		,previous_longitude
		,previous_vehicle_timestamp
		,vehicle_timestamp_gap_hour
	)
	
	SELECT
		dvp.record_id
		,dvp.service_date
		,dvp.file_time
		,dvp.file_time_dt
		,dvp.trip_start_time
		,dvp.trip_schedule_relationship
		,dvp.route_id
		,dvp.trip_id
		,dvp.direction_id
		,dvp.stop_id
		,dvp.stop_sequence
		,dvp.vehicle_id
		,dvp.vehicle_label
		,dvp.current_status
		,dvp.vehicle_timestamp
		,dvp.latitude
		,dvp.longitude
		,(SELECT AVG(latitude) FROM dbo.daily_vehicle_position) AS avg_latitude
		,(SELECT AVG(longitude) FROM dbo.daily_vehicle_position) AS avg_longitude
		,LAG(dvp.file_time, 1) OVER (PARTITION BY dvp.trip_id, dvp.trip_schedule_relationship, dvp.route_id, dvp.vehicle_id ORDER BY dvp.file_time) AS previous_file_time
		,dvp.file_time - LAG(dvp.file_time, 1) OVER (PARTITION BY	dvp.trip_id, dvp.trip_schedule_relationship, dvp.route_id, dvp.vehicle_id ORDER BY dvp.file_time) AS file_time_gap
		,dvp.file_time - dvp.vehicle_timestamp AS file_time_vehicle_timestamp_lag
		,lag(dvp.latitude,1) over (partition by dvp.trip_id, dvp.trip_schedule_relationship, dvp.route_id, dvp.vehicle_id ORDER BY dvp.file_time) as previous_latitude 
		,lag(dvp.longitude,1) over (partition by dvp.trip_id, dvp.trip_schedule_relationship, dvp.route_id, dvp.vehicle_id ORDER BY dvp.file_time) as previous_longitude
		,lag(dvp.vehicle_timestamp,1) over (partition by dvp.trip_id, dvp.trip_schedule_relationship, dvp.route_id, dvp.vehicle_id ORDER BY dvp.file_time) as previous_vehicletimestamp
		,(dvp.vehicle_timestamp - (lag(dvp.vehicle_timestamp,1) over (partition by dvp.trip_id, dvp.trip_schedule_relationship, dvp.route_id, dvp.vehicle_id ORDER BY dvp.file_time)))*1.0 / 3600 *1.0 as vehicle_timestamp_gap_hour
				
	FROM dbo.daily_vehicle_position dvp

	UPDATE	#daily_vehicle_position_disaggregate
	SET		distance_from_centroid_feet = dbo.fnGetDistanceFeet(latitude, longitude, avg_latitude, avg_longitude)	
		
	--Calculate distance traveled since last file in miles 
	UPDATE #daily_vehicle_position_disaggregate
	SET distance_traveled_since_last_file_mi = dbo.fnGetDistanceFeet(latitude, longitude, previous_latitude, previous_longitude)*1.0/ 5280*1.0

	--Calculate the vehicle speed in mph
	UPDATE #daily_vehicle_position_disaggregate
	SET vehicle_mph = 
		CASE WHEN vehicle_timestamp_gap_hour *1.0 = 0 then 0 
		ELSE distance_traveled_since_last_file_mi *1.0 / vehicle_timestamp_gap_hour *1.0
		END

	--Create temporary reference table with each unique combination of file_time and previous_file_time and determine the number of missing files between each.
	--And join to #daily_vehicle_position_disaggregate to set files_missing_since_previous_file_time.

	IF OBJECT_ID('tempdb..#file_gap_reference') IS NOT NULL 
		DROP TABLE #file_gap_reference

	CREATE TABLE #file_gap_reference
	(
		file_time								INT				
		,previous_file_time						INT
		,file_time_record_id					INT
		,previous_file_time_record_id			INT
		,files_missing_since_previous_file_time	INT
	)

	INSERT INTO #file_gap_reference
	(
		file_time
		,previous_file_time
	)
	
	SELECT DISTINCT
		dvpd.file_time
		,dvpd.previous_file_time
	FROM #daily_vehicle_position_disaggregate dvpd

	UPDATE	#file_gap_reference
	SET		file_time_record_id = dvpfd.record_id
	FROM	#file_gap_reference fgr
	LEFT JOIN	#daily_vehicle_position_file_disaggregate dvpfd
	ON	fgr.file_time = dvpfd.file_time

	UPDATE	#file_gap_reference
	SET		previous_file_time_record_id = dvpfd.record_id
	FROM	#file_gap_reference fgr
	LEFT JOIN	#daily_vehicle_position_file_disaggregate dvpfd
	ON	fgr.previous_file_time = dvpfd.file_time

	UPDATE	#file_gap_reference
	SET		files_missing_since_previous_file_time = file_time_record_id - previous_file_time_record_id - 1

	UPDATE	#daily_vehicle_position_disaggregate
	SET		files_missing_since_previous_file_time = fgr.files_missing_since_previous_file_time
	FROM	#daily_vehicle_position_disaggregate dvpd
	LEFT JOIN	#file_gap_reference fgr
	ON	
			dvpd.file_time = fgr.file_time
		AND
			dvpd.previous_file_time = fgr.previous_file_time
	
	--Create temporary reference table with the first vehicle_timestamp of each unique trip and lat/long. 
	--And join to #daily_vehicle_position_disaggregate to set first_vehicle_timestamp_of_position.
	
	IF OBJECT_ID('tempdb..#unique_vehicle_positions') IS NOT NULL 
		DROP TABLE #unique_vehicle_positions

	CREATE TABLE #unique_vehicle_positions 
	(
		trip_id							VARCHAR(300)
		,trip_schedule_relationship				VARCHAR(255)
		,route_id								VARCHAR(255)
		,vehicle_id								VARCHAR(255)
		,latitude								FLOAT
		,longitude								FLOAT
		,first_vehicle_timestamp_of_position	INT
	)

	INSERT INTO #unique_vehicle_positions
	(
		trip_id
		,trip_schedule_relationship
		,route_id
		,vehicle_id
		,latitude
		,longitude
		,first_vehicle_timestamp_of_position
	)
	
	SELECT
		dvp.trip_id
		,dvp.trip_schedule_relationship
		,dvp.route_id
		,dvp.vehicle_id
		,dvp.latitude
		,dvp.longitude
		,MIN(dvp.vehicle_timestamp)
	FROM	dbo.daily_vehicle_position dvp
	GROUP BY	
		dvp.trip_id
		,dvp.trip_schedule_relationship
		,dvp.route_id
		,dvp.vehicle_id
		,dvp.latitude
		,dvp.longitude

	UPDATE	#daily_vehicle_position_disaggregate
	SET		first_vehicle_timestamp_of_position = uvp.first_vehicle_timestamp_of_position
	FROM	#daily_vehicle_position_disaggregate dvpd
	LEFT JOIN	#unique_vehicle_positions uvp
	ON	
			dvpd.trip_id = uvp.trip_id 
		AND
			dvpd.trip_schedule_relationship = uvp.trip_schedule_relationship
		AND
			dvpd.route_id = uvp.route_id
		AND
			dvpd.vehicle_id = uvp.vehicle_id
		AND
			dvpd.latitude = uvp.latitude
		AND
			dvpd.longitude = uvp.longitude

	UPDATE	#daily_vehicle_position_disaggregate
	SET		time_since_last_movement = vehicle_timestamp - first_vehicle_timestamp_of_position

	--Create temporary reference table that stores the first and last stop sequence of each trip. 
	--These stop sequences are omitted from recieving a vehicle_movement_flag in an attempt to avoid falsely flagging buses that are laying over.
	
	IF OBJECT_ID('tempdb..#first_last_stop_sequence') IS NOT NULL
		DROP TABLE #first_last_stop_sequence

	CREATE TABLE #first_last_stop_sequence 
	(
		trip_id				VARCHAR(300)	
		,trip_schedule_relationship	VARCHAR(255)
		,route_id					VARCHAR(255)
		,vehicle_id					VARCHAR(255)
		,first_stop_sequence		INT
		,last_stop_sequence			INT
	)

	INSERT INTO #first_last_stop_sequence
	(
		trip_id
		,trip_schedule_relationship
		,route_id
		,vehicle_id
		,first_stop_sequence
		,last_stop_sequence
	)
	SELECT
		dvp.trip_id
		,dvp.trip_schedule_relationship
		,dvp.route_id
		,dvp.vehicle_id
		,MIN(dvp.stop_sequence)
		,MAX(dvp.stop_sequence)
	FROM	dbo.daily_vehicle_position dvp
	GROUP BY	
		dvp.trip_id
		,dvp.trip_schedule_relationship
		,dvp.route_id
		,dvp.vehicle_id
	
	UPDATE	#daily_vehicle_position_disaggregate
	SET		first_stop_sequence = flss.first_stop_sequence
	FROM	#daily_vehicle_position_disaggregate dvpd
	LEFT JOIN	#first_last_stop_sequence flss
	ON	
			dvpd.trip_id =  flss.trip_id
		AND
			dvpd.trip_schedule_relationship =  flss.trip_schedule_relationship
		AND
			dvpd.route_id =  flss.route_id
		AND
			dvpd.vehicle_id =  flss.vehicle_id

	UPDATE	#daily_vehicle_position_disaggregate
	SET		last_stop_sequence = flss.last_stop_sequence
	FROM	#daily_vehicle_position_disaggregate dvpd
	LEFT JOIN	#first_last_stop_sequence flss
	ON	
			dvpd.trip_id =  flss.trip_id
		AND
			dvpd.trip_schedule_relationship =  flss.trip_schedule_relationship
		AND
			dvpd.route_id =  flss.route_id
		AND
			dvpd.vehicle_id =  flss.vehicle_id

	--Set all data quality flags in #daily_vehicle_position_disaggregate. 

	UPDATE	#daily_vehicle_position_disaggregate
	SET		file_quality_flag = dvpfd.overall_file_quality_flag
	FROM	#daily_vehicle_position_disaggregate dvpd
	LEFT JOIN	#daily_vehicle_position_file_disaggregate dvpfd
	ON	dvpd.file_time = dvpfd.file_time

	UPDATE	#daily_vehicle_position_disaggregate
	SET		location_missing_flag = 1
	WHERE	
			(latitude = 0 OR longitude = 0)
		AND
			file_quality_flag <> 1
	
	UPDATE	#daily_vehicle_position_disaggregate
	SET		location_quality_flag = 1
	WHERE	
			distance_from_centroid_feet > @distance_from_centroid_feet_threshold
		AND
			file_quality_flag <> 1
		AND
			location_missing_flag <> 1

	UPDATE	#daily_vehicle_position_disaggregate
	SET		missing_from_file_flag = 1
	WHERE	
			files_missing_since_previous_file_time > 0
		AND
			file_quality_flag <> 1
	
	UPDATE	#daily_vehicle_position_disaggregate
	SET		file_time_gap_flag = 1
	WHERE	
			file_time_gap > @file_time_gap_threshold
		AND
			file_quality_flag <> 1
	
	UPDATE	#daily_vehicle_position_disaggregate
	SET		vehicle_update_flag = 1
	WHERE	
			file_time_vehicle_timestamp_lag > @vehicle_gap_threshold
		AND 
			file_quality_flag <> 1
		AND
			file_time_gap_flag <> 1

	UPDATE	#daily_vehicle_position_disaggregate
	SET		vehicle_update_flag_lead = a.vehicle_update_flag
	FROM	#daily_vehicle_position_disaggregate 
	LEFT JOIN	#daily_vehicle_position_disaggregate a
	ON	#daily_vehicle_position_disaggregate.record_id = (a.record_id - 1)

	UPDATE	#daily_vehicle_position_disaggregate
	SET		vehicle_movement_flag = 1
	WHERE	
			time_since_last_movement > @vehicle_movement_threshold
		AND 
			file_quality_flag <> 1
		AND
			location_missing_flag <> 1
		AND
			file_time_gap_flag <> 1
		AND
			vehicle_update_flag <> 1
		AND
			stop_sequence <> first_stop_sequence
		AND
			stop_sequence <> last_stop_sequence
		AND
			current_status <> 'STOPPED_AT'

	UPDATE	#daily_vehicle_position_disaggregate
	SET		vehicle_movement_flag_lead = a.vehicle_movement_flag
	FROM	#daily_vehicle_position_disaggregate
	LEFT JOIN	#daily_vehicle_position_disaggregate a
	ON	#daily_vehicle_position_disaggregate.record_id = (a.record_id - 1)
	
	UPDATE #daily_vehicle_position_disaggregate
	SET vehicle_speed_flag = 1
	FROM #daily_vehicle_position_disaggregate
	LEFT JOIN config_route_description crd
	ON #daily_vehicle_position_disaggregate.route_id = crd.route_id
	WHERE	
			CASE 
				WHEN crd.route_desc in ('Community','Core') then @vehicle_speed_threshold_local
				WHEN crd.route_desc in ('Commuter','Contracted') then @vehicle_speed_threshold_commuter
				ELSE @vehicle_speed_threshold_local
			END	< vehicle_mph
		AND 
			file_quality_flag <> 1
		AND
			location_missing_flag <> 1
		AND
			file_time_gap_flag <> 1
		AND
			vehicle_update_flag <> 1
		AND
			vehicle_movement_flag <> 1


----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

	-- Create trip, route, and vehicle summaries for each metric

	-- Create temporary reference table for number of trips by route.
	-- Referenced by each route level summary to calculate the percentage of daily trips on each route that witnessed an issue.

	IF OBJECT_ID('tempdb..#trips_by_route') IS NOT NULL
		DROP TABLE #trips_by_route

	CREATE TABLE #trips_by_route
	(
		service_date				DATE		
		,trip_schedule_relationship	VARCHAR(255)
		,route_id					VARCHAR(255)
		,direction_id				INT	
		,count_trips				INT
	)
	
	INSERT INTO #trips_by_route
	(
		service_date	
		,trip_schedule_relationship
		,route_id
		,direction_id
		,count_trips
	)	
	SELECT
		dvpd.service_date	
		,dvpd.trip_schedule_relationship
		,dvpd.route_id
		,dvpd.direction_id
		,COUNT(DISTINCT trip_id)
	FROM	#daily_vehicle_position_disaggregate dvpd
	GROUP BY	
		dvpd.service_date
		,dvpd.trip_schedule_relationship
		,dvpd.route_id
		,dvpd.direction_id

	-- Create temporary reference table for number of trips by vehicle.
	-- Referenced by each route level summary to calculate the percentage of daily trips for each vehicle that witnessed an issue.

	IF OBJECT_ID('tempdb..#trips_by_vehicle') IS NOT NULL
		DROP TABLE #trips_by_vehicle

	CREATE TABLE #trips_by_vehicle
	(
		service_date		DATE
		,vehicle_id			VARCHAR(255)		
		,count_trips		INT
	)
	
	INSERT INTO #trips_by_vehicle
	(
		service_date
		,vehicle_id	
		,count_trips
	)	
	SELECT
		dvpd.service_date
		,dvpd.vehicle_id	
		,COUNT(DISTINCT trip_id)
	FROM	#daily_vehicle_position_disaggregate dvpd
	GROUP BY	
		dvpd.service_date
		,dvpd.vehicle_id

-----------------------------------------------------------------------------------------------------------------------------------------------------------------

	-- Create trip, route, and vehicle summaries for location_missing metric (i.e. vehicle lat or long = '0')

	--Trip Summary

	IF OBJECT_ID('tempdb..#vehicle_location_missing_summary_trip') IS NOT NULL
		DROP TABLE #vehicle_location_missing_summary_trip

	CREATE TABLE #vehicle_location_missing_summary_trip
	(
		service_date							DATE		
		,trip_schedule_relationship				VARCHAR(255)
		,route_id								VARCHAR(255)
		,direction_id							INT	
		,trip_start_time						VARCHAR(8)	
		,vehicle_id								VARCHAR(255)
		,trip_id								VARCHAR(255)
		,count_file_updates						INT
		,count_updates_with_missing_location	INT
		,percent_updates_with_missing_location	FLOAT
		,issue									VARCHAR(255)	DEFAULT 'NONE'
		,details								VARCHAR(255)	DEFAULT 'n/a'
	)

	INSERT INTO #vehicle_location_missing_summary_trip
	(
		service_date
		,trip_schedule_relationship
		,route_id
		,direction_id
		,trip_start_time
		,vehicle_id
		,trip_id
		,count_file_updates
		,count_updates_with_missing_location
	)
	SELECT
		dvpd.service_date
		,dvpd.trip_schedule_relationship
		,dvpd.route_id
		,dvpd.direction_id
		,dvpd.trip_start_time
		,dvpd.vehicle_id
		,dvpd.trip_id
		,COUNT(*)
		,SUM(CONVERT(INT, dvpd.location_missing_flag))
	FROM	#daily_vehicle_position_disaggregate dvpd
	GROUP BY	
		dvpd.service_date
		,dvpd.trip_schedule_relationship
		,dvpd.route_id
		,dvpd.direction_id
		,dvpd.trip_start_time
		,dvpd.vehicle_id
		,dvpd.trip_id
	ORDER BY	
		dvpd.service_date
		,dvpd.trip_schedule_relationship
		,dvpd.route_id
		,dvpd.direction_id
		,dvpd.trip_start_time
		,dvpd.vehicle_id
		,dvpd.trip_id

	UPDATE	#vehicle_location_missing_summary_trip
	SET		percent_updates_with_missing_location = ((count_updates_with_missing_location * 1.0) / (count_file_updates * 1.0)) 

	UPDATE	#vehicle_location_missing_summary_trip
	SET		issue = 'MISSING LOCATION DATA'
	WHERE	count_updates_with_missing_location > 0

	UPDATE	#vehicle_location_missing_summary_trip
	SET		details = CONCAT(
							CONVERT(VARCHAR(255), count_updates_with_missing_location)
							,' of '
							,CONVERT(VARCHAR(255), count_file_updates)
							,' vehicle position files ('
							,CONVERT(VARCHAR(255), ROUND(percent_updates_with_missing_location * 100.0,1))
							,' percent) contained missing lat and long data')
	WHERE	count_updates_with_missing_location > 0

	--Route Summary	

	IF OBJECT_ID('tempdb..#vehicle_location_missing_summary_route') IS NOT NULL
		DROP TABLE #vehicle_location_missing_summary_route

	CREATE TABLE #vehicle_location_missing_summary_route
	(
		service_date							DATE		
		,route_id								VARCHAR(255)
		,count_file_updates						INT
		,count_updates_with_missing_location	INT
		,percent_updates_with_missing_location	FLOAT
		,issue									VARCHAR(255)	DEFAULT 'NONE'
		,details								VARCHAR(255)	DEFAULT 'n/a'
	)

	INSERT INTO #vehicle_location_missing_summary_route
	(
		service_date
		,route_id
		,count_file_updates
		,count_updates_with_missing_location
	)
	SELECT
		vlmt.service_date
		,vlmt.route_id
		,SUM(vlmt.count_file_updates)
		,SUM(vlmt.count_updates_with_missing_location)
	FROM	#vehicle_location_missing_summary_trip vlmt
	GROUP BY	
		vlmt.service_date
		,vlmt.route_id
	ORDER BY	
		vlmt.service_date
		,vlmt.route_id

	UPDATE	#vehicle_location_missing_summary_route
	SET		percent_updates_with_missing_location = ((count_updates_with_missing_location * 1.0) / (count_file_updates * 1.0))

	UPDATE	#vehicle_location_missing_summary_route
	SET		issue = 'MISSING LOCATION DATA'
	WHERE	count_updates_with_missing_location > 0

	UPDATE	#vehicle_location_missing_summary_route
	SET		details = CONCAT(
						CONVERT(VARCHAR(255), count_updates_with_missing_location)
						,' of '
						,CONVERT(VARCHAR(255), count_file_updates)
						,' vehicle position files ('
						,CONVERT(VARCHAR(255), ROUND(percent_updates_with_missing_location *100.0,2))
						,' percent) contained missing lat and long data')
	WHERE	count_updates_with_missing_location > 0

-----------------------------------------------------------------------------------------------------------------------------------------------------------------

	-- Create trip, route, and vehicle summaries for file_time_gap metric (i.e. vehicle disappeared from vehicle positions file for an extended period of time)

	--Trip Summary

	IF OBJECT_ID('tempdb..#file_time_gap_summary_trip') IS NOT NULL
		DROP TABLE #file_time_gap_summary_trip

	CREATE TABLE #file_time_gap_summary_trip
	(
		service_date				DATE
		,trip_schedule_relationship	VARCHAR(255)
		,route_id					VARCHAR(255)
		,direction_id				INT	
		,trip_start_time			VARCHAR(8)
		,vehicle_id					VARCHAR(255)
		,trip_id					VARCHAR(255)
		,count_file_updates			INT
		,count_updates_with_file_time_gap	INT
		,percent_updates_with_file_time_gap	FLOAT
		,issue						VARCHAR(255)	DEFAULT 'NONE'
		,details					VARCHAR(255)	DEFAULT 'n/a'
	)

	INSERT INTO #file_time_gap_summary_trip
	(
		service_date
		,trip_schedule_relationship	
		,route_id
		,direction_id
		,trip_start_time
		,vehicle_id	
		,trip_id
		,count_file_updates
		,count_updates_with_file_time_gap
	)

	SELECT
		dvpd.service_date
		,dvpd.trip_schedule_relationship	
		,dvpd.route_id
		,dvpd.direction_id
		,dvpd.trip_start_time
		,dvpd.vehicle_id	
		,dvpd.trip_id
		,COUNT(*)
		,SUM(CONVERT (INT, dvpd.file_time_gap_flag))
	FROM	#daily_vehicle_position_disaggregate dvpd
	GROUP BY	
		dvpd.service_date
		,dvpd.trip_schedule_relationship
		,dvpd.route_id
		,dvpd.direction_id
		,dvpd.trip_start_time
		,dvpd.vehicle_id
		,dvpd.trip_id
	ORDER BY	
		dvpd.service_date
		,dvpd.trip_schedule_relationship
		,dvpd.route_id
		,dvpd.direction_id
		,dvpd.trip_start_time
		,dvpd.vehicle_id
		,dvpd.trip_id

	UPDATE	#file_time_gap_summary_trip
	SET		percent_updates_with_file_time_gap = ((count_updates_with_file_time_gap * 1.0) / (count_file_updates * 1.0)) 

	UPDATE	#file_time_gap_summary_trip
	SET		issue = 'TRIP MISSING FROM VEHICLE POSITIONS FILE FOR EXTENDED PERIOD'
	WHERE	count_updates_with_file_time_gap > 0

	UPDATE	#file_time_gap_summary_trip
	SET		details = CONCAT(
							CONVERT(VARCHAR(255), count_updates_with_file_time_gap)
							,' of '
							,CONVERT(VARCHAR(255), count_file_updates)
							,' vehicle position files ('
							,CONVERT(VARCHAR(255), ROUND(percent_updates_with_file_time_gap * 100.0,1))
							,' percent) contained trip that went missing from vehicle positions file for extended period of time')
	WHERE	count_updates_with_file_time_gap > 0

	--Route Summary	

	IF OBJECT_ID('tempdb..#file_time_gap_summary_route') IS NOT NULL
		DROP TABLE #file_time_gap_summary_route

	CREATE TABLE #file_time_gap_summary_route
	(
		service_date						DATE
		,route_id							VARCHAR(255)
		,count_file_updates					INT
		,count_updates_with_file_time_gap	INT
		,percent_updates_with_file_time_gap	FLOAT
		,issue								VARCHAR(255)	DEFAULT 'NONE'
		,details							VARCHAR(255)	DEFAULT 'n/a'
	)

	INSERT INTO #file_time_gap_summary_route
	(
		service_date
		,route_id
		,count_file_updates
		,count_updates_with_file_time_gap
	)

	SELECT
		ftgt.service_date
		,ftgt.route_id
		,SUM(ftgt.count_file_updates)
		,SUM(ftgt.count_updates_with_file_time_gap)
	FROM	#file_time_gap_summary_trip ftgt
	GROUP BY	
		ftgt.service_date
		,ftgt.route_id

	UPDATE	#file_time_gap_summary_route
	SET		percent_updates_with_file_time_gap	= ((count_updates_with_file_time_gap * 1.0) / (count_file_updates * 1.0)) 

	UPDATE	#file_time_gap_summary_route
	SET		issue = 'TRIP MISSING FROM VEHICLE POSITIONS FILE FOR EXTENDED PERIOD'
	WHERE	count_updates_with_file_time_gap > 0

	UPDATE	#file_time_gap_summary_route
	SET		details = CONCAT(
						CONVERT(VARCHAR(255), count_updates_with_file_time_gap)
						,' of '
						,CONVERT(VARCHAR(255), count_file_updates)
						,' vehicle position file ('
						,CONVERT(VARCHAR(255), ROUND(percent_updates_with_file_time_gap * 100.0, 2))
						,' percent) containted trips that went missing for at least '
						,CONVERT(VARCHAR(255), @file_time_gap_threshold)
						,' seconds')
	WHERE	count_updates_with_file_time_gap > 0

-----------------------------------------------------------------------------------------------------------------------------------------------------------------
	-- Create trip, route, and vehicle summaries for vehicle speed metric (i.e. speed reported above threshold)

	--Trip Summary

	IF OBJECT_ID('tempdb..#vehicle_speed_high_summary_trip') IS NOT NULL
		DROP TABLE #vehicle_speed_high_summary_trip

	CREATE TABLE #vehicle_speed_high_summary_trip
	(
		service_date							DATE		
		,trip_schedule_relationship				VARCHAR(255)
		,route_id								VARCHAR(255)
		,direction_id							INT	
		,trip_start_time						VARCHAR(8)	
		,vehicle_id								VARCHAR(255)
		,trip_id								VARCHAR(255)
		,count_file_updates						INT
		,count_updates_with_high_speed			INT
		,percent_updates_with_high_speed		INT
		,issue									VARCHAR(255)	DEFAULT 'NONE'
		,details								VARCHAR(255)	DEFAULT 'n/a'
	)

	INSERT INTO #vehicle_speed_high_summary_trip
	(
		service_date
		,trip_schedule_relationship
		,route_id
		,direction_id
		,trip_start_time
		,vehicle_id
		,trip_id
		,count_file_updates
		,count_updates_with_high_speed
	)

	SELECT
		dvpd.service_date
		,dvpd.trip_schedule_relationship
		,dvpd.route_id
		,dvpd.direction_id
		,dvpd.trip_start_time
		,dvpd.vehicle_id
		,dvpd.trip_id
		,COUNT(*)
		,SUM(CONVERT(INT, dvpd.vehicle_speed_flag))
	FROM	#daily_vehicle_position_disaggregate dvpd
	GROUP BY	
		dvpd.service_date
		,dvpd.trip_schedule_relationship
		,dvpd.route_id
		,dvpd.direction_id
		,dvpd.trip_start_time
		,dvpd.vehicle_id
		,dvpd.trip_id
	ORDER BY	
		dvpd.service_date
		,dvpd.trip_schedule_relationship
		,dvpd.route_id
		,dvpd.direction_id
		,dvpd.trip_start_time
		,dvpd.vehicle_id
		,dvpd.trip_id

	UPDATE	#vehicle_speed_high_summary_trip
	SET		percent_updates_with_high_speed = ((count_updates_with_high_speed * 1.0) / (count_file_updates * 1.0)) * 100

	UPDATE	#vehicle_speed_high_summary_trip
	SET		issue = 'SPEED TOO HIGH'
	WHERE	count_updates_with_high_speed > 0

	UPDATE	#vehicle_speed_high_summary_trip
	SET		details = CONCAT(
							CONVERT(VARCHAR(255), count_updates_with_high_speed)
							,' of '
							,CONVERT(VARCHAR(255), count_file_updates)
							,' vehicle position files ('
							,CONVERT(VARCHAR(255), percent_updates_with_high_speed)
							,' percent) had high speeds reported')
	WHERE	count_updates_with_high_speed > 0

	--Route Summary	

	IF OBJECT_ID('tempdb..#vehicle_speed_high_summary_route') IS NOT NULL
		DROP TABLE #vehicle_speed_high_summary_route

	CREATE TABLE #vehicle_speed_high_summary_route
	(
		service_date							DATE		
		,route_id								VARCHAR(255)
		,count_file_updates						INT
		,count_updates_with_high_speed			INT
		,percent_updates_with_high_speed		FLOAT
		,issue									VARCHAR(255)	DEFAULT 'NONE'
		,details								VARCHAR(255)	DEFAULT 'n/a'
	)

	INSERT INTO #vehicle_speed_high_summary_route
	(
		service_date
		,route_id
		,count_file_updates
		,count_updates_with_high_speed
	)
	SELECT
		vlmt.service_date
		,vlmt.route_id
		,SUM(vlmt.count_file_updates)
		,SUM(vlmt.count_updates_with_high_speed)
	FROM	#vehicle_speed_high_summary_trip vlmt
	GROUP BY	
		vlmt.service_date
		,vlmt.route_id
	ORDER BY	
		vlmt.service_date
		,vlmt.route_id

	UPDATE	#vehicle_speed_high_summary_route
	SET		percent_updates_with_high_speed = ((count_updates_with_high_speed * 1.0) / (count_file_updates * 1.0)) 

	UPDATE	#vehicle_speed_high_summary_route
	SET		issue = 'SPEED TOO HIGH'
	WHERE	count_updates_with_high_speed > 0

	UPDATE	#vehicle_speed_high_summary_route
	SET		details = CONCAT(
						CONVERT(VARCHAR(255), count_updates_with_high_speed)
						,' of '
						,CONVERT(VARCHAR(255), count_file_updates)
						,' vehicle position files ('
						,CONVERT(VARCHAR(255), ROUND(percent_updates_with_high_speed * 100.0, 2))
						,' percent) had high speeds reported')
	WHERE	count_updates_with_high_speed > 0

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

	--Write the following daily summaries to database:
	--dbo.daily_vehicle_position_metrics_route
		
	IF OBJECT_ID('dbo.daily_vehicle_position_metrics_route') IS NOT NULL
		DROP TABLE dbo.daily_vehicle_position_metrics_route

	CREATE TABLE dbo.daily_vehicle_position_metrics_route
	(
		service_date				DATE
		,route_id					VARCHAR(255)
		,numerator					INT
		,denominator				INT
		,metric_result				FLOAT
		,issue						VARCHAR(255)
		,details					VARCHAR(255)
	)

	INSERT INTO dbo.daily_vehicle_position_metrics_route
	(	
		service_date
		,route_id
		,numerator
		,denominator
		,metric_result
		,issue
		,details
	)
	SELECT
		r.service_date
		,r.route_id
		,r.numerator
		,r.denominator
		,r.metric_result
		,r.issue
		,r.details
	FROM
			(SELECT
					vlmr.service_date
					,vlmr.route_id
					,vlmr.count_updates_with_missing_location as numerator
					,vlmr.count_file_updates as denominator
					,vlmr.percent_updates_with_missing_location as metric_result
					,vlmr.issue
					,vlmr.details
			FROM	#vehicle_location_missing_summary_route vlmr
			WHERE	vlmr.count_updates_with_missing_location > 0		
			
			UNION 
			
			SELECT
					ftgr.service_date
					,ftgr.route_id
					,ftgr.count_updates_with_file_time_gap as numerator
					,ftgr.count_file_updates as denominator
					,ftgr.percent_updates_with_file_time_gap as metric_result
					,ftgr.issue
					,ftgr.details
			FROM	#file_time_gap_summary_route ftgr
			WHERE	ftgr.count_updates_with_file_time_gap > 0

			UNION 

			SELECT
					vsr.service_date
					,vsr.route_id
					,vsr.count_updates_with_high_speed as numerator
					,vsr.count_file_updates as denominator
					,vsr.percent_updates_with_high_speed as metric_result
					,vsr.issue
					,vsr.details
			FROM	#vehicle_speed_high_summary_route vsr
			WHERE	vsr.count_updates_with_high_speed > 0

			) r
			
-----------------------------------------------------------------------------------------------------------------------------------------------------------------

	--Write final daily_vehicle_position_disaggregate file to database

	IF OBJECT_ID('dbo.daily_vehicle_position_disaggregate', 'U') IS NOT NULL 
		DROP TABLE dbo.daily_vehicle_position_disaggregate


	CREATE TABLE dbo.daily_vehicle_position_disaggregate
	(
		record_id					INT	
		,service_date				DATE
		,file_time					INT
		,file_time_dt				DATETIME
		,trip_start_time			VARCHAR(8)
		,trip_schedule_relationship	VARCHAR(255)
		,route_id					VARCHAR(255)
		,trip_id					VARCHAR(255)
		,direction_id				INT	
		,stop_id					VARCHAR(255)
		,stop_sequence				INT
		,vehicle_id					VARCHAR(255)
		,current_status				VARCHAR(255)
		,vehicle_timestamp			INT
		,latitude					FLOAT
		,longitude					FLOAT
		,previous_latitude			FLOAT 
		,previous_longitude			FLOAT 
		,previous_vehicle_timestamp	INT 
		,vehicle_mph				FLOAT
		,file_quality_flag			BIT
		,location_missing_flag		BIT
		,location_quality_flag		BIT
		,missing_from_file_flag		BIT	
		,file_time_gap_flag			BIT
		,vehicle_update_flag		BIT						
		,vehicle_movement_flag		BIT
		,vehicle_speed_flag			BIT				
	)

	INSERT INTO dbo.daily_vehicle_position_disaggregate
	(
		record_id
		,service_date
		,file_time
		,file_time_dt
		,trip_start_time
		,trip_schedule_relationship
		,route_id
		,trip_id
		,direction_id
		,stop_id
		,stop_sequence
		,vehicle_id
		,current_status
		,vehicle_timestamp
		,latitude
		,longitude
		,previous_latitude
		,previous_longitude
		,previous_vehicle_timestamp
		,vehicle_mph
		,file_quality_flag
		,location_missing_flag
		,location_quality_flag
		,missing_from_file_flag
		,file_time_gap_flag
		,vehicle_update_flag					
		,vehicle_movement_flag
		,vehicle_speed_flag
	)
	
	SELECT
		record_id
		,service_date
		,file_time
		,file_time_dt
		,trip_start_time
		,trip_schedule_relationship
		,route_id
		,trip_id
		,direction_id
		,stop_id
		,stop_sequence
		,vehicle_id
		,current_status
		,vehicle_timestamp
		,latitude
		,longitude
		,previous_latitude
		,previous_longitude
		,previous_vehicle_timestamp
		,vehicle_mph
		,file_quality_flag
		,location_missing_flag
		,location_quality_flag
		,missing_from_file_flag
		,file_time_gap_flag
		,vehicle_update_flag					
		,vehicle_movement_flag
		,vehicle_speed_flag
		FROM	#daily_vehicle_position_disaggregate

-----------------------------------------------------------------------------------------------------------------------------------------------------------------

	--Write to historical tables

	IF 
	(
		SELECT
			COUNT(*)
		FROM dbo.historical_vehicle_position_metrics_route
		WHERE
			service_date = @service_date_process 
	) > 0

	DELETE FROM dbo.historical_vehicle_position_metrics_route
	WHERE
		service_date = @service_date_process

	INSERT INTO historical_vehicle_position_metrics_route
	(
		service_date				
		,route_id					
		,numerator					
		,denominator				
		,metric_result				
		,issue						
		,details					
	)
	SELECT
		service_date				
		,route_id					
		,numerator					
		,denominator				
		,metric_result				
		,issue						
		,details
	FROM daily_vehicle_position_metrics_route

	IF OBJECT_ID('tempdb..#daily_vehicle_position_file_disaggregate', 'U') IS NOT NULL 
		DROP TABLE #daily_vehicle_position_file_disaggregate

	IF OBJECT_ID('tempdb..#daily_vehicle_position_disaggregate', 'U') IS NOT NULL 
		DROP TABLE #daily_vehicle_position_disaggregate

	IF OBJECT_ID('tempdb..#file_gap_reference') IS NOT NULL 
		DROP TABLE #file_gap_reference

	IF OBJECT_ID('tempdb..#unique_vehicle_positions') IS NOT NULL 
		DROP TABLE #unique_vehicle_positions

	IF OBJECT_ID('tempdb..#first_last_stop_sequence') IS NOT NULL
		DROP TABLE #first_last_stop_sequence

	IF OBJECT_ID('tempdb..#trips_by_route') IS NOT NULL
		DROP TABLE #trips_by_route

	IF OBJECT_ID('tempdb..#trips_by_vehicle') IS NOT NULL
		DROP TABLE #trips_by_vehicle

	IF OBJECT_ID('tempdb..#vehicle_location_missing_summary_trip') IS NOT NULL
		DROP TABLE #vehicle_location_missing_summary_trip

	IF OBJECT_ID('tempdb..#vehicle_location_missing_summary_route') IS NOT NULL
		DROP TABLE #vehicle_location_missing_summary_route

	IF OBJECT_ID('tempdb..#vehicle_location_missing_summary_vehicle') IS NOT NULL
		DROP TABLE #vehicle_location_missing_summary_vehicle

	IF OBJECT_ID('tempdb..#vehicle_location_quality_summary_trip') IS NOT NULL
		DROP TABLE #vehicle_location_quality_summary_trip

	IF OBJECT_ID('tempdb..#vehicle_location_quality_summary_route') IS NOT NULL
		DROP TABLE #vehicle_location_quality_summary_route

	IF OBJECT_ID('tempdb..#vehicle_location_quality_summary_vehicle') IS NOT NULL
		DROP TABLE #vehicle_location_quality_summary_vehicle

	IF OBJECT_ID('tempdb..#vehicle_missing_from_file_summary_trip') IS NOT NULL
		DROP TABLE #vehicle_missing_from_file_summary_trip

	IF OBJECT_ID('tempdb..#vehicle_missing_from_file_summary_route') IS NOT NULL
		DROP TABLE #vehicle_missing_from_file_summary_route

	IF OBJECT_ID('tempdb..#vehicle_missing_from_file_summary_vehicle') IS NOT NULL
		DROP TABLE #vehicle_missing_from_file_summary_vehicle

	IF OBJECT_ID('tempdb..#file_time_gap_summary_trip') IS NOT NULL
		DROP TABLE #file_time_gap_summary_trip

	IF OBJECT_ID('tempdb..#trips_with_file_time_gap') IS NOT NULL
		DROP TABLE #trips_with_file_time_gap

	IF OBJECT_ID('tempdb..#file_time_gap_summary_route') IS NOT NULL
		DROP TABLE #file_time_gap_summary_route

	IF OBJECT_ID('tempdb..#file_time_gap_summary_vehicle') IS NOT NULL
		DROP TABLE #file_time_gap_summary_vehicle

	IF OBJECT_ID('tempdb..#vehicle_update_gap_summary_trip') IS NOT NULL
		DROP TABLE #vehicle_update_gap_summary_trip

	IF OBJECT_ID('tempdb..#trips_with_vehicle_update_gap') IS NOT NULL
		DROP TABLE #trips_with_vehicle_update_gap

	IF OBJECT_ID('tempdb..#vehicle_update_gap_summary_route') IS NOT NULL
		DROP TABLE #vehicle_update_gap_summary_route

	IF OBJECT_ID('tempdb..#vehicle_update_gap_summary_vehicle') IS NOT NULL
		DROP TABLE #vehicle_update_gap_summary_vehicle

	IF OBJECT_ID('tempdb..#vehicle_movement_summary_trip') IS NOT NULL
		DROP TABLE #vehicle_movement_summary_trip

	IF OBJECT_ID('tempdb..#trips_with_vehicle_movement_freeze') IS NOT NULL
		DROP TABLE #trips_with_vehicle_movement_freeze

	IF OBJECT_ID('tempdb..#vehicle_movement_summary_route') IS NOT NULL
		DROP TABLE #vehicle_movement_summary_route

	IF OBJECT_ID('tempdb..#vehicle_movement_summary_vehicle') IS NOT NULL
		DROP TABLE #vehicle_movement_summary_vehicle

	IF OBJECT_ID('tempdb..#vehicle_speed_high_summary_trip') IS NOT NULL
		DROP TABLE #vehicle_speed_high_summary_trip

	IF OBJECT_ID('tempdb..#vehicle_speed_high_summary_route') IS NOT NULL
		DROP TABLE #vehicle_speed_high_summary_route

	IF OBJECT_ID('tempdb..#vehicle_speed_high_summary_vehicle') IS NOT NULL
		DROP TABLE #vehicle_speed_high_summary_vehicle



END



