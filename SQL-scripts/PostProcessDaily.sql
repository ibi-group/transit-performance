
---run this script in the transit-performance database
--USE transit_performance
--GO

--This procedure processes all of the events for the service_date being processed. It runs after the PreProcessDaily.

--IF OBJECT_ID('dbo.PostProcessDaily','P') IS NOT NULL
--	DROP PROCEDURE dbo.PostProcessDaily

--GO

--SET ANSI_NULLS ON
--GO

--SET QUOTED_IDENTIFIER ON
--GO


--CREATE PROCEDURE dbo.PostProcessDaily 

--	@service_date DATE

--AS


--BEGIN
--	SET NOCOUNT ON;

	DECLARE @service_date DATE = '2017-07-26'

	DECLARE @service_date_process DATE
	SET @service_date_process = @service_date

	--ensure events from vehicle positions have direction_id and event_time_sec----------------------
	UPDATE dbo.rt_event
		SET direction_id = t.direction_id
		FROM gtfs.trips t
		WHERE
			dbo.rt_event.trip_id = t.trip_id
			AND dbo.rt_event.direction_id IS NULL
			AND dbo.rt_event.service_date = @service_date_process

	UPDATE dbo.rt_event
		SET direction_id = rds.direction_id
		FROM gtfs.route_direction_stop rds
		WHERE
			rds.route_id = dbo.rt_event.route_id
			AND rds.stop_id = dbo.rt_event.stop_id
			AND dbo.rt_event.direction_id IS NULL
			AND dbo.rt_event.service_date = @service_date_process

	UPDATE dbo.rt_event
		SET direction_id = 3
		WHERE
			dbo.rt_event.direction_id IS NULL
			AND dbo.rt_event.service_date = @service_date_process

	UPDATE dbo.rt_event
		SET event_time_sec = DATEDIFF(s,service_date,dbo.fnConvertEpochToDateTime(event_time))
		WHERE
			dbo.rt_event.event_time_sec IS NULL
			AND dbo.rt_event.service_date = @service_date_process

	--ensure events from trip_updates have direction_id---------------------
	UPDATE dbo.event_rt_trip_archive
		SET direction_id = t.direction_id
		FROM gtfs.trips t
		WHERE
			dbo.event_rt_trip_archive.trip_id = t.trip_id
			AND dbo.event_rt_trip_archive.direction_id IS NULL
			AND dbo.event_rt_trip_archive.service_date = @service_date_process

	UPDATE dbo.event_rt_trip_archive
		SET direction_id = rds.direction_id
		FROM gtfs.route_direction_stop rds
		WHERE
			rds.route_id = dbo.event_rt_trip_archive.route_id
			AND rds.stop_id = dbo.event_rt_trip_archive.stop_id
			AND dbo.event_rt_trip_archive.direction_id IS NULL
			AND dbo.event_rt_trip_archive.service_date = @service_date_process

	UPDATE dbo.event_rt_trip_archive
		SET direction_id = 3
		WHERE
			dbo.event_rt_trip_archive.direction_id IS NULL
			AND dbo.event_rt_trip_archive.service_date = @service_date_process

	--Create Event Time Table which stores arrival and departure events and times for the day being processed
	IF OBJECT_ID('dbo.daily_event','U') IS NOT NULL
		DROP TABLE dbo.daily_event
		;

	CREATE TABLE dbo.daily_event
	(
		record_id				INT				PRIMARY KEY NOT NULL IDENTITY
		,service_date			DATE			NOT NULL
		,file_time				INT				NOT NULL
		,route_id				VARCHAR(255)	NOT NULL
		,route_type				INT				NOT NULL
		,trip_id				VARCHAR(255)	NOT NULL
		,direction_id			INT				NOT NULL
		,stop_id				VARCHAR(255)	NOT NULL
		,stop_sequence			INT				NOT NULL
		,vehicle_id				VARCHAR(255)	NOT NULL
		,event_type				CHAR(3)			NOT NULL
		,event_time				INT				NOT NULL
		,event_time_sec			INT				NOT NULL
		,event_processed_rt		BIT				NOT NULL
		,event_processed_daily	BIT				NOT NULL
		,suspect_record			BIT				DEFAULT 0 NOT NULL
	)
	;

	CREATE NONCLUSTERED INDEX IX_daily_event_service_date ON daily_event (service_date);

	CREATE NONCLUSTERED INDEX IX_daily_event_route_id ON daily_event (route_id);

	CREATE NONCLUSTERED INDEX IX_daily_event_route_type ON daily_event (route_type);

	CREATE NONCLUSTERED INDEX IX_daily_event_trip_id ON daily_event (trip_id);

	CREATE NONCLUSTERED INDEX IX_daily_event_direction_id ON daily_event (direction_id);

	CREATE NONCLUSTERED INDEX IX_daily_event_stop_id ON daily_event (stop_id);

	CREATE NONCLUSTERED INDEX IX_daily_event_stop_sequence ON daily_event (stop_sequence);

	CREATE NONCLUSTERED INDEX IX_daily_event_vehicle_id ON daily_event (vehicle_id);

	CREATE NONCLUSTERED INDEX IX_daily_event_event_type ON daily_event (event_type);

	CREATE NONCLUSTERED INDEX IX_daily_event_event_time ON daily_event (event_time);

	CREATE NONCLUSTERED INDEX IX_daily_event_event_time_sec ON daily_event (event_time_sec);

	CREATE NONCLUSTERED INDEX IX_daily_event_event_processed_rt ON daily_event (event_processed_rt);

	CREATE NONCLUSTERED INDEX IX_daily_event_event_processed_daily ON daily_event (event_processed_daily);

	CREATE NONCLUSTERED INDEX IX_daily_event_event_suspect_record ON daily_event (suspect_record);

	CREATE NONCLUSTERED INDEX IX_daily_event_1 ON dbo.daily_event (event_type)
	INCLUDE (trip_id,stop_sequence,event_time_sec)

	INSERT INTO dbo.daily_event
	(
		service_date
		,file_time
		,route_id
		,route_type
		,trip_id
		,direction_id
		,stop_id
		,stop_sequence
		,vehicle_id
		,event_type
		,event_time
		,event_time_sec
		,event_processed_rt
		,event_processed_daily
	)

		--insert vehicle positions real-time events into daily events---------------------
		SELECT
			service_date
			,file_time
			,ed.route_id
			,r.route_type
			,trip_id
			,direction_id
			,stop_id
			,stop_sequence
			,vehicle_id
			,event_type
			,event_time
			,event_time_sec
			,event_processed_rt
			,event_processed_daily
		FROM	dbo.rt_event ed
				,gtfs.routes r
		WHERE
			service_date = @service_date_process
			AND r.route_id = ed.route_id
			AND event_time_sec IS NOT NULL
		ORDER BY
			record_id

	-----mark suspect record where event happens after 3:30 am the next day
	UPDATE dbo.daily_event
		SET suspect_record = 1
		WHERE
			event_time_sec > 99000

	-------finding missed CR events at the destination and adding last available prediction from trip_udpates----------
	IF OBJECT_ID('tempdb..##missed_events','U') IS NOT NULL
		DROP TABLE ##missed_events

	CREATE TABLE ##missed_events
	(
		service_date		DATE
		,trip_id			VARCHAR(255)
		,route_type			INT
		,stop_id			VARCHAR(255)
		,stop_sequence		INT
		,stop_order_flag	INT
	)

	INSERT INTO ##missed_events
	(
		service_date
		,trip_id
		,route_type
		,stop_id
		,stop_sequence
		,stop_order_flag
	)

		SELECT DISTINCT
			st.service_date
			,st.trip_id
			,st.route_type
			,st.stop_id
			,st.stop_sequence
			,st.stop_order_flag
		FROM	dbo.daily_stop_times_sec st
				,dbo.daily_event de
		WHERE
			de.trip_id = st.trip_id
			AND de.service_date = st.service_date
			AND st.route_type = 2
			AND st.stop_order_flag = 3

	DELETE FROM ##missed_events
	FROM	##missed_events me
			,dbo.daily_event de
	WHERE
		me.trip_id = de.trip_id
		AND me.stop_id = de.stop_id
		AND me.stop_sequence = de.stop_sequence
		AND me.service_date = de.service_date

	--add events from trip_updates into daily event---------------------
	IF OBJECT_ID('tempdb..##valid_trip_update_events','U') IS NOT NULL
		DROP TABLE ##valid_trip_update_events

	CREATE TABLE ##valid_trip_update_events
	(
		service_date			DATE			NOT NULL
		,file_time				INT				NOT NULL
		,route_id				VARCHAR(255)	NOT NULL
		,route_type				INT				NOT NULL
		,trip_id				VARCHAR(255)	NOT NULL
		,direction_id			INT				NOT NULL
		,stop_id				VARCHAR(255)	NOT NULL
		,stop_sequence			INT				NOT NULL
		,vehicle_id				VARCHAR(255)	NOT NULL
		,event_type				CHAR(3)			NOT NULL
		,event_time				INT				NOT NULL
		,event_time_sec			INT				NOT NULL
		,event_processed_rt		BIT				NOT NULL
		,event_processed_daily	BIT				NOT NULL
	)

	INSERT INTO ##valid_trip_update_events
	(
		service_date
		,file_time
		,route_id
		,route_type
		,trip_id
		,direction_id
		,stop_id
		,stop_sequence
		,vehicle_id
		,event_type
		,event_time
		,event_time_sec
		,event_processed_rt
		,event_processed_daily
	)

		SELECT DISTINCT
			tue.service_date
			,tue.file_time
			,tue.route_id
			,me.route_type
			,tue.trip_id
			,tue.direction_id
			,tue.stop_id
			,tue.stop_sequence
			,tue.vehicle_id
			,tue.event_type
			,tue.event_time
			,DATEDIFF(s,tue.service_date,dbo.fnConvertEpochToDateTime(tue.event_time)) AS event_time_sec
			,0 AS event_processed_rt
			,0 AS event_processed_daily

		FROM dbo.event_rt_trip_archive tue
		JOIN ##missed_events me
			ON
				(
				tue.trip_id = me.trip_id
				AND tue.stop_id = me.stop_id
				AND tue.stop_sequence = me.stop_sequence
				AND tue.service_date = me.service_date
				AND tue.event_type = 'PRA'   --add only arrival predictions
				AND
				tue.event_time > 0
				)
		JOIN
		(
			SELECT
				service_date
				,trip_id
				,stop_sequence
				,stop_id
				,event_type
				,COUNT(vehicle_id) AS num_duplicates
			FROM dbo.event_rt_trip_archive
			WHERE
				service_date = @service_date_process
			GROUP BY
				service_date
				,trip_id
				,stop_sequence
				,stop_id
				,event_type
			HAVING COUNT(vehicle_id) = 1
		) t --valid trip updates for a stop should only have one vehicle_id
			ON
				(
				tue.service_date = t.service_date
				AND tue.trip_id = t.trip_id
				AND tue.stop_sequence = t.stop_sequence
				AND tue.stop_id = t.stop_id
				)

	INSERT INTO dbo.daily_event
	(
		service_date
		,file_time
		,route_id
		,route_type
		,trip_id
		,direction_id
		,stop_id
		,stop_sequence
		,vehicle_id
		,event_type
		,event_time
		,event_time_sec
		,event_processed_rt
		,event_processed_daily
	)


		SELECT
			t.y_service_date
			,t.y_file_time
			,t.y_route_id
			,t.y_route_type
			,t.y_trip_id
			,t.y_direction_id
			,t.y_stop_id
			,t.y_stop_sequence
			,t.y_vehicle_id
			,t.y_event_type
			,t.y_event_time
			,t.y_event_time_sec
			,t.y_event_processed_rt
			,t.y_event_processed_daily

		FROM
		(
			SELECT
				y.service_date AS y_service_date
				,y.file_time AS y_file_time
				,y.route_id AS y_route_id
				,y.route_type AS y_route_type
				,y.trip_id AS y_trip_id
				,y.direction_id AS y_direction_id
				,y.stop_id AS y_stop_id
				,y.stop_sequence AS y_stop_sequence
				,y.vehicle_id AS y_vehicle_id
				,y.event_type AS y_event_type
				,y.event_time AS y_event_time
				,y.event_time_sec AS y_event_time_sec
				,y.event_processed_rt AS y_event_processed_rt
				,y.event_processed_daily AS y_event_processed_daily
				,ROW_NUMBER() OVER ( -- Partition finds the most recent relevant "previous" stop on the trip
				PARTITION BY
				y.trip_id
				ORDER BY x.event_time_sec DESC) AS rn

			FROM ##valid_trip_update_events y --y is the "current" 

			JOIN dbo.daily_event x --x is the most recent relevant "previous" 

				ON
					(
					y.service_date = x.service_date
					AND y.route_id = x.route_id
					AND y.direction_id = x.direction_id
					AND y.trip_id = x.trip_id
					AND y.event_time_sec > x.event_time_sec --make sure x DEP/ARR event is before y PRA event
					AND y.stop_sequence > x.stop_sequence --make sure x DEP/ARR stop sequence is before y PRA stop sequence
					AND y.event_type = 'PRA'
					AND x.suspect_record = 0
					AND x.event_time_sec < 99000 --make sure x event is valid
					AND y.file_time + 900 >= y.event_time --make sure the predicted event is less than 15 minutes from file time
					)
		) t
		WHERE
			rn = 1

	--MARK SUSPECT RECORDS

	--mark trip update events with 0 epoch time as suspect
	UPDATE dbo.daily_event
		SET suspect_record = 1
		WHERE
			(event_type = 'PRA'
				OR event_type = 'PRD')
			AND event_time = 0

	--Records where stop_sequence = 0
	UPDATE dbo.daily_event
		SET suspect_record = 1
		WHERE
			stop_sequence = 0

	--records where a trip-vehicle only had 2 or fewer records
	UPDATE dbo.daily_event
		SET suspect_record = 1
		FROM dbo.daily_event ed
			JOIN
			(
				SELECT
					service_date
					,route_id
					,vehicle_id
					,trip_id
					,count_event
				FROM
				(
					SELECT
						service_date
						,route_id
						,route_type
						,vehicle_id
						,trip_id
						,COUNT(*) AS count_event
					FROM dbo.daily_event ed
					GROUP BY
						service_date
						,route_id
						,route_type
						,vehicle_id
						,trip_id
				) t
				WHERE
					t.count_event <= 2
					AND t.route_type IN (1,2) --MBTA specific
			) s
				ON
					(
					ed.service_date = s.service_date
					AND ed.vehicle_id = s.vehicle_id
					AND ed.trip_id = s.trip_id
					)

	--Records where there are duplicate events for trip-stop that are not already suspect
	UPDATE dbo.daily_event
		SET suspect_record = 1
		FROM dbo.daily_event ed
			JOIN
			(
				SELECT
					service_date
					,trip_id
					,stop_sequence
					,stop_id
					,event_type
					,COUNT(*) AS num_duplicates
				FROM dbo.daily_event
				WHERE
					suspect_record = 0
				GROUP BY
					service_date
					,trip_id
					,stop_sequence
					,stop_id
					,event_type
				HAVING COUNT(*) > 1
			) t
				ON
					(
					ed.service_date = t.service_date
					AND ed.trip_id = t.trip_id
					AND ed.stop_sequence = t.stop_sequence
					AND ed.stop_id = t.stop_id
					AND ed.event_type = t.event_type
					)

	-----------------------------processing starts-------------------------------------------------------------------------------------------------

	--Create temp table daily_cd_time. This table stores the dwell times for the day being processed.
	--c is arrival at "From" stop, d is departure at "From" stop

	IF OBJECT_ID('tempdb..##daily_cd_time','U') IS NOT NULL
		DROP TABLE ##daily_cd_time

	CREATE TABLE ##daily_cd_time
	(
		service_date		VARCHAR(255)	NOT NULL
		,cd_stop_id			VARCHAR(255)	NOT NULL
		,cd_stop_sequence	INT				NOT NULL
		,cd_direction_id	INT				NOT NULL
		,cd_route_id		VARCHAR(255)	NOT NULL
		,cd_route_type		INT				NOT NULL
		,cd_trip_id			VARCHAR(255)	NOT NULL
		,cd_vehicle_id		VARCHAR(255)	NOT NULL
		,c_record_id		INT				NOT NULL
		,d_record_id		INT				NOT NULL
		,c_time_sec			INT				NOT NULL
		,d_time_sec			INT				NOT NULL
		,cd_time_sec		INT				NOT NULL
	)

	INSERT INTO ##daily_cd_time
	(
		service_date
		,cd_stop_id
		,cd_stop_sequence
		,cd_direction_id
		,cd_route_id
		,cd_route_type
		,cd_trip_id
		,cd_vehicle_id
		,c_record_id
		,d_record_id
		,c_time_sec
		,d_time_sec
		,cd_time_sec
	)

		SELECT
			edc.service_date
			,edc.stop_id AS cd_stop_id
			,edc.stop_sequence AS cd_stop_sequence
			,edc.direction_id AS cd_direction_id
			,edc.route_id AS cd_route_id
			,edc.route_type AS cd_route_type
			,edc.trip_id AS cd_trip_id
			,edc.vehicle_id AS cd_vehicle_id
			,edc.record_id AS c_record_id
			,edd.record_id AS d_record_id
			,edc.event_time_sec AS c_time_sec
			,edd.event_time_sec AS d_time_sec
			,edd.event_time_sec - edc.event_time_sec AS cd_time_sec

		FROM dbo.daily_event edd -- d is departure at "From" stop

		JOIN dbo.daily_event edc -- c is arrival at "From" stop
			ON
				(
				(edc.event_type = 'ARR'
				OR edc.event_type = 'PRA')
				AND edd.event_type = 'DEP'
				AND edc.service_date = edd.service_date
				AND edc.stop_id = edd.stop_id
				AND edc.stop_sequence = edd.stop_sequence
				AND edc.direction_id = edd.direction_id
				AND edc.vehicle_id = edd.vehicle_id
				AND edc.trip_id = edd.trip_id
				AND edd.event_time_sec >= edc.event_time_sec
				)
		WHERE
			edd.suspect_record = 0
			AND edc.suspect_record = 0

	--Create temp table daily_de_time. This table stores the travel times for the day being processed.
	-- d is departure at "From" stop, e is arrival at "To" stop

	IF OBJECT_ID('tempdb..##daily_de_time','U') IS NOT NULL
		DROP TABLE ##daily_de_time

	CREATE TABLE ##daily_de_time
	(
		service_date		VARCHAR(255)	NOT NULL
		,d_stop_id			VARCHAR(255)	NOT NULL
		,e_stop_id			VARCHAR(255)	NOT NULL
		,d_stop_sequence	INT				NOT NULL
		,e_stop_sequence	INT				NOT NULL
		,de_direction_id	INT				NOT NULL
		,de_route_id		VARCHAR(255)	NOT NULL
		,de_route_type		INT				NOT NULL
		,de_trip_id			VARCHAR(255)	NOT NULL
		,de_vehicle_id		VARCHAR(255)	NOT NULL
		,d_record_id		INT				NOT NULL
		,e_record_id		INT				NOT NULL
		,d_time_sec			INT				NOT NULL
		,e_time_sec			INT				NOT NULL
		,de_time_sec		INT				NOT NULL
	)

	INSERT INTO ##daily_de_time
	(
		service_date
		,d_stop_id
		,e_stop_id
		,d_stop_sequence
		,e_stop_sequence
		,de_direction_id
		,de_route_id
		,de_route_type
		,de_trip_id
		,de_vehicle_id
		,d_record_id
		,e_record_id
		,d_time_sec
		,e_time_sec
		,de_time_sec
	)


		SELECT
			edd.service_date
			,edd.stop_id AS d_stop_id
			,ede.stop_id AS e_stop_id
			,edd.stop_sequence AS d_stop_sequence
			,ede.stop_sequence AS e_stop_sequence
			,edd.direction_id AS de_direction_id
			,edd.route_id AS de_route_id
			,edd.route_type AS route_type
			,edd.trip_id AS de_trip_id
			,edd.vehicle_id AS de_vehicle_id
			,edd.record_id AS d_record_id
			,ede.record_id AS e_record_id
			,edd.event_time_sec AS d_time_sec
			,ede.event_time_sec AS e_time_sec
			,ede.event_time_sec - edd.event_time_sec AS de_time_sec

		FROM dbo.daily_event ede -- e is arrival at "To" stop

		JOIN dbo.daily_event edd -- d is departure at "From" stop
			ON
				(
				edd.event_type = 'DEP'
				AND (ede.event_type = 'ARR'
				OR ede.event_type = 'PRA')
				AND ede.service_date = edd.service_date
				AND ede.vehicle_id = edd.vehicle_id
				AND ede.trip_id = edd.trip_id
				AND ede.direction_id = edd.direction_id
				AND ede.stop_sequence > edd.stop_sequence
				AND ede.event_time_sec > edd.event_time_sec
				)
		WHERE
			ede.suspect_record = 0
			AND edd.suspect_record = 0

	--Create temp table daily_cde_time. This table stores the dwell + travel times for the day being processed.

	IF OBJECT_ID('tempdb..##daily_cde_time','U') IS NOT NULL
		DROP TABLE ##daily_cde_time

	CREATE TABLE ##daily_cde_time
	(
		service_date		VARCHAR(255)	NOT NULL
		,cd_stop_id			VARCHAR(255)	NOT NULL
		,e_stop_id			VARCHAR(255)	NOT NULL
		,cd_stop_sequence	INT				NOT NULL
		,e_stop_sequence	INT				NOT NULL
		,cde_direction_id	INT				NOT NULL
		,cde_route_id		VARCHAR(255)	NOT NULL
		,cde_route_type		INT				NOT NULL
		,cde_trip_id		VARCHAR(255)	NOT NULL
		,cde_vehicle_id		VARCHAR(255)	NOT NULL
		,c_record_id		INT				NOT NULL
		,d_record_id		INT				NOT NULL
		,e_record_id		INT				NOT NULL
		,c_time_sec			INT				NOT NULL
		,d_time_sec			INT				NOT NULL
		,e_time_sec			INT				NOT NULL
		,cd_time_sec		INT				NOT NULL
		,de_time_sec		INT				NOT NULL
	)

	INSERT INTO ##daily_cde_time
	(
		service_date
		,cd_stop_id
		,e_stop_id
		,cd_stop_sequence
		,e_stop_sequence
		,cde_direction_id
		,cde_route_id
		,cde_route_type
		,cde_trip_id
		,cde_vehicle_id
		,c_record_id
		,d_record_id
		,e_record_id
		,c_time_sec
		,d_time_sec
		,e_time_sec
		,cd_time_sec
		,de_time_sec
	)

		SELECT
			de.service_date
			,cd.cd_stop_id
			,de.e_stop_id
			,cd.cd_stop_sequence
			,de.e_stop_sequence
			,cd.cd_direction_id AS cde_direction_id
			,cd.cd_route_id AS cde_route_id
			,cd.cd_route_type AS cde_route_type
			,cd.cd_trip_id AS cde_trip_id
			,cd.cd_vehicle_id AS cde_vehicle_id
			,cd.c_record_id
			,cd.d_record_id
			,de.e_record_id
			,cd.c_time_sec
			,cd.d_time_sec
			,de.e_time_sec
			,cd.cd_time_sec
			,de.de_time_sec

		FROM ##daily_de_time de -- de is travel between "From" stop and "To" stop

		JOIN ##daily_cd_time cd --cd is dwell time at "From" stop
			ON
				(
				de.d_record_id = cd.d_record_id
				)

	--Create temp table daily_abcde_time. This table stores the day being processed joined events (abcde_time)
	IF OBJECT_ID('tempdb..##daily_abcde_time','U') IS NOT NULL
		DROP TABLE ##daily_abcde_time

	CREATE TABLE ##daily_abcde_time
	(
		service_date		VARCHAR(255)	NOT NULL
		,abcd_stop_id		VARCHAR(255)	NOT NULL
		,e_stop_id			VARCHAR(255)	NOT NULL
		,ab_stop_sequence	INT				NOT NULL
		,cd_stop_sequence	INT				NOT NULL
		,e_stop_sequence	INT				NOT NULL
		,abcde_direction_id	INT				NOT NULL
		,ab_route_id		VARCHAR(255)	NOT NULL
		,cde_route_id		VARCHAR(255)	NOT NULL
		,abcde_route_type	INT				NOT NULL
		,ab_trip_id			VARCHAR(255)	NOT NULL
		,cde_trip_id		VARCHAR(255)	NOT NULL
		,ab_vehicle_id		VARCHAR(255)	NOT NULL
		,cde_vehicle_id		VARCHAR(255)	NOT NULL
		,a_record_id		INT				NOT NULL
		,b_record_id		INT				NOT NULL
		,c_record_id		INT				NOT NULL
		,d_record_id		INT				NOT NULL
		,e_record_id		INT				NOT NULL
		,a_time_sec			INT				NOT NULL
		,b_time_sec			INT				NOT NULL
		,c_time_sec			INT				NOT NULL
		,d_time_sec			INT				NOT NULL
		,e_time_sec			INT				NOT NULL
		,cd_time_sec		INT				NOT NULL
		,de_time_sec		INT				NOT NULL
		,bd_time_sec		INT				NOT NULL

	)

	INSERT INTO ##daily_abcde_time
	(
		service_date
		,abcd_stop_id
		,e_stop_id
		,ab_stop_sequence
		,cd_stop_sequence
		,e_stop_sequence
		,abcde_direction_id
		,ab_route_id
		,cde_route_id
		,abcde_route_type
		,ab_trip_id
		,cde_trip_id
		,ab_vehicle_id
		,cde_vehicle_id
		,a_record_id
		,b_record_id
		,c_record_id
		,d_record_id
		,e_record_id
		,a_time_sec
		,b_time_sec
		,c_time_sec
		,d_time_sec
		,e_time_sec
		,cd_time_sec
		,de_time_sec
		,bd_time_sec
	)

		SELECT
			service_date
			,abcd_stop_id
			,e_stop_id
			,ab_stop_sequence
			,cd_stop_sequence
			,e_stop_sequence
			,abcde_direction_id
			,ab_route_id
			,cde_route_id
			,abcde_route_type
			,ab_trip_id
			,cde_trip_id
			,ab_vehicle_id
			,cde_vehicle_id
			,a_record_id
			,b_record_id
			,c_record_id
			,d_record_id
			,e_record_id
			,a_time_sec
			,b_time_sec
			,c_time_sec
			,d_time_sec
			,e_time_sec
			,cd_time_sec
			,de_time_sec
			,bd_time_sec

		FROM
		(
			SELECT
				y.service_date
				,y.cd_stop_id AS abcd_stop_id
				,y.e_stop_id
				,x.cd_stop_sequence AS ab_stop_sequence
				,y.cd_stop_sequence
				,y.e_stop_sequence
				,y.cde_direction_id AS abcde_direction_id
				,x.cde_route_id AS ab_route_id
				,y.cde_route_id
				,y.cde_route_type AS abcde_route_type
				,x.cde_trip_id AS ab_trip_id
				,y.cde_trip_id
				,x.cde_vehicle_id AS ab_vehicle_id
				,y.cde_vehicle_id
				,x.c_record_id AS a_record_id
				,x.d_record_id AS b_record_id
				,y.c_record_id
				,y.d_record_id
				,y.e_record_id
				,x.c_time_sec AS a_time_sec
				,x.d_time_sec AS b_time_sec
				,y.c_time_sec
				,y.d_time_sec
				,y.e_time_sec
				,y.cd_time_sec
				,y.de_time_sec
				,y.d_time_sec - x.d_time_sec AS bd_time_sec

				,ROW_NUMBER() OVER ( -- Partition finds the most recent relevant "previous" trip travelling from d to e.
				PARTITION BY
				y.c_record_id
				,y.d_record_id
				,y.e_record_id
				ORDER BY x.d_time_sec DESC) AS rn

			FROM ##daily_cde_time y --y is the "current" trip

			JOIN ##daily_cde_time x --x is the most recent relevant "previous" trip

				ON
					(
					y.service_date = x.service_date
					AND y.cd_stop_id = x.cd_stop_id
					AND y.e_stop_id = x.e_stop_id
					AND y.cde_direction_id = x.cde_direction_id
					AND y.cde_vehicle_id <> x.cde_vehicle_id
					AND y.cde_trip_id <> x.cde_trip_id
					AND y.c_time_sec > x.d_time_sec --the arrival time of the current trip should be later than the departure time of the previous trip
					--, but not by more than 30 minutes, as determined by the next statement
					AND
					y.c_time_sec - x.d_time_sec <= 1800
					)
		) temp
		WHERE
			rn = 1

	--Create temp table bd_sr_all_time. This table stores the day being processed's headway times at a stop between trips of all routes
	IF OBJECT_ID('tempdb..##daily_bd_sr_all_time','u') IS NOT NULL
		DROP TABLE ##daily_bd_sr_all_time;

	CREATE TABLE ##daily_bd_sr_all_time
	(
		service_date		VARCHAR(255)	NOT NULL
		,bd_stop_id			VARCHAR(255)	NOT NULL
		,b_stop_sequence	INT				NOT NULL
		,d_stop_sequence	INT				NOT NULL
		,b_route_id			VARCHAR(255)	NOT NULL
		,d_route_id			VARCHAR(255)	NOT NULL
		,bd_route_type		INT				NOT NULL
		,bd_direction_id	INT				NOT NULL
		,b_trip_id			VARCHAR(255)	NOT NULL
		,d_trip_id			VARCHAR(255)	NOT NULL
		,b_vehicle_id		VARCHAR(255)	NOT NULL
		,d_vehicle_id		VARCHAR(255)	NOT NULL
		,b_record_id		INT				NOT NULL
		,d_record_id		INT				NOT NULL
		,b_time_sec			INT				NOT NULL
		,d_time_sec			INT				NOT NULL
		,bd_time_sec		INT				NOT NULL
	)
	;

	INSERT INTO ##daily_bd_sr_all_time
	(
		service_date
		,bd_stop_id
		,b_stop_sequence
		,d_stop_sequence
		,b_route_id
		,d_route_id
		,bd_route_type
		,bd_direction_id
		,b_trip_id
		,d_trip_id
		,b_vehicle_id
		,d_vehicle_id
		,b_record_id
		,d_record_id
		,b_time_sec
		,d_time_sec
		,bd_time_sec
	)

		SELECT
			service_date
			,bd_stop_id
			,b_stop_sequence
			,d_stop_sequence
			,b_route_id
			,d_route_id
			,bd_route_type
			,bd_direction_id
			,b_trip_id
			,d_trip_id
			,b_vehicle_id
			,d_vehicle_id
			,b_record_id
			,d_record_id
			,b_time_sec
			,d_time_sec
			,bd_time_sec

		FROM
		(
			SELECT
				y.service_date
				,y.stop_id AS bd_stop_id
				,x.stop_sequence AS b_stop_sequence
				,y.stop_sequence AS d_stop_sequence
				,x.route_id AS b_route_id
				,y.route_id AS d_route_id
				,y.route_type AS bd_route_type
				,y.direction_id AS bd_direction_id
				,x.trip_id AS b_trip_id
				,y.trip_id AS d_trip_id
				,x.vehicle_id AS b_vehicle_id
				,y.vehicle_id AS d_vehicle_id
				,x.record_id AS b_record_id
				,y.record_id AS d_record_id
				,x.event_time_sec AS b_time_sec
				,y.event_time_sec AS d_time_sec
				,y.event_time_sec - x.event_time_sec AS bd_time_sec
				,ROW_NUMBER() OVER (
				PARTITION BY
				y.record_id
				ORDER BY x.event_time_sec DESC) AS rn

			FROM dbo.daily_event y --y is the "current" trip

			JOIN dbo.daily_event x --x is the most recent "previous" trip

				ON
					(
					y.event_type = 'DEP'
					AND x.event_type = 'DEP'
					AND y.service_date = x.service_date
					AND y.stop_id = x.stop_id
					AND y.direction_id = x.direction_id
					AND y.vehicle_id <> x.vehicle_id
					AND y.trip_id <> x.trip_id
					--AND 
					--	y.route_id =x.route_id
					AND y.event_time_sec >= x.event_time_sec --Green Line at park can have two with exactly the same time
					AND y.event_time_sec - x.event_time_sec <= 1800
					)
			WHERE
				y.suspect_record = 0
				AND x.suspect_record = 0
		) temp
		WHERE
			rn = 1

	--Create temp table rt_bd_sr_same_time. This table stores the day being processed's headway times at a stop between trips of the same routes in real-time
	IF OBJECT_ID('tempdb..##daily_bd_sr_same_time','u') IS NOT NULL
		DROP TABLE ##daily_bd_sr_same_time

	CREATE TABLE ##daily_bd_sr_same_time
	(
		service_date		VARCHAR(255)	NOT NULL
		,bd_stop_id			VARCHAR(255)	NOT NULL
		,b_stop_sequence	INT				NOT NULL
		,d_stop_sequence	INT				NOT NULL
		,bd_route_id		VARCHAR(255)	NOT NULL
		,bd_route_type		INT				NOT NULL
		,bd_direction_id	INT				NOT NULL
		,b_trip_id			VARCHAR(255)	NOT NULL
		,d_trip_id			VARCHAR(255)	NOT NULL
		,b_vehicle_id		VARCHAR(255)	NOT NULL
		,d_vehicle_id		VARCHAR(255)	NOT NULL
		,b_record_id		INT				NOT NULL
		,d_record_id		INT				NOT NULL
		,b_time_sec			INT				NOT NULL
		,d_time_sec			INT				NOT NULL
		,bd_time_sec		INT				NOT NULL
	)
	;

	INSERT INTO ##daily_bd_sr_same_time
	(
		service_date
		,bd_stop_id
		,b_stop_sequence
		,d_stop_sequence
		,bd_route_id
		,bd_route_type
		,bd_direction_id
		,b_trip_id
		,d_trip_id
		,b_vehicle_id
		,d_vehicle_id
		,b_record_id
		,d_record_id
		,b_time_sec
		,d_time_sec
		,bd_time_sec
	)

		SELECT
			service_date
			,bd_stop_id
			,b_stop_sequence
			,d_stop_sequence
			,bd_route_id
			,bd_route_type
			,bd_direction_id
			,b_trip_id
			,d_trip_id
			,b_vehicle_id
			,d_vehicle_id
			,b_record_id
			,d_record_id
			,b_time_sec
			,d_time_sec
			,bd_time_sec
		FROM
		(
			SELECT
				y.service_date
				,y.stop_id AS bd_stop_id
				,x.stop_sequence AS b_stop_sequence
				,y.stop_sequence AS d_stop_sequence
				,y.route_id AS bd_route_id
				,y.route_type AS bd_route_type
				,y.direction_id AS bd_direction_id
				,x.trip_id AS b_trip_id
				,y.trip_id AS d_trip_id
				,x.vehicle_id AS b_vehicle_id
				,y.vehicle_id AS d_vehicle_id
				,x.record_id AS b_record_id
				,y.record_id AS d_record_id
				,x.event_time_sec AS b_time_sec
				,y.event_time_sec AS d_time_sec
				,y.event_time_sec - x.event_time_sec AS bd_time_sec

				,ROW_NUMBER() OVER (
				PARTITION BY
				y.record_id
				ORDER BY x.event_time_sec DESC) AS rn

			FROM dbo.daily_event y --y is the most recent "previous" trip

			JOIN dbo.daily_event x --x is the "current" trip

				ON
					(
					y.event_type = 'DEP'
					AND x.event_type = 'DEP'
					AND y.service_date = x.service_date
					AND y.stop_id = x.stop_id
					AND y.direction_id = x.direction_id
					AND y.vehicle_id <> x.vehicle_id
					AND y.trip_id <> x.trip_id
					AND y.route_id = x.route_id --for routes that are the same
					AND y.event_time_sec >= x.event_time_sec --Green Line at park can have two with exactly the same time
					AND y.event_time_sec - x.event_time_sec <= 1800
					)
			WHERE
				y.suspect_record = 0
				AND x.suspect_record = 0
		) temp
		WHERE
			rn = 1


	--------------------------------------------------------------------schedule adherence inputs part starts------------------------------------------------

	--set up daily disaggregate schedule adherence tables

	IF OBJECT_ID('tempdb..#daily_arrival_time_sec','U') IS NOT NULL
		DROP TABLE #daily_arrival_time_sec

	CREATE TABLE #daily_arrival_time_sec
	(
		service_date				VARCHAR(255)	NOT NULL
		,route_id					VARCHAR(255)	NOT NULL
		,route_type					INT				NOT NULL
		,direction_id				INT				NOT NULL
		,trip_id					VARCHAR(255)	NOT NULL
		,stop_sequence				INT				NOT NULL
		,stop_id					VARCHAR(255)	NOT NULL
		,vehicle_id					VARCHAR(255)
		,scheduled_arrival_time_sec	INT
		,actual_arrival_time_sec	INT
		,arrival_delay_sec			INT
		,stop_order_flag			INT -- 1 is first stop, 2 is mid stop, 3 is last stop
		,agency_timepoint_name		VARCHAR(255)
	)
	;

	INSERT INTO #daily_arrival_time_sec
	(
		service_date
		,route_id
		,route_type
		,direction_id
		,trip_id
		,stop_sequence
		,stop_id
		,vehicle_id
		,scheduled_arrival_time_sec
		,actual_arrival_time_sec
		,arrival_delay_sec
		,stop_order_flag
		,agency_timepoint_name
	)

		SELECT
			ds.service_date
			,ds.route_id
			,ds.route_type
			,ds.direction_id
			,ds.trip_id
			,ds.stop_sequence
			,ds.stop_id
			,rea.vehicle_id
			,ds.arrival_time_sec AS scheduled_arrival_time
			,rea.event_time_sec AS actual_arrival_time
			,rea.event_time_sec - ds.arrival_time_sec AS arrival_delay
			,ds.stop_order_flag
			,ds.agency_timepoint_name

		FROM	dbo.daily_stop_times_sec ds
				,dbo.daily_event rea
		WHERE
			(rea.event_type = 'ARR'
			OR rea.event_type = 'PRA')
			AND rea.service_date = ds.service_date
			AND rea.trip_id = ds.trip_id
			AND rea.stop_id = ds.stop_id
			AND rea.stop_sequence = ds.stop_sequence
			AND rea.suspect_record = 0

	IF OBJECT_ID('tempdb..#daily_departure_time_sec','U') IS NOT NULL
		DROP TABLE #daily_departure_time_sec

	CREATE TABLE #daily_departure_time_sec
	(
		service_date					VARCHAR(255)	NOT NULL
		,route_id						VARCHAR(255)	NOT NULL
		,route_type						INT				NOT NULL
		,direction_id					INT				NOT NULL
		,trip_id						VARCHAR(255)	NOT NULL
		,stop_sequence					INT				NOT NULL
		,stop_id						VARCHAR(255)	NOT NULL
		,vehicle_id						VARCHAR(255)
		,scheduled_departure_time_sec	INT
		,actual_departure_time_sec		INT
		,departure_delay_sec			INT
		,stop_order_flag				INT
		,agency_timepoint_name			VARCHAR(255)
	)
	;

	INSERT INTO #daily_departure_time_sec
	(
		service_date
		,route_id
		,route_type
		,direction_id
		,trip_id
		,stop_sequence
		,stop_id
		,vehicle_id
		,scheduled_departure_time_sec
		,actual_departure_time_sec
		,departure_delay_sec
		,stop_order_flag
		,agency_timepoint_name
	)

		SELECT
			ds.service_date
			,ds.route_id
			,ds.route_type
			,ds.direction_id
			,ds.trip_id
			,ds.stop_sequence
			,ds.stop_id
			,red.vehicle_id
			,ds.departure_time_sec AS scheduled_departure_time
			,red.event_time_sec AS actual_departure_time
			,red.event_time_sec - ds.departure_time_sec AS departure_delay
			,ds.stop_order_flag
			,ds.agency_timepoint_name
		FROM	dbo.daily_stop_times_sec ds
				,dbo.daily_event red
		WHERE
			red.event_type = 'DEP'
			AND red.service_date = ds.service_date
			AND red.trip_id = ds.trip_id
			AND red.stop_id = ds.stop_id
			AND red.stop_sequence = ds.stop_sequence
			AND red.suspect_record = 0

	--------------------------------------------------------------------schedule adherence inputs part ends---------------------------------------------------


	--Determine which scheduled stops were not in actual events and remove related records from abcde time daily

	IF OBJECT_ID('dbo.daily_missed_stop_times_scheduled','U') IS NOT NULL
		DROP TABLE dbo.daily_missed_stop_times_scheduled
		;

	CREATE TABLE dbo.daily_missed_stop_times_scheduled
	(
		record_id								INT	IDENTITY PRIMARY KEY
		,service_date							DATE
		,trip_id								VARCHAR(255)
		,stop_sequence							INT
		,stop_id								VARCHAR(255)
		,scheduled_arrival_time_sec				INT
		,scheduled_departure_time_sec			INT
		,actual_arrival_time_sec				INT
		,actual_departure_time_sec				INT
		,max_before_stop_sequence				INT
		,max_before_arrival_time_sec			FLOAT
		,max_before_departure_time_sec			FLOAT
		,max_before_event_time_arrival_sec		FLOAT
		,max_before_event_time_departure_sec	FLOAT
		,min_after_stop_sequence				INT
		,min_after_arrival_time_sec				FLOAT
		,min_after_departure_time_sec			FLOAT
		,min_after_event_time_arrival_sec		FLOAT
		,min_after_event_time_departure_sec		FLOAT
		,expected_arrival_time_sec				INT
		,expected_departure_time_sec			INT
	)

	--insert all stop times for scheduled trips for the day
	INSERT INTO daily_missed_stop_times_scheduled
	(
		service_date
		,trip_id
		,stop_sequence
		,stop_id
		,scheduled_arrival_time_sec
		,scheduled_departure_time_sec
	)

		SELECT
			st.service_date
			,st.trip_id
			,st.stop_sequence
			,st.stop_id
			,st.arrival_time_sec
			,st.departure_time_sec
		FROM dbo.daily_stop_times_sec st

		WHERE
			st.trip_id IN
			(
				SELECT DISTINCT
					trip_id
				FROM dbo.daily_event
			)
			AND st.route_type IN (0,1)

	--delete all stop times for scheduled trips where we got an event. we should be left with all stop times for scheduled trips where we did not get an event		
	DELETE FROM daily_missed_stop_times_scheduled
	FROM	daily_missed_stop_times_scheduled tst
			,
			(
				SELECT
					trip_id
					,stop_id
					,stop_sequence
					,COUNT(*) AS perfect_record
				FROM dbo.daily_event ed
				WHERE
					suspect_record = 0
				GROUP BY
					trip_id
					,stop_id
					,stop_sequence
				HAVING COUNT(*) = 2
			) temp

	WHERE
		tst.trip_id = temp.trip_id
		AND tst.stop_sequence = temp.stop_sequence
		AND tst.stop_id = temp.stop_id

	UPDATE daily_missed_stop_times_scheduled
		SET actual_arrival_time_sec = ed.event_time_sec
		FROM daily_missed_stop_times_scheduled mst
			JOIN
			daily_event ed
				ON
					mst.trip_id = ed.trip_id
					AND mst.stop_id = ed.stop_id
					AND mst.stop_sequence = ed.stop_sequence
					AND ed.event_type = 'ARR'
					AND ed.suspect_record = 0

	UPDATE daily_missed_stop_times_scheduled
		SET actual_departure_time_sec = ed.event_time_sec
		FROM daily_missed_stop_times_scheduled mst
			JOIN
			daily_event ed
				ON
					mst.trip_id = ed.trip_id
					AND mst.stop_id = ed.stop_id
					AND mst.stop_sequence = ed.stop_sequence
					AND ed.event_type = 'DEP'
					AND ed.suspect_record = 0



	---first update for arrivals----------	
	--fill in the expected times for the missed events based on the actual times of the previous event and next event

	UPDATE daily_missed_stop_times_scheduled
		SET	min_after_stop_sequence = mb.min_after_stop_sequence
			,min_after_event_time_arrival_sec = mb.min_after_event_time_arrival_sec
		FROM daily_missed_stop_times_scheduled mst,
			(
				SELECT
					mst.record_id
					,MIN(ed.stop_sequence) AS min_after_stop_sequence
					,MIN(ed.event_time_sec) AS min_after_event_time_arrival_sec
				FROM	dbo.daily_event ed
						,daily_missed_stop_times_scheduled mst
				WHERE
					mst.trip_id = ed.trip_id
					AND ed.event_type = 'ARR'
					AND mst.stop_sequence < ed.stop_sequence
				GROUP BY
					mst.record_id
			) AS mb

		WHERE
			mst.record_id = mb.record_id

	UPDATE daily_missed_stop_times_scheduled
		SET min_after_arrival_time_sec = st.arrival_time_sec
		FROM daily_missed_stop_times_scheduled mst,
			gtfs.stop_times st
		WHERE
			mst.trip_id = st.trip_id
			AND mst.min_after_stop_sequence = st.stop_sequence

	UPDATE daily_missed_stop_times_scheduled
		SET	max_before_stop_sequence = ma.max_before_stop_sequence
			,max_before_event_time_arrival_sec = ma.max_before_event_time_arrival_sec
		FROM daily_missed_stop_times_scheduled mst,
			(
				SELECT
					mst.record_id
					,MAX(ed.stop_sequence) AS max_before_stop_sequence
					,MAX(ed.event_time_sec) AS max_before_event_time_arrival_sec
				FROM	dbo.daily_event ed
						,daily_missed_stop_times_scheduled mst
				WHERE
					mst.trip_id = ed.trip_id
					AND ed.event_type = 'ARR'
					AND mst.stop_sequence > ed.stop_sequence
				GROUP BY
					mst.record_id
			) AS ma

		WHERE
			mst.record_id = ma.record_id

	UPDATE daily_missed_stop_times_scheduled
		SET max_before_arrival_time_sec = st.arrival_time_sec
		FROM daily_missed_stop_times_scheduled mst,
			gtfs.stop_times st
		WHERE
			mst.trip_id = st.trip_id
			AND mst.max_before_stop_sequence = st.stop_sequence

	UPDATE daily_missed_stop_times_scheduled
		SET expected_arrival_time_sec = CAST((scheduled_arrival_time_sec - max_before_arrival_time_sec) / (min_after_arrival_time_sec - max_before_arrival_time_sec) * (min_after_event_time_arrival_sec - max_before_event_time_arrival_sec) + max_before_event_time_arrival_sec AS INT)
		FROM daily_missed_stop_times_scheduled mst
		WHERE
			mst.max_before_stop_sequence IS NOT NULL
			AND min_after_stop_sequence IS NOT NULL

	UPDATE daily_missed_stop_times_scheduled
		SET expected_arrival_time_sec = min_after_event_time_arrival_sec - (min_after_arrival_time_sec - scheduled_arrival_time_sec)
		FROM daily_missed_stop_times_scheduled
		WHERE
			max_before_stop_sequence IS NULL
			AND min_after_stop_sequence IS NOT NULL

	UPDATE daily_missed_stop_times_scheduled
		SET expected_arrival_time_sec = max_before_event_time_arrival_sec + (scheduled_arrival_time_sec - max_before_arrival_time_sec)
		FROM daily_missed_stop_times_scheduled
		WHERE
			max_before_stop_sequence IS NOT NULL
			AND min_after_stop_sequence IS NULL

	------then update for departures--------------------------------
	UPDATE daily_missed_stop_times_scheduled
		SET	min_after_stop_sequence = mb.min_after_stop_sequence
			,min_after_event_time_departure_sec = mb.min_after_event_time_departure_sec
		FROM daily_missed_stop_times_scheduled mst,
			(
				SELECT
					mst.record_id
					,MIN(ed.stop_sequence) AS min_after_stop_sequence
					,MIN(ed.event_time_sec) AS min_after_event_time_departure_sec
				FROM	dbo.daily_event ed
						,daily_missed_stop_times_scheduled mst
				WHERE
					mst.trip_id = ed.trip_id
					AND ed.event_type = 'DEP'
					AND mst.stop_sequence < ed.stop_sequence
				GROUP BY
					mst.record_id
			) AS mb

		WHERE
			mst.record_id = mb.record_id

	UPDATE daily_missed_stop_times_scheduled
		SET min_after_departure_time_sec = st.departure_time_sec
		FROM daily_missed_stop_times_scheduled mst,
			gtfs.stop_times st
		WHERE
			mst.trip_id = st.trip_id
			AND mst.min_after_stop_sequence = st.stop_sequence

	UPDATE daily_missed_stop_times_scheduled
		SET	max_before_stop_sequence = ma.max_before_stop_sequence
			,max_before_event_time_departure_sec = ma.max_before_event_time_departure_sec
		FROM daily_missed_stop_times_scheduled mst,
			(
				SELECT
					mst.record_id
					,MAX(ed.stop_sequence) AS max_before_stop_sequence
					,MAX(ed.event_time_sec) AS max_before_event_time_departure_sec
				FROM	dbo.daily_event ed
						,daily_missed_stop_times_scheduled mst
				WHERE
					mst.trip_id = ed.trip_id
					AND ed.event_type = 'DEP'
					AND mst.stop_sequence > ed.stop_sequence
				GROUP BY
					mst.record_id
			) AS ma

		WHERE
			mst.record_id = ma.record_id

	UPDATE daily_missed_stop_times_scheduled
		SET max_before_departure_time_sec = st.departure_time_sec
		FROM daily_missed_stop_times_scheduled mst,
			gtfs.stop_times st
		WHERE
			mst.trip_id = st.trip_id
			AND mst.max_before_stop_sequence = st.stop_sequence

	UPDATE daily_missed_stop_times_scheduled
		SET expected_departure_time_sec = CAST((scheduled_departure_time_sec - max_before_departure_time_sec) / (min_after_departure_time_sec - max_before_departure_time_sec) * (min_after_event_time_departure_sec - max_before_event_time_departure_sec) + max_before_event_time_departure_sec AS INT)
		FROM daily_missed_stop_times_scheduled mst
		WHERE
			mst.max_before_stop_sequence IS NOT NULL
			AND min_after_stop_sequence IS NOT NULL

	UPDATE daily_missed_stop_times_scheduled
		SET expected_departure_time_sec = min_after_event_time_departure_sec - (min_after_departure_time_sec - scheduled_departure_time_sec)
		FROM daily_missed_stop_times_scheduled
		WHERE
			max_before_stop_sequence IS NULL
			AND min_after_stop_sequence IS NOT NULL

	UPDATE daily_missed_stop_times_scheduled
		SET expected_departure_time_sec = max_before_event_time_departure_sec + (scheduled_departure_time_sec - max_before_departure_time_sec)
		FROM daily_missed_stop_times_scheduled
		WHERE
			max_before_stop_sequence IS NOT NULL
			AND min_after_stop_sequence IS NULL

	IF
		(
			SELECT
				COUNT(*)
			FROM dbo.deleted_from_abcde_time
			WHERE
				service_date = @service_date_process
		)
		> 0

		DELETE FROM dbo.deleted_from_abcde_time
		WHERE
			service_date = @service_date_process

	--save the records we are going to delete from ##daily_abcde_time where the headway is missed at the from stop
	INSERT INTO dbo.deleted_from_abcde_time
	(
		service_date
		,abcd_stop_id
		,e_stop_id
		,ab_stop_sequence
		,cd_stop_sequence
		,e_stop_sequence
		,abcde_direction_id
		,ab_route_id
		,cde_route_id
		,abcde_route_type
		,ab_trip_id
		,cde_trip_id
		,ab_vehicle_id
		,cde_vehicle_id
		,a_record_id
		,b_record_id
		,c_record_id
		,d_record_id
		,e_record_id
		,a_time_sec
		,b_time_sec
		,c_time_sec
		,d_time_sec
		,e_time_sec
		,cd_time_sec
		,de_time_sec
		,bd_time_sec
	)
		SELECT
			abcde.service_date
			,abcde.abcd_stop_id
			,abcde.e_stop_id
			,abcde.ab_stop_sequence
			,abcde.cd_stop_sequence
			,abcde.e_stop_sequence
			,abcde.abcde_direction_id
			,abcde.ab_route_id
			,abcde.cde_route_id
			,abcde.abcde_route_type
			,abcde.ab_trip_id
			,abcde.cde_trip_id
			,abcde.ab_vehicle_id
			,abcde.cde_vehicle_id
			,abcde.a_record_id
			,abcde.b_record_id
			,abcde.c_record_id
			,abcde.d_record_id
			,abcde.e_record_id
			,abcde.a_time_sec
			,abcde.b_time_sec
			,abcde.c_time_sec
			,abcde.d_time_sec
			,abcde.e_time_sec
			,abcde.cd_time_sec
			,abcde.de_time_sec
			,abcde.bd_time_sec
		FROM	##daily_abcde_time abcde
				,daily_missed_stop_times_scheduled mst
		WHERE
			abcde.service_date = mst.service_date
			AND abcde.abcd_stop_id = mst.stop_id
			AND abcde.b_time_sec < expected_departure_time_sec
			AND abcde.d_time_sec > expected_departure_time_sec
			AND abcde.abcd_stop_id NOT IN ('70061','70105','70093','70094','70060','70038','70036','70001') ---these are terminal stops on Red, Orange, Blue lines
			AND mst.actual_departure_time_sec IS NULL

	-- DELETE where the headway is missed at the from stop------------------
	DELETE FROM ##daily_abcde_time
	FROM	##daily_abcde_time abcde
			,daily_missed_stop_times_scheduled mst
	WHERE
		abcde.service_date = mst.service_date
		AND abcde.abcd_stop_id = mst.stop_id
		AND abcde.b_time_sec < expected_departure_time_sec
		AND abcde.d_time_sec > expected_departure_time_sec
		AND abcde.abcd_stop_id NOT IN ('70061','70105','70093','70094','70060','70038','70036','70001')
		AND mst.actual_departure_time_sec IS NULL

	DELETE FROM ##daily_bd_sr_same_time
	FROM	##daily_bd_sr_same_time hsr
			,daily_missed_stop_times_scheduled mst
	WHERE
		hsr.service_date = mst.service_date
		AND hsr.bd_stop_id = mst.stop_id
		AND hsr.b_time_sec < expected_departure_time_sec
		AND hsr.d_time_sec > expected_departure_time_sec
		AND hsr.bd_stop_id NOT IN ('70061','70105','70093','70094','70060','70038','70036','70001')
		AND mst.actual_departure_time_sec IS NULL

	DELETE FROM ##daily_bd_sr_all_time
	FROM	##daily_bd_sr_all_time hsr
			,daily_missed_stop_times_scheduled mst
	WHERE
		hsr.service_date = mst.service_date
		AND hsr.bd_stop_id = mst.stop_id
		AND hsr.b_time_sec < expected_departure_time_sec
		AND hsr.d_time_sec > expected_departure_time_sec
		AND hsr.bd_stop_id NOT IN ('70061','70105','70093','70094','70060','70038','70036','70001')
		AND mst.actual_departure_time_sec IS NULL

	--put records that are deleted to remove records where we know we missed the arrival at the to_stop (e) for abcde
	INSERT INTO dbo.deleted_from_abcde_time
	(
		service_date
		,abcd_stop_id
		,e_stop_id
		,ab_stop_sequence
		,cd_stop_sequence
		,e_stop_sequence
		,abcde_direction_id
		,ab_route_id
		,cde_route_id
		,abcde_route_type
		,ab_trip_id
		,cde_trip_id
		,ab_vehicle_id
		,cde_vehicle_id
		,a_record_id
		,b_record_id
		,c_record_id
		,d_record_id
		,e_record_id
		,a_time_sec
		,b_time_sec
		,c_time_sec
		,d_time_sec
		,e_time_sec
		,cd_time_sec
		,de_time_sec
		,bd_time_sec
	)
		SELECT
			abcde.service_date
			,abcde.abcd_stop_id
			,abcde.e_stop_id
			,abcde.ab_stop_sequence
			,abcde.cd_stop_sequence
			,abcde.e_stop_sequence
			,abcde.abcde_direction_id
			,abcde.ab_route_id
			,abcde.cde_route_id
			,abcde.abcde_route_type
			,abcde.ab_trip_id
			,abcde.cde_trip_id
			,abcde.ab_vehicle_id
			,abcde.cde_vehicle_id
			,abcde.a_record_id
			,abcde.b_record_id
			,abcde.c_record_id
			,abcde.d_record_id
			,abcde.e_record_id
			,abcde.a_time_sec
			,abcde.b_time_sec
			,abcde.c_time_sec
			,abcde.d_time_sec
			,abcde.e_time_sec
			,abcde.cd_time_sec
			,abcde.de_time_sec
			,abcde.bd_time_sec
		FROM	##daily_abcde_time abcde
				,
				(
					SELECT
						mst.stop_id AS e_stop_id
						,cd.cd_stop_id
						,cd.d_time_sec
					FROM	##daily_cd_time cd
							,daily_missed_stop_times_scheduled mst
					WHERE
						cd.cd_trip_id = mst.trip_id
						AND cd.cd_stop_sequence < mst.stop_sequence
						AND mst.actual_arrival_time_sec IS NULL
				) temp
		WHERE
			abcde.abcd_stop_id = temp.cd_stop_id
			AND abcde.b_time_sec < temp.d_time_sec
			AND abcde.d_time_sec > temp.d_time_sec
			AND abcde.e_stop_id = temp.e_stop_id


	--DELETE to remove records where we know we missed the arrival at the to_stop (e) for abcde--------
	DELETE FROM ##daily_abcde_time
	FROM	##daily_abcde_time abcde
			,
			(
				SELECT
					mst.stop_id AS e_stop_id
					,cd.cd_stop_id
					,cd.d_time_sec
				FROM	##daily_cd_time cd
						,daily_missed_stop_times_scheduled mst
				WHERE
					cd.cd_trip_id = mst.trip_id
					AND cd.cd_stop_sequence < mst.stop_sequence
					AND mst.actual_arrival_time_sec IS NULL
			) temp
	WHERE
		abcde.abcd_stop_id = temp.cd_stop_id
		AND abcde.b_time_sec < temp.d_time_sec
		AND abcde.d_time_sec > temp.d_time_sec
		AND abcde.e_stop_id = temp.e_stop_id

	--put records that will be DELETED headways between events with suspect records in the middle
	INSERT INTO dbo.deleted_from_abcde_time
	(
		service_date
		,abcd_stop_id
		,e_stop_id
		,ab_stop_sequence
		,cd_stop_sequence
		,e_stop_sequence
		,abcde_direction_id
		,ab_route_id
		,cde_route_id
		,abcde_route_type
		,ab_trip_id
		,cde_trip_id
		,ab_vehicle_id
		,cde_vehicle_id
		,a_record_id
		,b_record_id
		,c_record_id
		,d_record_id
		,e_record_id
		,a_time_sec
		,b_time_sec
		,c_time_sec
		,d_time_sec
		,e_time_sec
		,cd_time_sec
		,de_time_sec
		,bd_time_sec
	)
		SELECT
			abcde.service_date
			,abcde.abcd_stop_id
			,abcde.e_stop_id
			,abcde.ab_stop_sequence
			,abcde.cd_stop_sequence
			,abcde.e_stop_sequence
			,abcde.abcde_direction_id
			,abcde.ab_route_id
			,abcde.cde_route_id
			,abcde.abcde_route_type
			,abcde.ab_trip_id
			,abcde.cde_trip_id
			,abcde.ab_vehicle_id
			,abcde.cde_vehicle_id
			,abcde.a_record_id
			,abcde.b_record_id
			,abcde.c_record_id
			,abcde.d_record_id
			,abcde.e_record_id
			,abcde.a_time_sec
			,abcde.b_time_sec
			,abcde.c_time_sec
			,abcde.d_time_sec
			,abcde.e_time_sec
			,abcde.cd_time_sec
			,abcde.de_time_sec
			,abcde.bd_time_sec
		FROM	##daily_abcde_time abcde
				,daily_event mst
		WHERE
			abcde.service_date = mst.service_date
			AND abcde.abcd_stop_id = mst.stop_id
			AND abcde.b_time_sec < event_time_sec
			AND abcde.d_time_sec > event_time_sec
			AND abcde.abcd_stop_id NOT IN ('70061','70105','70093','70094','70060','70038','70036','70001')
			AND mst.suspect_record = 1
			AND mst.event_type = 'DEP'

	---DELETE headways between events with suspect records in the middle-------------------------------
	DELETE FROM ##daily_abcde_time
	FROM	##daily_abcde_time abcde
			,daily_event mst
	WHERE
		abcde.service_date = mst.service_date
		AND abcde.abcd_stop_id = mst.stop_id
		AND abcde.b_time_sec < event_time_sec
		AND abcde.d_time_sec > event_time_sec
		AND abcde.abcd_stop_id NOT IN ('70061','70105','70093','70094','70060','70038','70036','70001')
		AND mst.suspect_record = 1
		AND mst.event_type = 'DEP'

	DELETE FROM ##daily_bd_sr_same_time
	FROM	##daily_bd_sr_same_time hsr
			,daily_event mst
	WHERE
		hsr.service_date = mst.service_date
		AND hsr.bd_stop_id = mst.stop_id
		AND hsr.b_time_sec < event_time_sec
		AND hsr.d_time_sec > event_time_sec
		AND hsr.bd_stop_id NOT IN ('70061','70105','70093','70094','70060','70038','70036','70001')
		AND mst.suspect_record = 1
		AND mst.event_type = 'DEP'

	DELETE FROM ##daily_bd_sr_all_time
	FROM	##daily_bd_sr_all_time hsr
			,daily_event mst
	WHERE
		hsr.service_date = mst.service_date
		AND hsr.bd_stop_id = mst.stop_id
		AND hsr.b_time_sec < event_time_sec
		AND hsr.d_time_sec > event_time_sec
		AND hsr.bd_stop_id NOT IN ('70061','70105','70093','70094','70060','70038','70036','70001')
		AND mst.suspect_record = 1
		AND mst.event_type = 'DEP'

	--Create passenger weighted travel time vs. threshold tables 
	IF OBJECT_ID('dbo.daily_travel_time_threshold_pax','U') IS NOT NULL
		DROP TABLE dbo.daily_travel_time_threshold_pax

	CREATE TABLE dbo.daily_travel_time_threshold_pax
	(
		service_date									VARCHAR(255)	NOT NULL
		,from_stop_id									VARCHAR(255)	NOT NULL
		,to_stop_id										VARCHAR(255)	NOT NULL
		,direction_id									INT				NOT NULL
		,prev_route_id									VARCHAR(255)	NOT NULL
		,route_id										VARCHAR(255)	NOT NULL
		,trip_id										VARCHAR(255)	NOT NULL
		,start_time_sec									INT				NOT NULL
		,end_time_sec									INT				NOT NULL
		,travel_time_sec								INT				NOT NULL
		,threshold_id									VARCHAR(255)	NOT NULL
		,threshold_historical_median_travel_time_sec	INT
		,threshold_scheduled_median_travel_time_sec		INT				NOT NULL
		,threshold_historical_average_travel_time_sec	INT
		,threshold_scheduled_average_travel_time_sec	INT				NOT NULL
		,denominator_pax								FLOAT			NULL
		,historical_threshold_numerator_pax				FLOAT			NULL
		,scheduled_threshold_numerator_pax				FLOAT			NULL
	)


	CREATE NONCLUSTERED INDEX IX_daily_travel_time_threshold_pax_from_stop_id ON daily_travel_time_threshold_pax (from_stop_id);

	CREATE NONCLUSTERED INDEX IX_daily_travel_time_threshold_pax_to_stop_id ON daily_travel_time_threshold_pax (to_stop_id);

	CREATE NONCLUSTERED INDEX IX_daily_travel_time_threshold_pax_prev_route_id ON daily_travel_time_threshold_pax (prev_route_id);

	CREATE NONCLUSTERED INDEX IX_daily_travel_time_threshold_pax_route_id ON daily_travel_time_threshold_pax (route_id);

	CREATE NONCLUSTERED INDEX IX_daily_travel_time_threshold_pax_direction_id ON daily_travel_time_threshold_pax (direction_id);


	INSERT INTO dbo.daily_travel_time_threshold_pax
	(
		service_date
		,from_stop_id
		,to_stop_id
		,direction_id
		,prev_route_id
		,route_id
		,trip_id
		,start_time_sec
		,end_time_sec
		,travel_time_sec
		,threshold_id
		,threshold_historical_median_travel_time_sec
		,threshold_scheduled_median_travel_time_sec
		,threshold_historical_average_travel_time_sec
		,threshold_scheduled_average_travel_time_sec
		,denominator_pax
		,historical_threshold_numerator_pax
		,scheduled_threshold_numerator_pax
	)

		SELECT
			abcde.service_date AS service_date
			,abcde.abcd_stop_id AS from_stop_id
			,abcde.e_stop_id AS to_stop_id
			,abcde.abcde_direction_id AS direction_id
			,abcde.ab_route_id AS prev_route_id
			,abcde.cde_route_id AS route_id
			,abcde.cde_trip_id AS trip_id
			,abcde.d_time_sec AS start_time_sec
			,abcde.e_time_sec AS end_time_sec
			,(abcde.e_time_sec - abcde.d_time_sec) AS travel_time_sec
			,ttt.threshold_id AS threshold_id
			,ttt.threshold_historical_median_travel_time_sec AS threshold_historical_median_travel_time_sec
			,ttt.threshold_scheduled_median_travel_time_sec AS threshold_scheduled_median_travel_time_sec
			,ttt.threshold_historical_average_travel_time_sec AS threshold_historical_average_travel_time_sec
			,ttt.threshold_scheduled_average_travel_time_sec AS threshold_scheduled_average_travel_time_sec
			,(abcde.d_time_sec - abcde.b_time_sec) * ISNULL(par.passenger_arrival_rate,1/(abcde.d_time_sec - abcde.b_time_sec)) AS denominator_pax
			,CASE
				WHEN ((abcde.e_time_sec - abcde.d_time_sec) - ttt.threshold_historical_median_travel_time_sec > 0) THEN (abcde.d_time_sec - abcde.b_time_sec) * ISNULL(par.passenger_arrival_rate,1/(abcde.d_time_sec - abcde.b_time_sec))
				WHEN ((abcde.e_time_sec - abcde.d_time_sec) - ttt.threshold_historical_median_travel_time_sec <= 0) THEN 0
				ELSE 0
			END AS historical_threshold_numerator_pax
			,CASE
				WHEN ((abcde.e_time_sec - abcde.d_time_sec) - ttt.threshold_scheduled_average_travel_time_sec > 0) THEN (abcde.d_time_sec - abcde.b_time_sec) * ISNULL(par.passenger_arrival_rate,1/(abcde.d_time_sec - abcde.b_time_sec))
				WHEN ((abcde.e_time_sec - abcde.d_time_sec) - ttt.threshold_scheduled_average_travel_time_sec <= 0) THEN 0
				ELSE 0
			END AS scheduled_threshold_numerator_pax
		FROM ##daily_abcde_time abcde

		JOIN dbo.config_time_slice ts
			ON
				(
				abcde.e_time_sec >= ts.time_slice_start_sec
				AND abcde.e_time_sec < ts.time_slice_end_sec
				)
		JOIN dbo.service_date sd
			ON
				(
				abcde.service_date = sd.service_date
				)
		LEFT JOIN dbo.config_passenger_arrival_rate par --changed to LEFT JOIN to still count trip-stops with no passenger rates
			ON
				(
				par.day_type_id = sd.day_type_id
				AND ts.time_slice_id = par.time_slice_id
				AND abcde.abcd_stop_id = par.from_stop_id
				AND abcde.e_stop_id = par.to_stop_id
				)
		JOIN dbo.daily_travel_time_threshold ttt
			ON
				(
				abcde.service_date = ttt.service_date
				AND abcde.abcde_direction_id = ttt.direction_id
				AND abcde.abcd_stop_id = ttt.from_stop_id
				AND abcde.e_stop_id = ttt.to_stop_id
				AND ts.time_slice_id = ttt.time_slice_id
				AND ttt.route_type IN (0,1,3)
				--(ttt.route_type = 1
				--OR ttt.route_type = 0) --subway and green line passenger weighted numbers
				AND
				abcde.cde_route_id = ttt.route_id --added for multiple routes visiting the same stops, green line
				)

		---------------CR travel times pax start--------------------------------------------------------------------------------------------
		UNION
		SELECT
			dat.service_date AS service_date
			,dat.d_stop_id AS from_stop_id
			,dat.e_stop_id AS to_stop_id
			,dat.de_direction_id AS direction_id
			,dat.de_route_id AS prev_route_id
			,dat.de_route_id AS route_id
			,dat.de_trip_id AS trip_id
			,dat.d_time_sec AS start_time_sec
			,dat.e_time_sec AS end_time_sec
			,dat.de_time_sec AS travel_time_sec
			,dtt.threshold_id AS threshold_id
			,dtt.threshold_historical_median_travel_time_sec AS threshold_historical_median_travel_time_sec
			,dtt.threshold_scheduled_median_travel_time_sec AS threshold_scheduled_median_travel_time_sec
			,dtt.threshold_historical_average_travel_time_sec AS threshold_historical_average_travel_time_sec
			,dtt.threshold_scheduled_average_travel_time_sec AS threshold_scheduled_average_travel_time_sec
			,po.num_passenger_off_subset AS denominator_pax
			,CASE
				WHEN (dat.de_time_sec - dtt.threshold_historical_median_travel_time_sec > 0) THEN po.num_passenger_off_subset
				WHEN (dat.de_time_sec - dtt.threshold_historical_median_travel_time_sec <= 0) THEN 0
				ELSE 0
			END AS historical_threshold_numerator_pax
			,CASE
				WHEN (dat.de_time_sec - dtt.threshold_scheduled_average_travel_time_sec > 0) THEN po.num_passenger_off_subset
				WHEN (dat.de_time_sec - dtt.threshold_scheduled_average_travel_time_sec <= 0) THEN 0
				ELSE 0
			END AS scheduled_threshold_numerator_pax
		FROM ##daily_de_time dat

		JOIN dbo.config_time_slice ts
			ON
				(
				dat.e_time_sec >= ts.time_slice_start_sec
				AND dat.e_time_sec < ts.time_slice_end_sec
				)

		JOIN dbo.service_date sd
			ON
				(
				dat.service_date = sd.service_date
				)

		JOIN dbo.daily_travel_time_threshold dtt
			ON
				(
				dat.service_date = dtt.service_date
				AND dat.de_direction_id = dtt.direction_id
				AND dat.de_trip_id = dat.de_trip_id
				AND dat.d_stop_id = dtt.from_stop_id
				AND dat.e_stop_id = dtt.to_stop_id
				AND ts.time_slice_id = dtt.time_slice_id
				AND dtt.route_type = 2 --commuter rail passenger weighted numbers
				AND
				dat.de_route_id = dtt.route_id --added for multiple routes visiting the same stops
				)

		LEFT JOIN dbo.config_passenger_od_load_CR po --changed to LEFT JOIN to still count trip-stops with no passenger rates
			ON
				(
				dat.de_trip_id = po.trip_id
				AND dat.d_stop_id = po.from_stop_id
				AND dat.e_stop_id = po.to_stop_id
				)

	----------------------------------------------------------CR travel times pax end ---------------------------------------------------------

	--Create passenger weighted wait time metrics
	IF OBJECT_ID('dbo.daily_wait_time_od_threshold_pax','U') IS NOT NULL
		DROP TABLE dbo.daily_wait_time_od_threshold_pax

	CREATE TABLE dbo.daily_wait_time_od_threshold_pax
	(
		service_date								VARCHAR(255)	NOT NULL
		,from_stop_id								VARCHAR(255)	NOT NULL
		,to_stop_id									VARCHAR(255)	NOT NULL
		,direction_id								INT				NOT NULL
		,prev_route_id								VARCHAR(255)	NOT NULL
		,route_id									VARCHAR(255)	NOT NULL
		,start_time_sec								INT				NOT NULL
		,end_time_sec								INT				NOT NULL
		,max_wait_time_sec							INT				NOT NULL
		,dwell_time_sec								INT				NOT NULL
		,threshold_id								VARCHAR(255)	NOT NULL
		,threshold_historical_median_wait_time_sec	INT				NULL
		,threshold_scheduled_median_wait_time_sec	INT				NOT NULL
		,threshold_historical_average_wait_time_sec	INT				NULL
		,threshold_scheduled_average_wait_time_sec	INT				NOT NULL
		,denominator_pax							FLOAT			NULL
		,historical_threshold_numerator_pax			FLOAT			NULL
		,scheduled_threshold_numerator_pax			FLOAT			NULL
	)

	CREATE NONCLUSTERED INDEX IX_daily_wait_time_od_threshold_pax_from_stop_id ON daily_wait_time_od_threshold_pax (from_stop_id);

	CREATE NONCLUSTERED INDEX IX_daily_wait_time_od_threshold_pax_to_stop_id ON daily_wait_time_od_threshold_pax (to_stop_id);

	CREATE NONCLUSTERED INDEX IX_daily_wait_time_od_threshold_pax_prev_route_id ON daily_wait_time_od_threshold_pax (prev_route_id);

	CREATE NONCLUSTERED INDEX IX_daily_wait_time_od_threshold_pax_route_id ON daily_wait_time_od_threshold_pax (route_id);

	CREATE NONCLUSTERED INDEX IX_daily_wait_time_od_threshold_pax_direction_id ON daily_wait_time_od_threshold_pax (direction_id);


	INSERT INTO dbo.daily_wait_time_od_threshold_pax
	(
		service_date
		,from_stop_id
		,to_stop_id
		,direction_id
		,prev_route_id
		,route_id
		,start_time_sec
		,end_time_sec
		,max_wait_time_sec
		,dwell_time_sec
		,threshold_id
		,threshold_historical_median_wait_time_sec
		,threshold_scheduled_median_wait_time_sec
		,threshold_historical_average_wait_time_sec
		,threshold_scheduled_average_wait_time_sec
		,denominator_pax
		,historical_threshold_numerator_pax
		,scheduled_threshold_numerator_pax
	)

		SELECT
			abcde.service_date
			,abcde.abcd_stop_id
			,abcde.e_stop_id
			,abcde.abcde_direction_id
			,abcde.ab_route_id
			,abcde.cde_route_id
			,abcde.b_time_sec
			,abcde.d_time_sec
			,abcde.c_time_sec - abcde.b_time_sec
			,abcde.d_time_sec - abcde.c_time_sec
			,wtt.threshold_id
			,wtt.threshold_historical_median_wait_time_sec
			,wtt.threshold_scheduled_median_wait_time_sec
			,wtt.threshold_historical_average_wait_time_sec
			,wtt.threshold_scheduled_average_wait_time_sec
			,(d_time_sec - b_time_sec) * par.passenger_arrival_rate AS denominator_pax
			,CASE
				WHEN ((abcde.c_time_sec - abcde.b_time_sec) - wtt.threshold_historical_median_wait_time_sec > 0) THEN ((abcde.c_time_sec - abcde.b_time_sec) - wtt.threshold_historical_median_wait_time_sec) * par.passenger_arrival_rate
				WHEN ((abcde.c_time_sec - abcde.b_time_sec) - wtt.threshold_historical_median_wait_time_sec <= 0) THEN 0
				ELSE 0
			END AS historical_threshold_numerator_pax
			,CASE
				WHEN ((abcde.c_time_sec - abcde.b_time_sec) - wtt.threshold_scheduled_average_wait_time_sec > 0) THEN ((abcde.c_time_sec - abcde.b_time_sec) - wtt.threshold_scheduled_median_wait_time_sec) * par.passenger_arrival_rate
				WHEN ((abcde.c_time_sec - abcde.b_time_sec) - wtt.threshold_scheduled_average_wait_time_sec <= 0) THEN 0
				ELSE 0
			END AS scheduled_threshold_numerator_pax

		FROM ##daily_abcde_time abcde

		JOIN dbo.config_time_slice ts
			ON
				(
				abcde.d_time_sec >= ts.time_slice_start_sec
				AND abcde.d_time_sec < ts.time_slice_end_sec
				)

		JOIN dbo.service_date sd
			ON
				(
				abcde.service_date = sd.service_date
				)

		LEFT JOIN dbo.config_passenger_arrival_rate par --changed to LEFT JOIN to still count trips where no pax rates
			ON
				(
				par.day_type_id = sd.day_type_id -- will need to account for exceptions
				AND
				ts.time_slice_id = par.time_slice_id
				AND abcde.abcd_stop_id = par.from_stop_id
				AND abcde.e_stop_id = par.to_stop_id
				)

		JOIN dbo.daily_wait_time_od_threshold wtt
			ON
				(
				abcde.service_date = wtt.service_date
				AND abcde.abcde_direction_id = wtt.direction_id
				AND abcde.abcd_stop_id = wtt.stop_id
				AND abcde.e_stop_id = wtt.to_stop_id
				AND ts.time_slice_id = wtt.time_slice_id
				AND wtt.route_type IN (0,1,3)
				--(wtt.route_type = 1
				--OR wtt.route_type = 0) --subway and green line passenger weighted numbers only
				)
	--save headway trip metrics

	IF OBJECT_ID('dbo.daily_headway_time_threshold_trip','U') IS NOT NULL
		DROP TABLE dbo.daily_headway_time_threshold_trip
	--
	CREATE TABLE dbo.daily_headway_time_threshold_trip
	(
		service_date									VARCHAR(255)	NOT NULL
		,stop_id										VARCHAR(255)	NOT NULL
		,direction_id									INT				NOT NULL
		,prev_route_id									VARCHAR(255)	NOT NULL
		,route_id										VARCHAR(255)	NOT NULL
		,start_time_sec									INT				NOT NULL
		,end_time_sec									INT				NOT NULL
		,headway_time_sec								INT				NOT NULL
		,threshold_id									VARCHAR(255)	NOT NULL
		,threshold_scheduled_median_headway_time_sec	INT				NOT NULL
		,threshold_scheduled_average_headway_time_sec	INT				NOT NULL
		,denominator_trip								FLOAT			NOT NULL
		,scheduled_threshold_numerator_trip				FLOAT			NOT NULL
	)

	CREATE NONCLUSTERED INDEX IX_daily_headway_time_threshold_trip_stop_id ON daily_headway_time_threshold_trip (stop_id);

	CREATE NONCLUSTERED INDEX IX_daily_headway_time_threshold_trip_prev_route_id ON daily_headway_time_threshold_trip (prev_route_id);

	CREATE NONCLUSTERED INDEX IX_daily_headway_time_threshold_trip_route_id ON daily_headway_time_threshold_trip (route_id);

	CREATE NONCLUSTERED INDEX IX_daily_headway_time_threshold_trip_direction_id ON daily_headway_time_threshold_trip (direction_id);


	INSERT INTO dbo.daily_headway_time_threshold_trip
	(
		service_date
		,stop_id
		,direction_id
		,prev_route_id
		,route_id
		,start_time_sec
		,end_time_sec
		,headway_time_sec
		,threshold_id
		,threshold_scheduled_median_headway_time_sec
		,threshold_scheduled_average_headway_time_sec
		,denominator_trip
		,scheduled_threshold_numerator_trip
	)

		SELECT
			bd.service_date AS service_date
			,bd.bd_stop_id AS stop_id
			,bd.bd_direction_id AS direction_id
			,bd.b_route_id AS prev_route_id
			,bd.d_route_id AS route_id
			,bd.b_time_sec AS start_time_sec
			,bd.d_time_sec AS end_time_sec
			,bd.d_time_sec - bd.b_time_sec AS headway_time_sec
			,wtt.threshold_id AS threshold_id
			,wtt.threshold_scheduled_median_headway_time_sec AS threshold_scheduled_median_headway_time_sec
			,wtt.threshold_scheduled_average_headway_time_sec AS threshold_scheduled_average_headway_time_sec
			,1 AS denominator_trip
			,CASE
				WHEN ((bd.d_time_sec - bd.b_time_sec) > threshold_scheduled_average_headway_time_sec) THEN 1
				ELSE 0
			END AS scheduled_threshold_numerator_trip
		FROM ##daily_bd_sr_all_time bd

		JOIN dbo.config_time_slice ts
			ON
				(
				bd.d_time_sec >= ts.time_slice_start_sec
				AND bd.d_time_sec < ts.time_slice_end_sec
				)

		JOIN dbo.service_date sd
			ON
				(
				bd.service_date = sd.service_date
				)

		JOIN dbo.daily_headway_time_threshold wtt
			ON
				(
				bd.service_date = wtt.service_date
				AND bd.bd_direction_id = wtt.direction_id
				AND bd.bd_stop_id = wtt.stop_id
				AND ts.time_slice_id = wtt.time_slice_id
				AND (wtt.route_type = 1) --subway numbers only
				)
	--end headway trip metrics

	--save disaggreagate schedule adherence 
	IF OBJECT_ID('dbo.daily_schedule_adherence_disaggregate','U') IS NOT NULL
		DROP TABLE dbo.daily_schedule_adherence_disaggregate

	CREATE TABLE dbo.daily_schedule_adherence_disaggregate
	(
		service_date					VARCHAR(255)	NOT NULL
		,route_id						VARCHAR(255)	NOT NULL
		,route_type						INT				NOT NULL
		,direction_id					INT				NOT NULL
		,trip_id						VARCHAR(255)	NOT NULL
		,stop_sequence					INT				NOT NULL
		,stop_id						VARCHAR(255)	NOT NULL
		,vehicle_id						VARCHAR(255)
		,scheduled_arrival_time_sec		INT
		,actual_arrival_time_sec		INT
		,arrival_delay_sec				INT
		,scheduled_departure_time_sec	INT
		,actual_departure_time_sec		INT
		,departure_delay_sec			INT
		,stop_order_flag				INT -- 1 is first stop, 2 is mid stop, 3 is last stop
		,agency_timepoint_name			VARCHAR(255)
	)

	INSERT INTO dbo.daily_schedule_adherence_disaggregate
	(
		service_date
		,route_id
		,route_type
		,direction_id
		,trip_id
		,stop_sequence
		,stop_id
		,vehicle_id
		,scheduled_arrival_time_sec
		,actual_arrival_time_sec
		,arrival_delay_sec
		,scheduled_departure_time_sec
		,actual_departure_time_sec
		,departure_delay_sec
		,stop_order_flag
		,agency_timepoint_name
	)

		SELECT
			da.service_date
			,da.route_id
			,da.route_type
			,da.direction_id
			,da.trip_id
			,da.stop_sequence
			,da.stop_id
			,da.vehicle_id
			,da.scheduled_arrival_time_sec
			,da.actual_arrival_time_sec
			,da.arrival_delay_sec
			,dd.scheduled_departure_time_sec
			,dd.actual_departure_time_sec
			,dd.departure_delay_sec
			,da.stop_order_flag
			,da.agency_timepoint_name

		FROM #daily_arrival_time_sec da
		JOIN #daily_departure_time_sec dd
			ON
				da.service_date = dd.service_date
				AND da.route_id = dd.route_id
				AND da.trip_id = dd.trip_id
				AND da.stop_id = dd.stop_id
				AND da.vehicle_id = dd.vehicle_id

	-- INSERT arrivals at the destination stop into schedule adherence
	INSERT INTO dbo.daily_schedule_adherence_disaggregate
	(
		service_date
		,route_id
		,route_type
		,direction_id
		,trip_id
		,stop_sequence
		,stop_id
		,vehicle_id
		,scheduled_arrival_time_sec
		,actual_arrival_time_sec
		,arrival_delay_sec
		,scheduled_departure_time_sec
		,actual_departure_time_sec
		,departure_delay_sec
		,stop_order_flag
		,agency_timepoint_name
	)

		SELECT
			da.service_date
			,da.route_id
			,da.route_type
			,da.direction_id
			,da.trip_id
			,da.stop_sequence
			,da.stop_id
			,da.vehicle_id
			,da.scheduled_arrival_time_sec
			,da.actual_arrival_time_sec
			,da.arrival_delay_sec
			,NULL AS scheduled_departure_time_sec
			,NULL AS actual_departure_time_sec
			,NULL AS departure_delay_sec
			,da.stop_order_flag
			,da.agency_timepoint_name
		FROM #daily_arrival_time_sec da
		WHERE
			da.stop_order_flag = 3

	--Create table for schedule adherence weighted by passengers and trips ----

	IF OBJECT_ID('dbo.daily_schedule_adherence_threshold_pax','U') IS NOT NULL
		DROP TABLE dbo.daily_schedule_adherence_threshold_pax
		;

	CREATE TABLE dbo.daily_schedule_adherence_threshold_pax
	(
		service_date						VARCHAR(255)	NOT NULL
		,route_id							VARCHAR(255)	NOT NULL
		,route_type							INT				NOT NULL
		,direction_id						INT				NOT NULL
		,trip_id							VARCHAR(255)	NOT NULL
		,stop_sequence						INT				NOT NULL
		,stop_id							VARCHAR(255)	NOT NULL
		,vehicle_id							VARCHAR(255)
		,scheduled_arrival_time_sec			INT
		,actual_arrival_time_sec			INT
		,arrival_delay_sec					INT
		,scheduled_departure_time_sec		INT
		,actual_departure_time_sec			INT
		,departure_delay_sec				INT
		,stop_order_flag					INT -- 1 is first stop, 2 is mid stop, 3 is last stop
		,agency_timepoint_name				VARCHAR(255)
		,threshold_id						VARCHAR(255)	NOT NULL
		,threshold_id_lower					VARCHAR(255)
		,threshold_id_upper					VARCHAR(255)
		,threshold_value_lower				INT
		,threshold_value_upper				INT
		,denominator_pax					FLOAT			NULL
		,scheduled_threshold_numerator_pax	FLOAT			NULL
	)

	CREATE NONCLUSTERED INDEX IX_daily_schedule_adherence_threshold_pax_stop_id ON daily_schedule_adherence_threshold_pax (stop_id);

	CREATE NONCLUSTERED INDEX IX_daily_schedule_adherence_threshold_pax_route_id ON daily_schedule_adherence_threshold_pax (route_id);

	CREATE NONCLUSTERED INDEX IX_daily_schedule_adherence_threshold_pax_direction_id ON daily_schedule_adherence_threshold_pax (direction_id);

	INSERT INTO dbo.daily_schedule_adherence_threshold_pax
	(
		service_date
		,route_id
		,route_type
		,direction_id
		,trip_id
		,stop_sequence
		,stop_id
		,vehicle_id
		,scheduled_arrival_time_sec
		,actual_arrival_time_sec
		,arrival_delay_sec
		,scheduled_departure_time_sec
		,actual_departure_time_sec
		,departure_delay_sec
		,stop_order_flag
		,agency_timepoint_name
		,threshold_id
		,threshold_id_lower
		,threshold_id_upper
		,threshold_value_lower
		,threshold_value_upper
		,denominator_pax
		,scheduled_threshold_numerator_pax
	)

		SELECT DISTINCT
			sad.service_date
			,sad.route_id
			,sad.route_type
			,sad.direction_id
			,sad.trip_id
			,sad.stop_sequence
			,sad.stop_id
			,sad.vehicle_id
			,sad.scheduled_arrival_time_sec
			,sad.actual_arrival_time_sec
			,sad.arrival_delay_sec
			,sad.scheduled_departure_time_sec
			,sad.actual_departure_time_sec
			,sad.departure_delay_sec
			,sad.stop_order_flag
			,sad.agency_timepoint_name
			,th.threshold_id
			,th.threshold_id_lower
			,th.threshold_id_upper
			,thc1.add_to as threshold_value_lower
			,thc2.add_to as threshold_value_upper
			,par.passenger_arrival_rate as denominator_pax
			,CASE
				WHEN sad.stop_order_flag = 1 AND
					sad.departure_delay_sec NOT BETWEEN ISNULL(thc1.add_to,sad.departure_delay_sec) AND ISNULL(thc2.add_to,sad.departure_delay_sec) THEN 1 --par.passenger_arrival_rate
				WHEN sad.stop_order_flag = 2 AND
					sad.departure_delay_sec NOT BETWEEN ISNULL(thc1.add_to,sad.departure_delay_sec) AND ISNULL(thc2.add_to,sad.departure_delay_sec) THEN 1 -- par.passenger_arrival_rate
				WHEN sad.stop_order_flag = 3 AND
					sad.arrival_delay_sec NOT BETWEEN ISNULL(thc1.add_to,sad.arrival_delay_sec) AND ISNULL(thc2.add_to,sad.arrival_delay_sec) THEN 1 --par.passenger_arrival_rate
				WHEN sad.stop_order_flag = 1 AND 
					sad.departure_delay_sec BETWEEN ISNULL(thc1.add_to,sad.departure_delay_sec) AND ISNULL(thc2.add_to,sad.departure_delay_sec) THEN 0
				WHEN sad.stop_order_flag = 2 AND
					sad.departure_delay_sec BETWEEN ISNULL(thc1.add_to,sad.departure_delay_sec) AND ISNULL(thc2.add_to,sad.departure_delay_sec) THEN 0
				WHEN sad.stop_order_flag = 3 AND
					sad.arrival_delay_sec BETWEEN ISNULL(thc1.add_to,sad.arrival_delay_sec) AND ISNULL(thc2.add_to,sad.arrival_delay_sec) THEN 0
			END as scheduled_threshold_numerator_pax
		
		FROM daily_schedule_adherence_disaggregate sad
		JOIN dbo.service_date sd
			ON
					sad.service_date = sd.service_date
		CROSS JOIN
			(
				SELECT
					ct.threshold_id
					,ct.threshold_name
					,ct.threshold_type
					,ct1.threshold_id as threshold_id_lower
					,ct2.threshold_id as threshold_id_upper
				FROM config_threshold ct
				LEFT JOIN config_threshold ct1
					ON 
						ct.threshold_id = 
							CASE 
								when ct1.parent_child = 0 then ct1.threshold_id
								when ct1.parent_child = 2 then ct1.parent_threshold_id
							END
					AND 
						ct1.upper_lower = 'lower'
				LEFT JOIN config_threshold ct2
					ON 
						ct.threshold_id = 
							CASE 
								when ct2.parent_child = 0 then ct2.threshold_id
								when ct2.parent_child = 2 then ct2.parent_threshold_id
							END
					AND 
						ct2.upper_lower = 'upper'
				WHERE 
						ct.parent_child <> 2
					
		) th
		JOIN config_stop_order_flag_threshold sth
			ON
					sad.stop_order_flag = sth.stop_order_flag
				AND
					th.threshold_id = sth.threshold_id
	
		LEFT JOIN config_threshold_calculation thc1
			ON
					th.threshold_id_lower = thc1.threshold_id
	
		LEFT JOIN config_threshold_calculation thc2
			ON
					th.threshold_id_upper = thc2.threshold_id	
	
		JOIN config_mode_threshold mt
			ON
					mt.threshold_id = th.threshold_id
				AND
					mt.route_type = sad.route_type
			
		LEFT JOIN config_passenger_arrival_rate par
			ON
					par.day_type_id = sd.day_type_id
				AND 
					sad.stop_id = par.from_stop_id
		WHERE
				sad.route_type = 3 --bus only
			AND
				th.threshold_type = 'wait_time_schedule_based'

		UNION

		SELECT DISTINCT
			sad.service_date
			,sad.route_id
			,sad.route_type
			,sad.direction_id
			,sad.trip_id
			,sad.stop_sequence
			,sad.stop_id
			,vehicle_id
			,scheduled_arrival_time_sec
			,actual_arrival_time_sec
			,arrival_delay_sec
			,scheduled_departure_time_sec
			,actual_departure_time_sec
			,departure_delay_sec
			,stop_order_flag
			,agency_timepoint_name
			,th.threshold_id
			,th.threshold_id_lower
			,th.threshold_id_upper
			,thc1.add_to as threshold_value_lower
			,thc2.add_to as threshold_value_upper
			,po.from_stop_passenger_on AS denominator_pax
			,CASE
				WHEN sad.stop_order_flag = 1 AND
					sad.departure_delay_sec NOT BETWEEN ISNULL(thc1.add_to,sad.departure_delay_sec) AND ISNULL(thc2.add_to,sad.departure_delay_sec) THEN po.from_stop_passenger_on
				WHEN sad.stop_order_flag = 2 AND
					sad.arrival_delay_sec NOT BETWEEN ISNULL(thc1.add_to,sad.arrival_delay_sec) AND ISNULL(thc2.add_to, sad.arrival_delay_sec) THEN po.from_stop_passenger_on
				WHEN sad.stop_order_flag = 3 AND
					sad.arrival_delay_sec NOT BETWEEN ISNULL(thc1.add_to,sad.arrival_delay_sec) AND ISNULL(thc2.add_to, sad.arrival_delay_sec) THEN po.from_stop_passenger_on
				WHEN sad.stop_order_flag = 1 AND
					sad.departure_delay_sec BETWEEN ISNULL(thc1.add_to,sad.departure_delay_sec) AND ISNULL(thc2.add_to,sad.departure_delay_sec) THEN 0
				WHEN sad.stop_order_flag = 2 AND
					sad.arrival_delay_sec BETWEEN ISNULL(thc1.add_to,sad.arrival_delay_sec) AND ISNULL(thc2.add_to, sad.arrival_delay_sec) THEN 0
				WHEN sad.stop_order_flag = 3 AND
					sad.arrival_delay_sec BETWEEN ISNULL(thc1.add_to,sad.arrival_delay_sec) AND ISNULL(thc2.add_to, sad.arrival_delay_sec) THEN 0
				ELSE 0
			END AS scheduled_threshold_numerator_pax

		FROM daily_schedule_adherence_disaggregate sad
		LEFT JOIN dbo.config_passenger_od_load_CR po --left join to include trip-stops where there are no pax rates
			ON
				sad.route_id = po.route_id
				AND sad.trip_id = po.trip_id
				AND sad.stop_id = po.from_stop_id

		CROSS JOIN
			(
				SELECT
					ct.threshold_id
					,ct.threshold_name
					,ct.threshold_type
					,ct1.threshold_id as threshold_id_lower
					,ct2.threshold_id as threshold_id_upper
				FROM config_threshold ct
				LEFT JOIN config_threshold ct1
					ON 
						ct.threshold_id = 
							CASE 
								when ct1.parent_child = 0 then ct1.threshold_id
								when ct1.parent_child = 2 then ct1.parent_threshold_id
							END
					AND 
						ct1.upper_lower = 'lower'
				LEFT JOIN config_threshold ct2
					ON 
						ct.threshold_id = 
							CASE 
								when ct2.parent_child = 0 then ct2.threshold_id
								when ct2.parent_child = 2 then ct2.parent_threshold_id
							END
					AND 
						ct2.upper_lower = 'upper'
				WHERE 
						ct.parent_child <> 2
					
		) th

		LEFT JOIN config_threshold_calculation thc1
			ON
					th.threshold_id_lower = thc1.threshold_id
	
		LEFT JOIN config_threshold_calculation thc2
			ON
					th.threshold_id_upper = thc2.threshold_id	
	
		JOIN config_mode_threshold mt
			ON
					mt.threshold_id = th.threshold_id
				AND
					mt.route_type = sad.route_type
		WHERE
				sad.route_type = 2 --commuter rail only
			AND
				th.threshold_type = 'wait_time_schedule_based'


	--Create table for headway adherence weighted by passengers and trips ----
	-- add pax numbers later ---
	IF OBJECT_ID('dbo.daily_headway_adherence_threshold_pax','U') IS NOT NULL
		DROP TABLE dbo.daily_headway_adherence_threshold_pax

	CREATE TABLE dbo.daily_headway_adherence_threshold_pax
	(
		service_date									VARCHAR(255)	NOT NULL
		,stop_id										VARCHAR(255)	NOT NULL
		,stop_order_flag								INT				NOT NULL
		,agency_timepoint_name							VARCHAR(255)
		,direction_id									INT				NOT NULL
		,route_id										VARCHAR(255)	NOT NULL
		,start_time_sec									INT				NOT NULL
		,end_time_sec									INT				NOT NULL
		,headway_time_sec								INT				NOT NULL
		,threshold_id									VARCHAR(255)	NOT NULL
		,threshold_scheduled_median_headway_time_sec	INT				NOT NULL
		,threshold_scheduled_average_headway_time_sec	INT				NOT NULL
		,denominator_pax								FLOAT			NOT NULL
		,scheduled_threshold_numerator_pax				FLOAT			NOT NULL
	)

	CREATE NONCLUSTERED INDEX IX_daily_headway_adherence_threshold_pax_stop_id ON daily_headway_adherence_threshold_pax (stop_id);

	CREATE NONCLUSTERED INDEX IX_daily_headway_adherence_threshold_pax_route_id ON daily_headway_adherence_threshold_pax (route_id);

	CREATE NONCLUSTERED INDEX IX_daily_headway_adherence_threshold_pax_direction_id ON daily_headway_adherence_threshold_pax (direction_id);

	INSERT INTO daily_headway_adherence_threshold_pax
	(
		service_date
		,stop_id
		,stop_order_flag
		,agency_timepoint_name
		,direction_id
		,route_id
		,start_time_sec
		,end_time_sec
		,headway_time_sec
		,threshold_id
		,threshold_scheduled_median_headway_time_sec
		,threshold_scheduled_average_headway_time_sec
		,denominator_pax
		,scheduled_threshold_numerator_pax
	)

		SELECT
			bd.service_date AS service_date
			,bd.bd_stop_id AS stop_id
			,st.stop_order_flag
			,st.agency_timepoint_name
			,bd.bd_direction_id AS direction_id
			,bd.bd_route_id AS route_id
			,bd.b_time_sec AS start_time_sec
			,bd.d_time_sec AS end_time_sec
			,bd.d_time_sec - bd.b_time_sec AS headway_time_sec
			,wtt.threshold_id AS threshold_id
			,wtt.threshold_scheduled_median_wait_time_sec AS threshold_scheduled_median_headway_time_sec
			,wtt.threshold_scheduled_average_wait_time_sec AS threshold_scheduled_average_headway_time_sec
			,1 AS denominator_pax
			,CASE
				WHEN ((bd.d_time_sec - bd.b_time_sec) > threshold_scheduled_average_wait_time_sec) THEN 1
				ELSE 0
			END AS scheduled_threshold_numerator_pax
		FROM ##daily_bd_sr_same_time bd

		JOIN dbo.config_time_slice ts
			ON
				(
				bd.d_time_sec >= ts.time_slice_start_sec
				AND bd.d_time_sec < ts.time_slice_end_sec
				)

		JOIN dbo.service_date sd
			ON
				(
				bd.service_date = sd.service_date
				)

		JOIN dbo.daily_headway_time_sr_same_threshold wtt
			ON
				(
				bd.service_date = wtt.service_date
				AND bd.bd_route_id = wtt.route_id
				AND bd.bd_direction_id = wtt.direction_id
				AND bd.bd_stop_id = wtt.stop_id
				AND ts.time_slice_id = wtt.time_slice_id
				AND (wtt.route_type = 3) --bus numbers only
				)
		JOIN dbo.daily_stop_times_sec st
			ON
				(
				bd.service_date = st.service_date
				AND bd.b_trip_id = st.trip_id
				AND bd.bd_stop_id = st.stop_id
				)

				SELECT * FROM daily_headway_adherence_threshold_pax
				WHERE stop_order_flag <> 3



	--save daily metrics for each route	
	IF OBJECT_ID('dbo.daily_metrics','U') IS NOT NULL
		DROP TABLE dbo.daily_metrics
	--
	CREATE TABLE dbo.daily_metrics
	(
		route_id			VARCHAR(255)	NOT NULL
		,threshold_id		VARCHAR(255)	NOT NULL
		,threshold_name		VARCHAR(255)	NOT NULL
		,threshold_type		VARCHAR(255)	NOT NULL
		,metric_result		FLOAT
		,metric_result_trip	FLOAT			NULL
		,numerator_pax		FLOAT			NULL
		,denominator_pax	FLOAT			NULL
		,numerator_trip		FLOAT			NULL
		,denominator_trip	FLOAT			NULL

	)

	DECLARE @from_stop_ids TABLE
	(
		stop_id VARCHAR(255)
	)

	DECLARE @to_stop_ids TABLE
	(
		stop_id VARCHAR(255)
	)

	DECLARE @direction_ids TABLE
	(
		direction_id INT
	)

	DECLARE @route_ids TABLE
	(
		route_id VARCHAR(255)
	)

	INSERT INTO @route_ids
		VALUES ('Red'),('Blue'),('Orange')
		,('Green-B'),('Green-C'),('Green-D'),('Green-E')
		,('CR-Fairmount'),('CR-Fitchburg'),('CR-Franklin'),('CR-Greenbush'),('CR-Haverhill'),('CR-Kingston'),('CR-Lowell'),('CR-Middleborough')
		,('CR-Needham'),('CR-Newburyport'),('CR-Providence'),('CR-Worcester'),('749'),('69'),('68'),('325'),('9'),('1'),('7'),('751')

	DECLARE @use_timepoints_only BIT

	SET @use_timepoints_only = 1 --TRUE = 1, FALSE = 0

	/*
	INSERT INTO dbo.daily_metrics
	(
		route_id
		,threshold_id
		,threshold_name
		,threshold_type
		,metric_result
		,metric_result_trip
		,numerator_pax
		,denominator_pax
		,numerator_trip
		,denominator_trip
	)

		SELECT
			route_id
			,ct.threshold_id
			,ct.threshold_name
			,ct.threshold_type
			,1 - SUM(scheduled_threshold_numerator_pax) / SUM(denominator_pax) AS metric_result
			,NULL
			,SUM(scheduled_threshold_numerator_pax) AS numerator_pax
			,SUM(denominator_pax) AS denominator_pax
			,NULL
			,NULL

		FROM	dbo.daily_wait_time_od_threshold_pax dwt
				,dbo.config_threshold ct
		WHERE
			ct.threshold_id = dwt.threshold_id
			AND 
				(
					(SELECT COUNT(stop_id) FROM @from_stop_ids) = 0
				OR 
					from_stop_id IN (SELECT stop_id FROM @from_stop_ids)
				)
			AND 
				(
					(SELECT COUNT(stop_id) FROM @to_stop_ids)= 0
				OR 
					to_stop_id IN (SELECT stop_id FROM @to_stop_ids)
				)
			AND 
				(
					(SELECT COUNT(direction_id) FROM @direction_ids)= 0
			OR 
					direction_id IN (SELECT direction_id FROM @direction_ids )
				)
			AND 
				(
					(SELECT COUNT(route_id) FROM @route_ids) = 0
				OR 
					prev_route_id IN (SELECT route_id FROM @route_ids )
				)
			AND 
				(
					(SELECT COUNT(route_id) FROM @route_ids ) = 0
				OR 
					route_id IN (SELECT route_id FROM @route_ids )
				)

		GROUP BY
			route_id
			,ct.threshold_id
			,ct.threshold_name
			,ct.threshold_type
			,ct.threshold_id

		UNION

		SELECT
			route_id
			,ct.threshold_id
			,ct.threshold_name
			,ct.threshold_type
			,1 - SUM(scheduled_threshold_numerator_pax) / SUM(denominator_pax) AS metric_result
			,NULL
			,SUM(scheduled_threshold_numerator_pax) AS numerator_pax
			,SUM(denominator_pax) AS denominator_pax
			,NULL
			,NULL
		FROM	dbo.daily_travel_time_threshold_pax dtt
				,dbo.config_threshold ct
		WHERE
			ct.threshold_id = dtt.threshold_id
			AND (
			(
				SELECT
					COUNT(stop_id)
				FROM @from_stop_ids
			)
			= 0
			OR from_stop_id IN
			(
				SELECT
					stop_id
				FROM @from_stop_ids
			)
			)
			AND (
			(
				SELECT
					COUNT(stop_id)
				FROM @to_stop_ids
			)
			= 0
			OR to_stop_id IN
			(
				SELECT
					stop_id
				FROM @to_stop_ids
			)
			)
			AND (
			(
				SELECT
					COUNT(direction_id)
				FROM @direction_ids
			)
			= 0
			OR direction_id IN
			(
				SELECT
					direction_id
				FROM @direction_ids
			)
			)
			AND (
			(
				SELECT
					COUNT(route_id)
				FROM @route_ids
			)
			= 0
			OR route_id IN
			(
				SELECT
					route_id
				FROM @route_ids
			)
			)

		GROUP BY
			route_id
			,ct.threshold_id
			,ct.threshold_name
			,ct.threshold_type
			,ct.threshold_id
		UNION
		
		SELECT
			route_id
			,ct.threshold_id
			,ct.threshold_name
			,ct.threshold_type
			,1-SUM(scheduled_threshold_numerator_pax)/SUM(denominator_pax) AS metric_result 
			,NULL--,1-SUM(scheduled_threshold_numerator_trip)/SUM(denominator_trip) AS metric_result_trip 
			,SUM(scheduled_threshold_numerator_pax) AS numerator_pax  --added
			,SUM(denominator_pax) AS denominator_pax --added
			,NULL--,SUM(scheduled_threshold_numerator_trip) AS numerator_trip --added
			,NULL--,SUM(denominator_trip) AS denominator_trip --added
		FROM
			dbo.daily_schedule_adherence_threshold_pax cap
			,dbo.config_threshold ct
		WHERE
				ct.threshold_id = cap.threshold_id
			AND
				(
					(SELECT COUNT(stop_id) FROM @from_stop_ids) = 0
				OR
					stop_id IN (SELECT stop_id FROM @from_stop_ids)
				)	
			AND
				(
					(SELECT COUNT(direction_id) FROM @direction_ids) = 0
				OR
					direction_id IN (SELECT direction_id FROM @direction_ids)
				)
			AND
				(
					(SELECT COUNT(route_id) FROM @route_ids) = 0
				OR
					route_id IN (SELECT route_id FROM @route_ids)
				)
			AND
				route_type = 2
		GROUP BY
				route_id
				,ct.threshold_id
				,ct.threshold_name
				,ct.threshold_type
				,ct.threshold_id
		UNION
		
		SELECT
			route_id
			,'threshold_id_14' + ', '+ 'threshold_id_17' + ', ' + 'threshold_id_20' as threshold_id --need to change this
			,'reliability_scheduled' --need to change this
			,ct.threshold_type
			,1 - SUM(scheduled_threshold_numerator_pax) / COUNT(*) AS metric_result--SUM(denominator_pax) AS metric_result
			,NULL
			,SUM(scheduled_threshold_numerator_pax) AS numerator_pax
			,SUM(denominator_pax) AS denominator_pax
			,NULL
			,NULL
		FROM	dbo.daily_schedule_adherence_threshold_pax cap
				,dbo.config_threshold ct
		WHERE
			ct.threshold_id = cap.threshold_id
			AND 
				(
					(SELECT COUNT(stop_id) FROM @from_stop_ids) = 0
				OR 
					stop_id IN (SELECT stop_id FROM @from_stop_ids)
				)
			AND 
				(
					(SELECT COUNT(direction_id) FROM @direction_ids) = 0
				OR 
					direction_id IN (SELECT direction_id FROM @direction_ids)
				)
			AND 
				(
					(SELECT COUNT(route_id) FROM @route_ids) = 0
				OR 
					route_id IN (SELECT route_id FROM @route_ids)
				)
			AND
				route_type = 3
			AND
				(
					(@use_timepoints_only = 1 AND agency_timepoint_name IS NOT NULL)
				OR
					@use_timepoints_only = 0 
				)
		GROUP BY
			route_id
			--,ct.threshold_id
			--,ct.threshold_name
			,ct.threshold_type
		UNION

		SELECT
			route_id
			,ct.threshold_id
			,ct.threshold_name
			,ct.threshold_type
			,NULL
			,1 - SUM(scheduled_threshold_numerator_trip) / SUM(denominator_trip) AS metric_result_trip
			,NULL
			,NULL
			,SUM(scheduled_threshold_numerator_trip) AS numerator_trip
			,SUM(denominator_trip) AS denominator_trip
		FROM	dbo.daily_headway_time_threshold_trip dtt
				,dbo.config_threshold ct
		WHERE
			ct.threshold_id = dtt.threshold_id
			AND (
			(
				SELECT
					COUNT(stop_id)
				FROM @from_stop_ids
			)
			= 0
			OR stop_id IN
			(
				SELECT
					stop_id
				FROM @from_stop_ids
			)
			)
			AND (
			(
				SELECT
					COUNT(direction_id)
				FROM @direction_ids
			)
			= 0
			OR direction_id IN
			(
				SELECT
					direction_id
				FROM @direction_ids
			)
			)
			AND (
			(
				SELECT
					COUNT(route_id)
				FROM @route_ids
			)
			= 0
			OR route_id IN
			(
				SELECT
					route_id
				FROM @route_ids
			)
			)

		GROUP BY
			route_id
			,ct.threshold_id
			,ct.threshold_name
			,ct.threshold_type
			,ct.threshold_id
		--end of adding in trip metrics

		UNION

		*/

		SELECT
			route_id
			,'threshold_id_21' + ', '+ 'threshold_id_22'  as threshold_id --need to change this
			,'reliability_frequent' --need to change this
			--,ct.threshold_type
			,1 - SUM(scheduled_threshold_numerator_pax) / COUNT(*) AS metric_result--SUM(denominator_pax) AS metric_result
			,NULL
			,SUM(scheduled_threshold_numerator_pax) AS numerator_pax
			,SUM(denominator_pax) AS denominator_pax
			,NULL
			,NULL
		FROM	
			(
				SELECT DISTINCT 
					dtt.service_date
					,dtt.to_stop_id AS stop_id
					,st.stop_order_flag
					,st.agency_timepoint_name
					,dtt.direction_id
					,dtt.route_id
					,dtt.start_time_sec
					,dtt.end_time_sec
					,dtt.travel_time_sec
					,dtt.threshold_id
					,dtt.threshold_scheduled_median_travel_time_sec
					,dtt.threshold_scheduled_average_travel_time_sec
					,dtt.denominator_pax
					,dtt.scheduled_threshold_numerator_pax
			  FROM dbo.daily_travel_time_threshold_pax dtt
			  JOIN dbo.daily_stop_times_sec st
			  ON
						dtt.trip_id = st.trip_id
				AND
						dtt.from_stop_id = st.trip_first_stop_id
				AND
						dtt.to_stop_id = st.trip_last_stop_id
			JOIN dbo.config_stop_order_flag_threshold sth
			ON
					st.stop_order_flag = sth.stop_order_flag
				AND
					dtt.threshold_id = sth.threshold_id

			UNION ALL
			SELECT *
			FROM dbo.daily_headway_adherence_threshold_pax
			) cap
				,dbo.config_threshold ct
		WHERE
			ct.threshold_id = cap.threshold_id
			AND 
				(
					(SELECT COUNT(stop_id) FROM @from_stop_ids) = 0
				OR 
					stop_id IN (SELECT stop_id FROM @from_stop_ids)
				)
			AND 
				(
					(SELECT COUNT(direction_id) FROM @direction_ids) = 0
				OR 
					direction_id IN (SELECT direction_id FROM @direction_ids)
				)
			AND 
				(
					(SELECT COUNT(route_id) FROM @route_ids) = 0
				OR 
					route_id IN (SELECT route_id FROM @route_ids)
				)
			--AND
			--	route_type = 3
			AND
				(
					(@use_timepoints_only = 1 AND agency_timepoint_name IS NOT NULL)
				OR
					@use_timepoints_only = 0 
				)
		GROUP BY
			route_id
			--,ct.threshold_id
			--,ct.threshold_name
			--,ct.threshold_type

		ORDER BY
			route_id,threshold_id

	-- Save daily disaggregate travel times 

	IF OBJECT_ID('dbo.daily_travel_time_disaggregate','U') IS NOT NULL
		DROP TABLE dbo.daily_travel_time_disaggregate
		;

	CREATE TABLE dbo.daily_travel_time_disaggregate
	(
		service_date				VARCHAR(255)	NOT NULL
		,from_stop_id				VARCHAR(255)	NOT NULL
		,to_stop_id					VARCHAR(255)	NOT NULL
		,route_id					VARCHAR(255)	NOT NULL
		,route_type					INT				NOT NULL
		,direction_id				INT				NOT NULL
		,start_time_sec				INT				NOT NULL
		,end_time_sec				INT				NOT NULL
		,travel_time_sec			INT				NOT NULL
		,benchmark_travel_time_sec	INT
	)

	INSERT INTO dbo.daily_travel_time_disaggregate
	(
		service_date
		,from_stop_id
		,to_stop_id
		,route_id
		,route_type
		,direction_id
		,start_time_sec
		,end_time_sec
		,travel_time_sec
		,benchmark_travel_time_sec
	)

		SELECT
			htt.service_date
			,htt.d_stop_id
			,htt.e_stop_id
			,htt.de_route_id
			,htt.de_route_type
			,htt.de_direction_id
			,htt.d_time_sec
			,htt.e_time_sec
			,htt.de_time_sec
			,dtb.scheduled_average_travel_time_sec
		FROM ##daily_de_time htt

		JOIN dbo.config_time_slice ts
			ON
				(
				htt.e_time_sec >= ts.time_slice_start_sec
				AND htt.e_time_sec < ts.time_slice_end_sec
				)
		JOIN dbo.daily_travel_time_benchmark dtb
			ON
				(
				htt.de_direction_id = dtb.direction_id
				AND htt.d_stop_id = dtb.from_stop_id
				AND htt.e_stop_id = dtb.to_stop_id
				AND htt.de_route_id = dtb.route_id --added because of green line
				AND
				ts.time_slice_id = dtb.time_slice_id
				)

	--Save daily disaggregate headway times between the same origin-destination stops 

	IF OBJECT_ID('dbo.daily_headway_time_od_disaggregate','u') IS NOT NULL
		DROP TABLE dbo.daily_headway_time_od_disaggregate
		;

	CREATE TABLE dbo.daily_headway_time_od_disaggregate
	(
		service_date				VARCHAR(255)	NOT NULL
		,stop_id					VARCHAR(255)	NOT NULL
		,to_stop_id					VARCHAR(255)	NOT NULL
		,route_id					VARCHAR(255)	NOT NULL
		,prev_route_id				VARCHAR(255)	NOT NULL
		,route_type					INT				NOT NULL
		,direction_id				INT				NOT NULL
		,start_time_sec				INT				NOT NULL
		,end_time_sec				INT				NOT NULL
		,headway_time_sec			INT				NOT NULL
		,benchmark_headway_time_sec	INT
	)
	;

	INSERT INTO dbo.daily_headway_time_od_disaggregate
	(
		service_date
		,stop_id
		,to_stop_id
		,route_id
		,prev_route_id
		,route_type
		,direction_id
		,start_time_sec
		,end_time_sec
		,headway_time_sec
		,benchmark_headway_time_sec
	)

		SELECT
			htt.service_date
			,htt.abcd_stop_id
			,htt.e_stop_id
			,htt.cde_route_id
			,htt.ab_route_id
			,htt.abcde_route_type
			,htt.abcde_direction_id
			,htt.b_time_sec
			,htt.d_time_sec
			,htt.bd_time_sec
			,dtb.scheduled_average_headway_sec
		FROM ##daily_abcde_time htt

		JOIN dbo.config_time_slice ts
			ON
				(
				htt.d_time_sec >= ts.time_slice_start_sec
				AND htt.d_time_sec < ts.time_slice_end_sec
				)
		JOIN dbo.daily_headway_time_od_benchmark dtb
			ON
				(
				htt.abcde_direction_id = dtb.direction_id
				AND htt.abcd_stop_id = dtb.stop_id
				AND htt.e_stop_id = dtb.to_stop_id
				AND ts.time_slice_id = dtb.time_slice_id
				)


	--Save daily disaggregate headway times at a stop for trips of all routes

	IF OBJECT_ID('dbo.daily_headway_time_sr_all_disaggregate','u') IS NOT NULL
		DROP TABLE dbo.daily_headway_time_sr_all_disaggregate
		;

	CREATE TABLE dbo.daily_headway_time_sr_all_disaggregate
	(
		service_date				VARCHAR(255)	NOT NULL
		,stop_id					VARCHAR(255)	NOT NULL
		,route_id					VARCHAR(255)	NOT NULL
		,prev_route_id				VARCHAR(255)	NOT NULL
		,route_type					INT				NOT NULL
		,direction_id				INT				NOT NULL
		,start_time_sec				INT				NOT NULL
		,end_time_sec				INT				NOT NULL
		,headway_time_sec			INT				NOT NULL
		,benchmark_headway_time_sec	INT
	)
	;

	INSERT INTO dbo.daily_headway_time_sr_all_disaggregate
	(
		service_date
		,stop_id
		,route_id
		,prev_route_id
		,route_type
		,direction_id
		,start_time_sec
		,end_time_sec
		,headway_time_sec
		,benchmark_headway_time_sec
	)

		SELECT
			htt.service_date
			,htt.bd_stop_id
			,htt.d_route_id
			,htt.b_route_id
			,htt.bd_route_type
			,htt.bd_direction_id
			,htt.b_time_sec
			,htt.d_time_sec
			,htt.bd_time_sec
			,dtb.scheduled_average_headway_sec
		FROM ##daily_bd_sr_all_time htt

		JOIN dbo.config_time_slice ts
			ON
				(
				htt.d_time_sec >= ts.time_slice_start_sec
				AND htt.d_time_sec < ts.time_slice_end_sec
				)
		JOIN dbo.daily_headway_time_sr_all_benchmark dtb
			ON
				(
				htt.bd_direction_id = dtb.direction_id
				AND htt.bd_stop_id = dtb.stop_id
				AND ts.time_slice_id = dtb.time_slice_id
				)

	--Save daily disaggregate headway times at a stop for trips of the same route
	IF OBJECT_ID('dbo.daily_headway_time_sr_same_disaggregate','u') IS NOT NULL
		DROP TABLE dbo.daily_headway_time_sr_same_disaggregate
		;

	CREATE TABLE dbo.daily_headway_time_sr_same_disaggregate
	(
		service_date				VARCHAR(255)	NOT NULL
		,stop_id					VARCHAR(255)	NOT NULL
		,route_id					VARCHAR(255)	NOT NULL
		,prev_route_id				VARCHAR(255)	NOT NULL
		,route_type					INT
		,direction_id				INT				NOT NULL
		,start_time_sec				INT				NOT NULL
		,end_time_sec				INT				NOT NULL
		,headway_time_sec			INT				NOT NULL
		,benchmark_headway_time_sec	INT
	)
	;

	INSERT INTO dbo.daily_headway_time_sr_same_disaggregate
	(
		service_date
		,stop_id
		,route_id
		,prev_route_id
		,route_type
		,direction_id
		,start_time_sec
		,end_time_sec
		,headway_time_sec
		,benchmark_headway_time_sec
	)
		SELECT
			htt.service_date
			,htt.bd_stop_id
			,htt.bd_route_id
			,htt.bd_route_id
			,htt.bd_route_type
			,htt.bd_direction_id
			,htt.b_time_sec
			,htt.d_time_sec
			,htt.bd_time_sec
			,dtb.scheduled_average_headway_sec
		FROM ##daily_bd_sr_same_time htt

		JOIN dbo.config_time_slice ts
			ON
				(
				htt.d_time_sec >= ts.time_slice_start_sec
				AND htt.d_time_sec < ts.time_slice_end_sec
				)
		JOIN dbo.daily_headway_time_sr_same_benchmark dtb
			ON
				(
				htt.bd_route_id = dtb.route_id
				AND htt.bd_direction_id = dtb.direction_id
				AND htt.bd_stop_id = dtb.stop_id
				AND ts.time_slice_id = dtb.time_slice_id
				)


	-- Save disaggregate dwell times
	IF OBJECT_ID('dbo.daily_dwell_time_disaggregate','u') IS NOT NULL
		DROP TABLE dbo.daily_dwell_time_disaggregate
		;

	CREATE TABLE dbo.daily_dwell_time_disaggregate
	(
		service_date	VARCHAR(255)	NOT NULL
		,stop_id		VARCHAR(255)	NOT NULL
		,route_id		VARCHAR(255)	NOT NULL
		,direction_id	INT				NOT NULL
		,start_time_sec	INT				NOT NULL
		,end_time_sec	INT				NOT NULL
		,dwell_time_sec	INT				NOT NULL
	)
	;

	INSERT INTO dbo.daily_dwell_time_disaggregate
	(
		service_date
		,stop_id
		,route_id
		,direction_id
		,start_time_sec
		,end_time_sec
		,dwell_time_sec
	)
		SELECT
			service_date
			,cd_stop_id
			,cd_route_id
			,cd_direction_id
			,c_time_sec
			,d_time_sec
			,cd_time_sec
		FROM ##daily_cd_time

	--WRITE TO historical tables------------------------------------------------------

	IF
		(
			SELECT
				COUNT(*)
			FROM dbo.historical_event
			WHERE
				service_date = @service_date_process
		)
		> 0

		DELETE FROM dbo.historical_event
		WHERE
			service_date = @service_date_process

	INSERT INTO dbo.historical_event
	(
		record_id
		,service_date
		,file_time
		,route_id
		,route_type
		,trip_id
		,direction_id
		,stop_id
		,stop_sequence
		,vehicle_id
		,event_type
		,event_time
		,event_time_sec
		,event_processed_rt
		,event_processed_daily
		,suspect_record
	)

		SELECT
			record_id
			,service_date
			,file_time
			,route_id
			,route_type
			,trip_id
			,direction_id
			,stop_id
			,stop_sequence
			,vehicle_id
			,event_type
			,event_time
			,event_time_sec
			,event_processed_rt
			,event_processed_daily
			,suspect_record
		FROM dbo.daily_event

	IF
		(
			SELECT
				COUNT(*)
			FROM dbo.historical_travel_time_disaggregate
			WHERE
				service_date = @service_date_process
		)
		> 0

		DELETE FROM dbo.historical_travel_time_disaggregate
		WHERE
			service_date = @service_date_process

	INSERT INTO dbo.historical_travel_time_disaggregate
	(
		service_date
		,from_stop_id
		,to_stop_id
		,route_id
		,route_type
		,direction_id
		,start_time_sec
		,end_time_sec
		,travel_time_sec
		,benchmark_travel_time_sec
	)
		SELECT
			service_date
			,from_stop_id
			,to_stop_id
			,route_id
			,route_type
			,direction_id
			,start_time_sec
			,end_time_sec
			,travel_time_sec
			,benchmark_travel_time_sec
		FROM dbo.daily_travel_time_disaggregate

	IF
		(
			SELECT
				COUNT(*)
			FROM dbo.historical_headway_time_od_disaggregate
			WHERE
				service_date = @service_date_process
		)
		> 0

		DELETE FROM dbo.historical_headway_time_od_disaggregate
		WHERE
			service_date = @service_date_process

	INSERT INTO dbo.historical_headway_time_od_disaggregate
	(
		service_date
		,stop_id
		,to_stop_id
		,route_id
		,prev_route_id
		,route_type
		,direction_id
		,start_time_sec
		,end_time_sec
		,headway_time_sec
		,benchmark_headway_time_sec
	)

		SELECT
			service_date
			,stop_id
			,to_stop_id
			,route_id
			,prev_route_id
			,route_type
			,direction_id
			,start_time_sec
			,end_time_sec
			,headway_time_sec
			,benchmark_headway_time_sec
		FROM dbo.daily_headway_time_od_disaggregate

	IF
		(
			SELECT
				COUNT(*)
			FROM dbo.historical_headway_time_sr_all_disaggregate
			WHERE
				service_date = @service_date_process
		)
		> 0

		DELETE FROM dbo.historical_headway_time_sr_all_disaggregate
		WHERE
			service_date = @service_date_process

	INSERT INTO dbo.historical_headway_time_sr_all_disaggregate
	(
		service_date
		,stop_id
		,route_id
		,prev_route_id
		,route_type
		,direction_id
		,start_time_sec
		,end_time_sec
		,headway_time_sec
		,benchmark_headway_time_sec
	)
		SELECT
			service_date
			,stop_id
			,route_id
			,prev_route_id
			,route_type
			,direction_id
			,start_time_sec
			,end_time_sec
			,headway_time_sec
			,benchmark_headway_time_sec
		FROM dbo.daily_headway_time_sr_all_disaggregate

	IF
		(
			SELECT
				COUNT(*)
			FROM dbo.historical_headway_time_sr_same_disaggregate
			WHERE
				service_date = @service_date_process
		)
		> 0

		DELETE FROM dbo.historical_headway_time_sr_same_disaggregate
		WHERE
			service_date = @service_date_process

	INSERT INTO dbo.historical_headway_time_sr_same_disaggregate
	(
		service_date
		,stop_id
		,route_id
		,prev_route_id
		,route_type
		,direction_id
		,start_time_sec
		,end_time_sec
		,headway_time_sec
		,benchmark_headway_time_sec
	)
		SELECT
			service_date
			,stop_id
			,route_id
			,prev_route_id
			,route_type
			,direction_id
			,start_time_sec
			,end_time_sec
			,headway_time_sec
			,benchmark_headway_time_sec
		FROM dbo.daily_headway_time_sr_same_disaggregate

	IF
		(
			SELECT
				COUNT(*)
			FROM dbo.historical_dwell_time_disaggregate
			WHERE
				service_date = @service_date_process
		)
		> 0

		DELETE FROM dbo.historical_dwell_time_disaggregate
		WHERE
			service_date = @service_date_process

	INSERT INTO dbo.historical_dwell_time_disaggregate
	(
		service_date
		,stop_id
		,route_id
		,direction_id
		,start_time_sec
		,end_time_sec
		,dwell_time_sec
	)
		SELECT
			service_date
			,stop_id
			,route_id
			,direction_id
			,start_time_sec
			,end_time_sec
			,dwell_time_sec
		FROM dbo.daily_dwell_time_disaggregate

	IF
		(
			SELECT
				COUNT(*)
			FROM dbo.historical_schedule_adherence_disaggregate
			WHERE
				service_date = @service_date_process
		)
		> 0

		DELETE FROM dbo.historical_schedule_adherence_disaggregate
		WHERE
			service_date = @service_date_process

	INSERT INTO dbo.historical_schedule_adherence_disaggregate
	(
		service_date
		,route_id
		,route_type
		,direction_id
		,trip_id
		,stop_sequence
		,stop_id
		,vehicle_id
		,scheduled_arrival_time_sec
		,actual_arrival_time_sec
		,arrival_delay_sec
		,scheduled_departure_time_sec
		,actual_departure_time_sec
		,departure_delay_sec
		,stop_order_flag
	)
		SELECT
			service_date
			,route_id
			,route_type
			,direction_id
			,trip_id
			,stop_sequence
			,stop_id
			,vehicle_id
			,scheduled_arrival_time_sec
			,actual_arrival_time_sec
			,arrival_delay_sec
			,scheduled_departure_time_sec
			,actual_departure_time_sec
			,departure_delay_sec
			,stop_order_flag
		FROM dbo.daily_schedule_adherence_disaggregate

	IF
		(
			SELECT
				COUNT(*)
			FROM dbo.historical_schedule_adherence_threshold_pax
			WHERE
				service_date = @service_date_process
		)
		> 0

		DELETE FROM dbo.historical_schedule_adherence_threshold_pax
		WHERE
			service_date = @service_date_process

	INSERT INTO dbo.historical_schedule_adherence_threshold_pax
	(
		service_date
		,route_id
		,direction_id
		,trip_id
		,stop_sequence
		,stop_id
		,vehicle_id
		,scheduled_arrival_time_sec
		,actual_arrival_time_sec
		,arrival_delay_sec
		,scheduled_departure_time_sec
		,actual_departure_time_sec
		,departure_delay_sec
		,stop_order_flag
		,threshold_id
		,threshold_value
		,denominator_pax
		,scheduled_threshold_numerator_pax
		,denominator_trip
		,scheduled_threshold_numerator_trip
	)
		SELECT
			service_date
			,route_id
			,direction_id
			,trip_id
			,stop_sequence
			,stop_id
			,vehicle_id
			,scheduled_arrival_time_sec
			,actual_arrival_time_sec
			,arrival_delay_sec
			,scheduled_departure_time_sec
			,actual_departure_time_sec
			,departure_delay_sec
			,stop_order_flag
			,threshold_id
			,threshold_value
			,denominator_pax
			,scheduled_threshold_numerator_pax
			,NULL
			,NULL
		FROM dbo.daily_schedule_adherence_threshold_pax

	IF
		(
			SELECT
				COUNT(*)
			FROM dbo.historical_travel_time_threshold_pax
			WHERE
				service_date = @service_date_process
		)
		> 0

		DELETE FROM dbo.historical_travel_time_threshold_pax
		WHERE
			service_date = @service_date_process

	INSERT INTO dbo.historical_travel_time_threshold_pax
	(
		service_date
		,from_stop_id
		,to_stop_id
		,direction_id
		,prev_route_id
		,route_id
		,start_time_sec
		,end_time_sec
		,travel_time_sec
		,threshold_id
		,threshold_historical_median_travel_time_sec
		,threshold_scheduled_median_travel_time_sec
		,threshold_historical_average_travel_time_sec
		,threshold_scheduled_average_travel_time_sec
		,denominator_pax
		,historical_threshold_numerator_pax
		,scheduled_threshold_numerator_pax
		,denominator_trip
		,historical_threshold_numerator_trip
		,scheduled_threshold_numerator_trip
	)
		SELECT
			service_date
			,from_stop_id
			,to_stop_id
			,direction_id
			,prev_route_id
			,route_id
			,start_time_sec
			,end_time_sec
			,travel_time_sec
			,threshold_id
			,threshold_historical_median_travel_time_sec
			,threshold_scheduled_median_travel_time_sec
			,threshold_historical_average_travel_time_sec
			,threshold_scheduled_average_travel_time_sec
			,denominator_pax
			,historical_threshold_numerator_pax
			,scheduled_threshold_numerator_pax
			,NULL
			,NULL
			,NULL
		FROM dbo.daily_travel_time_threshold_pax

	IF
		(
			SELECT
				COUNT(*)
			FROM dbo.historical_wait_time_od_threshold_pax
			WHERE
				service_date = @service_date_process
		)
		> 0

		DELETE FROM dbo.historical_wait_time_od_threshold_pax
		WHERE
			service_date = @service_date_process

	INSERT INTO dbo.historical_wait_time_od_threshold_pax
	(
		service_date
		,from_stop_id
		,to_stop_id
		,direction_id
		,prev_route_id
		,route_id
		,start_time_sec
		,end_time_sec
		,max_wait_time_sec
		,dwell_time_sec
		,threshold_id
		,threshold_historical_median_wait_time_sec
		,threshold_scheduled_median_wait_time_sec
		,threshold_historical_average_wait_time_sec
		,threshold_scheduled_average_wait_time_sec
		,denominator_pax
		,historical_threshold_numerator_pax
		,scheduled_threshold_numerator_pax
		,denominator_trip
		,historical_threshold_numerator_trip
		,scheduled_threshold_numerator_trip
	)
		SELECT
			service_date
			,from_stop_id
			,to_stop_id
			,direction_id
			,prev_route_id
			,route_id
			,start_time_sec
			,end_time_sec
			,max_wait_time_sec
			,dwell_time_sec
			,threshold_id
			,threshold_historical_median_wait_time_sec
			,threshold_scheduled_median_wait_time_sec
			,threshold_historical_average_wait_time_sec
			,threshold_scheduled_average_wait_time_sec
			,denominator_pax
			,historical_threshold_numerator_pax
			,scheduled_threshold_numerator_pax
			,NULL
			,NULL
			,NULL
		FROM dbo.daily_wait_time_od_threshold_pax

	IF
		(
			SELECT
				COUNT(*)
			FROM dbo.historical_headway_time_threshold_trip
			WHERE
				service_date = @service_date_process
		)
		> 0

		DELETE FROM dbo.historical_headway_time_threshold_trip
		WHERE
			service_date = @service_date_process

	INSERT INTO dbo.historical_headway_time_threshold_trip
	(
		service_date
		,stop_id
		,direction_id
		,prev_route_id
		,route_id
		,start_time_sec
		,end_time_sec
		,headway_time_sec
		,threshold_id
		,threshold_scheduled_median_headway_time_sec
		,threshold_scheduled_average_headway_time_sec
		,denominator_trip
		,scheduled_threshold_numerator_trip
	)
		SELECT
			@service_date_process
			,stop_id
			,direction_id
			,prev_route_id
			,route_id
			,start_time_sec
			,end_time_sec
			,headway_time_sec
			,threshold_id
			,threshold_scheduled_median_headway_time_sec
			,threshold_scheduled_average_headway_time_sec
			,denominator_trip
			,scheduled_threshold_numerator_trip
		FROM dbo.daily_headway_time_threshold_trip

	IF
		(
			SELECT
				COUNT(*)
			FROM dbo.historical_metrics
			WHERE
				service_date = @service_date_process
		)
		> 0

		DELETE FROM dbo.historical_metrics
		WHERE
			service_date = @service_date_process

	INSERT INTO dbo.historical_metrics
	(
		service_date
		,route_id
		,threshold_id
		,threshold_name
		,threshold_type
		,metric_result
		,metric_result_trip
		,numerator_pax
		,denominator_pax
		,numerator_trip
		,denominator_trip
	)

		SELECT
			@service_date_process
			,dm.route_id
			,dm.threshold_id
			,dm.threshold_name
			,dm.threshold_type
			,dm.metric_result
			,dm.metric_result_trip
			,dm.numerator_pax
			,dm.denominator_pax
			,dm.numerator_trip
			,dm.denominator_trip

		FROM dbo.daily_metrics dm

	IF
		(
			SELECT
				COUNT(*)
			FROM dbo.historical_missed_stop_times_scheduled
			WHERE
				service_date = @service_date_process
		)
		> 0

		DELETE FROM dbo.historical_missed_stop_times_scheduled
		WHERE
			service_date = @service_date_process

	INSERT INTO dbo.historical_missed_stop_times_scheduled
	(
		record_id
		,service_date
		,trip_id
		,stop_sequence
		,stop_id
		,scheduled_arrival_time_sec
		,scheduled_departure_time_sec
		,actual_arrival_time_sec
		,actual_departure_time_sec
		,max_before_stop_sequence
		,max_before_arrival_time_sec
		,max_before_departure_time_sec
		,max_before_event_time_arrival_sec
		,max_before_event_time_departure_sec
		,min_after_stop_sequence
		,min_after_arrival_time_sec
		,min_after_departure_time_sec
		,min_after_event_time_arrival_sec
		,min_after_event_time_departure_sec
		,expected_arrival_time_sec
		,expected_departure_time_sec
	)
		SELECT
			record_id
			,service_date
			,trip_id
			,stop_sequence
			,stop_id
			,scheduled_arrival_time_sec
			,scheduled_departure_time_sec
			,actual_arrival_time_sec
			,actual_departure_time_sec
			,max_before_stop_sequence
			,max_before_arrival_time_sec
			,max_before_departure_time_sec
			,max_before_event_time_arrival_sec
			,max_before_event_time_departure_sec
			,min_after_stop_sequence
			,min_after_arrival_time_sec
			,min_after_departure_time_sec
			,min_after_event_time_arrival_sec
			,min_after_event_time_departure_sec
			,expected_arrival_time_sec
			,expected_departure_time_sec
		FROM dbo.daily_missed_stop_times_scheduled

	--DROP all temp tables

	IF OBJECT_ID('tempdb..##daily_cd_time','U') IS NOT NULL
		DROP TABLE ##daily_cd_time

	IF OBJECT_ID('tempdb..##daily_de_time','U') IS NOT NULL
		DROP TABLE ##daily_de_time

	IF OBJECT_ID('tempdb..##daily_cde_time','U') IS NOT NULL
		DROP TABLE ##daily_cde_time

	IF OBJECT_ID('tempdb..##daily_abcde_time','U') IS NOT NULL
		DROP TABLE ##daily_abcde_time

	IF OBJECT_ID('tempdb..##daily_bd_sr_all_time','u') IS NOT NULL
		DROP TABLE ##daily_bd_sr_all_time

	IF OBJECT_ID('tempdb..##daily_bd_sr_same_time','u') IS NOT NULL
		DROP TABLE ##daily_bd_sr_same_time

	IF OBJECT_ID('tempdb..#daily_arrival_time_sec','U') IS NOT NULL
		DROP TABLE #daily_arrival_time_sec

	IF OBJECT_ID('tempdb..#daily_departure_time_sec','U') IS NOT NULL
		DROP TABLE #daily_departure_time_sec

END



GO

	