  

---run this script in the transit-performance database
--USE transit_performance
--GO

IF OBJECT_ID('dbo.PostProcessDaily','P') IS NOT NULL
	DROP PROCEDURE dbo.PostProcessDaily

GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE dbo.PostProcessDaily 

--Script Version: Master - 1.1.3.0 - extrapolated-events-and-ejt - 4

--This procedure processes all of the events for the service_date being processed. It runs after the PreProcessDaily.

	@service_date DATE

AS


BEGIN
	SET NOCOUNT ON;

	DECLARE @service_date_process DATE
	SET @service_date_process = @service_date

	--variable for using checkpoints only in bus metrics TRUE = 1, FALSE = 0
	DECLARE @use_checkpoints_only BIT
	SET @use_checkpoints_only = 1

	--MBTA-specific correct route-direction-stop id combinations 
	DECLARE @correct_stop_ids AS TABLE
	(
		route_id				VARCHAR(255)
		,direction_id			INT
		,correct_stop_id		VARCHAR(255)
		,correct_stop_sequence	INT
		,incorrect_stop_id		VARCHAR(255)
	)

	INSERT INTO @correct_stop_ids
	VALUES

		('Green-B',0,'70196',50,'70197')
		,('Green-B',0,'70196',50,'70198')
		,('Green-B',0,'70196',50,'70199')
		,('Green-C',0,'70197',60,'70196')
		,('Green-C',0,'70197',60,'70198')
		,('Green-C',0,'70197',60,'70199')
		,('Green-D',0,'70198',70,'70196')
		,('Green-D',0,'70198',70,'70197')
		,('Green-D',0,'70198',70,'70199')
		,('Green-E',0,'70199',80,'70196')
		,('Green-E',0,'70199',80,'70197')
		,('Green-E',0,'70199',80,'70198')

	--Terminals and Park Street for eliminating double headways
	DECLARE @multiple_berths as TABLE
	(
		route_id			VARCHAR(255)
		,direction_id		INT
		,stop_id			VARCHAR(255)
	)

	INSERT INTO @multiple_berths
	VALUES
		('Blue',0,'70059')
		,('Blue',1,'70038')
		,('Orange',0,'70036')
		,('Orange',1,'70001')
		,('Red',0,'70061')
		,('Red',1,'70105')
		,('Red',1,'70094')
		,('Green-B',0,'70196')
		,('Green-C',0,'70197')
		,('Green-D',0,'70198')
		,('Green-E',0,'70199')
		,('Green-B',1,'70200')
		,('Green-C',1,'70200')
		,('Green-D',1,'70200')
		,('Green-E',1,'70200')
		,('Green-B',0,'70210')
		,('Green-C',0,'70210')
		,('Green-D',0,'70210')
		,('Green-E',0,'70210')
		,('Green-B',1,'70106')
		,('Green-C',1,'70238')
		,('Green-D',1,'70160')
		,('Green-E',1,'70260')

	--Key Bus Routes for determining headway- or departure time-based metrics
	DECLARE @kbr TABLE
	(
		route_id VARCHAR(255)
	)
	INSERT INTO @kbr
		VALUES 
		('1'),('15'),('22'),('23'),('28'),('32'),('39'),('57'),('66'),('71'),('73'),('77'),('11'),('741'),('742'),('751'),('749'),('743')

	--ensure events from vehicle positions have direction_id and event_time_sec----------------------
	UPDATE dbo.rt_event
		SET direction_id = t.direction_id
		FROM gtfs.trips t
		WHERE
				dbo.rt_event.trip_id = t.trip_id
			AND 
				dbo.rt_event.direction_id IS NULL
			AND 
				dbo.rt_event.service_date = @service_date_process													 

	UPDATE dbo.rt_event
		SET direction_id = rds.direction_id
		FROM gtfs.route_direction_stop rds
		WHERE
				rds.route_id = dbo.rt_event.route_id
			AND 
				rds.stop_id = dbo.rt_event.stop_id
			AND 
				dbo.rt_event.direction_id IS NULL
			AND 
				dbo.rt_event.service_date = @service_date_process

	UPDATE dbo.rt_event
		SET direction_id = 3
		WHERE
				dbo.rt_event.direction_id IS NULL
			AND 
				dbo.rt_event.service_date = @service_date_process

	UPDATE dbo.rt_event
		SET event_time_sec = DATEDIFF(s,service_date,dbo.fnConvertEpochToDateTime(event_time))
		WHERE
				dbo.rt_event.event_time_sec IS NULL
			AND 
				dbo.rt_event.service_date = @service_date_process

	--ensure events from trip_updates have direction_id---------------------
	UPDATE dbo.event_rt_trip_archive
		SET direction_id = t.direction_id
		FROM gtfs.trips t
		WHERE
				dbo.event_rt_trip_archive.trip_id = t.trip_id
			AND 
				dbo.event_rt_trip_archive.direction_id IS NULL
			AND 
				dbo.event_rt_trip_archive.service_date = @service_date_process

	UPDATE dbo.event_rt_trip_archive
		SET direction_id = rds.direction_id
		FROM gtfs.route_direction_stop rds
		WHERE
				rds.route_id = dbo.event_rt_trip_archive.route_id
			AND 
				rds.stop_id = dbo.event_rt_trip_archive.stop_id
			AND 
				dbo.event_rt_trip_archive.direction_id IS NULL
			AND 
				dbo.event_rt_trip_archive.service_date = @service_date_process

	UPDATE dbo.event_rt_trip_archive
		SET direction_id = 3
		WHERE
				dbo.event_rt_trip_archive.direction_id IS NULL
			AND 
				dbo.event_rt_trip_archive.service_date = @service_date_process

	--Create Trip Updates Table which stores predicted arrival and departure events and times for the day being processed

	IF OBJECT_ID('dbo.daily_trip_updates', 'U') IS NOT NULL
	  DROP TABLE dbo.daily_trip_updates
	;

	CREATE TABLE dbo.daily_trip_updates
	(
		record_id				INT PRIMARY KEY NOT NULL IDENTITY 
		,service_date			DATE NOT NULL
		,file_time				INT NOT NULL
		,route_id				VARCHAR(255) NOT NULL
		,route_type				INT NOT NULL
		,trip_id				VARCHAR(255) NOT NULL
		,direction_id			INT NOT NULL
		,stop_id				VARCHAR(255) NOT NULL
		,stop_sequence			INT NOT NULL
		,vehicle_id				VARCHAR(255) NOT NULL
		,vehicle_label			VARCHAR(255) 
		,event_type				CHAR(3) NOT NULL
		,event_time				INT NOT NULL
		,event_time_sec			INT NOT NULL
		,event_processed_rt		BIT NOT NULL
		,event_processed_daily	BIT NOT NULL
		,suspect_record			BIT DEFAULT 0 NOT NULL
		)
	;

	INSERT INTO dbo.daily_trip_updates
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
		,vehicle_label
		,event_type
		,event_time
		,event_time_sec
		,event_processed_rt
		,event_processed_daily
	)

	SELECT
		ert.service_date
		,ert.file_time
		,ert.route_id
		,r.route_type
		,ert.trip_id
		,ert.direction_id
		,ert.stop_id
		,ert.stop_sequence
		,ert.vehicle_id
		,ert.vehicle_label
		,ert.event_type
		,ert.event_time
		,DATEDIFF(s,ert.service_date,dbo.fnConvertEpochToDateTime(ert.event_time)) 	AS event_time_sec
		,0 AS event_processed_rt
		,0 AS event_processed_daily
	FROM dbo.event_rt_trip_archive ert, gtfs.routes r
	WHERE 
			ert.service_date = @service_date_process
		AND 
			r.route_id = ert.route_id
		AND 
			ert.vehicle_id IS NOT NULL


	------update "incorrect" berths for Green Line in trip updates
	UPDATE daily_trip_updates
	SET
		stop_id = cs.correct_stop_id
		,stop_sequence = cs.correct_stop_sequence
	FROM daily_trip_updates dtu
	JOIN @correct_stop_ids cs
	ON
			dtu.route_id = cs.route_id
		AND
			dtu.direction_id = cs.direction_id
		AND
			dtu.stop_id = cs.incorrect_stop_id

	--update events from trip updates for platform-specific child stops with station child stops
	UPDATE dbo.daily_trip_updates
		SET stop_id = ps.stop_id
		FROM dbo.daily_trip_updates e
		JOIN
		(
			SELECT rds.route_type, rds.route_id, rds.direction_id, rds.stop_order, rds.stop_id, s.parent_station
			FROM gtfs.route_direction_stop rds
			JOIN gtfs.stops s
			ON
				rds.stop_id = s.stop_id
		) ps
		ON
				e.route_id = ps.route_id
			AND
				e.direction_id = ps.direction_id
			AND
				(SELECT parent_station FROM gtfs.stops s WHERE e.stop_id = s.stop_id) = ps.parent_station
		WHERE		
				e.service_date = @service_date_process
			AND
				e.stop_id NOT IN (SELECT stop_id FROM gtfs.route_direction_stop)
										
	--MARK SUSPECT RECORDS FOR TRIP UPDATES
	--mark records with stop_sequence 0 as suspect
	UPDATE dbo.daily_trip_updates
		SET suspect_record = 1
		WHERE stop_sequence = 0
	
	--mark records with event time 0 as suspect			   
	UPDATE dbo.daily_trip_updates
		SET suspect_record = 1
		WHERE event_time = 0
		
	--mark records for trips with multiple vehicle ids as suspect		
	UPDATE dbo.daily_trip_updates
		SET suspect_record = 1
		FROM
			dbo.daily_trip_updates dtu
			JOIN 
			(
				SELECT
					service_date
					,trip_id
					,stop_sequence
					,stop_id
					,event_type
					,COUNT(vehicle_id) as num_duplicates
				FROM dbo.daily_trip_updates 									   
				GROUP BY
					service_date
					,trip_id
					,stop_sequence
					,stop_id
					,event_type
				HAVING COUNT(vehicle_id) > 1
			) t
				ON
						dtu.service_date = t.service_date    
					AND 
						dtu.trip_id = t.trip_id
					AND 
						dtu.stop_sequence = t.stop_sequence
					AND 
						dtu.stop_id = t.stop_id			 

	--mark records for trips with only one file time as suspect
	UPDATE dbo.daily_trip_updates
		SET suspect_record = 1
		FROM
			dbo.daily_trip_updates dtu
			JOIN 
			( 	
				SELECT
					service_date
					,trip_id
					,COUNT(DISTINCT file_time) as num_file_time
				FROM dbo.daily_trip_updates			   
				GROUP BY
					service_date
					,trip_id
				HAVING COUNT(DISTINCT file_time) = 1
			) t
				ON
						dtu.service_date = t.service_date
					AND 
						dtu.trip_id = t.trip_id	
	
	--mark stale records as suspect
	UPDATE dbo.daily_trip_updates
		SET suspect_record = 1
		WHERE event_time - file_time > 300

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
		,vehicle_label			VARCHAR(255)	
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
		,vehicle_label
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
			,vehicle_label
			,event_type
			,event_time
			,event_time_sec
			,event_processed_rt
			,event_processed_daily
		FROM	dbo.rt_event ed
				,gtfs.routes r
		WHERE
				service_date = @service_date_process
			AND 
				r.route_id = ed.route_id
			AND 
				event_time_sec IS NOT NULL
		ORDER BY record_id
	
	------update "incorrect" berths for Green Line in daily_event
	UPDATE daily_event
	SET
		stop_id = cs.correct_stop_id
		,stop_sequence = cs.correct_stop_sequence
	FROM daily_event de
	JOIN @correct_stop_ids cs
	ON
			de.route_id = cs.route_id
		AND
			de.direction_id = cs.direction_id
		AND
			de.stop_id = cs.incorrect_stop_id

	--update events from vehicle positions for platform-specific child stops with station child stops
	UPDATE dbo.daily_event
		SET stop_id = ps.stop_id
		FROM dbo.daily_event e
		JOIN
		(
			SELECT rds.route_type, rds.route_id, rds.direction_id, rds.stop_order, rds.stop_id, s.parent_station
			FROM gtfs.route_direction_stop rds
			JOIN gtfs.stops s
			ON
				rds.stop_id = s.stop_id
		) ps
		ON
				e.route_id = ps.route_id
			AND
				e.direction_id = ps.direction_id
			AND
				(SELECT parent_station FROM gtfs.stops s WHERE e.stop_id = s.stop_id) = ps.parent_station
		WHERE		
				e.service_date = @service_date_process
			AND
				e.stop_id NOT IN (SELECT stop_id FROM gtfs.route_direction_stop)

	--MARK SUSPECT RECORDS
	-----mark suspect record where event happens after 3:30 am the next day
	UPDATE dbo.daily_event
		SET suspect_record = 1
		WHERE
			event_time_sec > 99000

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
					AND 
						t.route_type IN (1,2,3) --MBTA specific
			) s
				ON
						ed.service_date = s.service_date
					AND 
						ed.vehicle_id = s.vehicle_id
					AND 
						ed.trip_id = s.trip_id

	--Records where there are duplicate events for trip-stop that are not already suspect
	--BUT do not mark the first arrival time as suspect for Heavy Rail
	UPDATE dbo.daily_event
		SET suspect_record = 1
		FROM dbo.daily_event ed
			JOIN
			(
				SELECT
					e.service_date
					,e.trip_id
					,e.stop_sequence
					,e.stop_id
					,e.event_type
					,MIN(e.event_time) as min_event_time
				FROM dbo.daily_event e
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
							AND
								event_type = 'ARR'
							AND
								route_type = 1
						GROUP BY
							service_date
							,trip_id
							,stop_sequence
							,stop_id
							,event_type
						HAVING COUNT(*) > 1
					) t
						ON
								e.service_date = t.service_date
							AND 
								e.trip_id = t.trip_id
							AND 
								e.stop_sequence = t.stop_sequence
							AND	
								e.stop_id = t.stop_id
							AND 
								e.event_type = t.event_type
						GROUP BY
							e.service_date
							,e.trip_id
							,e.stop_sequence
							,e.stop_id
							,e.event_type
			) s
				ON
						ed.service_date = s.service_date
					AND
						ed.trip_id = s.trip_id
					AND
						ed.stop_sequence = s.stop_sequence
					AND
						ed.stop_id = s.stop_id
					AND
						ed.event_type = s.event_type
			WHERE
				ed.event_time <> min_event_time	
				
	--Records where there are duplicate events for trip-stop that are not already suspect
	--BUT do not mark the last departure time as suspect for Heavy Rail
	UPDATE dbo.daily_event
		SET suspect_record = 1
		FROM dbo.daily_event ed
			JOIN
			(
				SELECT
					e.service_date
					,e.trip_id
					,e.stop_sequence
					,e.stop_id
					,e.event_type
					,MAX(e.event_time) as max_event_time
				FROM dbo.daily_event e
				JOIN
					(
						SELECT
							service_date
							,trip_id
							,stop_sequence
							,stop_id
							,event_type
							,vehicle_id
							,COUNT(*) AS num_duplicates
						FROM dbo.daily_event
						WHERE
								suspect_record = 0
							AND
								event_type = 'DEP'
							AND
								route_type = 1
						GROUP BY
							service_date
							,trip_id
							,stop_sequence
							,stop_id
							,event_type
							,vehicle_id
						HAVING COUNT(*) > 1
					) t
						ON
								e.service_date = t.service_date
							AND 
								e.trip_id = t.trip_id
							AND 
								e.stop_sequence = t.stop_sequence
							AND	
								e.stop_id = t.stop_id
							AND 
								e.event_type = t.event_type
						GROUP BY
							e.service_date
							,e.trip_id
							,e.stop_sequence
							,e.stop_id
							,e.event_type
			) s
				ON
						ed.service_date = s.service_date
					AND
						ed.trip_id = s.trip_id
					AND
						ed.stop_sequence = s.stop_sequence
					AND
						ed.stop_id = s.stop_id
					AND
						ed.event_type = s.event_type
			WHERE
				ed.event_time <> s.max_event_time	
	
	
	--Records where there are duplicate events for trip-stop that are not already suspect
	--For all other modes
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
					AND
						route_type <> 1
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
					AND 
						ed.trip_id = t.trip_id
					AND 
						ed.stop_sequence = t.stop_sequence
					AND 
						ed.stop_id = t.stop_id
					AND 
						ed.event_type = t.event_type
					)	

	--Records where there are duplicate events for vehicle-stop under different trip_id's within a certain time threshold (20 minutes)
	--For all other modes
	UPDATE dbo.daily_event
	SET suspect_record = 1
	FROM dbo.daily_event ed,
		(
			SELECT 
				de.trip_id
				,de.route_id
				,de.direction_id
				,de.vehicle_id
				,de.stop_id
				,MAX(de.event_time_sec) AS max_event_time_sec
			FROM
				dbo.daily_event de
			WHERE
				de.route_type IN (0,1)
				AND de.stop_id <> '70260'	-- Heath Street Outbound
			GROUP BY
				de.trip_id
				,de.route_id
				,de.direction_id
				,de.vehicle_id
				,de.stop_id
		) a,
		(
			SELECT 
				de.trip_id
				,de.route_id
				,de.direction_id
				,de.vehicle_id
				,de.stop_id
				,MIN(de.event_time_sec) AS min_event_time_sec
			FROM
				daily_event de
			GROUP BY
				de.trip_id
				,de.route_id
				,de.direction_id
				,de.vehicle_id
				,de.stop_id				
		) b		
	WHERE
			ed.trip_id = a.trip_id
		AND ed.route_id = a.route_id
		AND ed.direction_id = a.direction_id
		AND	ed.vehicle_id = a.vehicle_id
		AND ed.stop_id = a.stop_id
		AND a.route_id = b.route_id
		AND	a.direction_id = b.direction_id
		AND	a.vehicle_id = b.vehicle_id
		AND	a.stop_id = b.stop_id
		AND a.trip_id <> b.trip_id
		AND	ABS(a.max_event_time_sec - b.min_event_time_sec) <= 20*60

	
		
	-------finding missed events at the destination and adding last available prediction from trip_udpates----------
	IF OBJECT_ID('tempdb..##missed_events','U') IS NOT NULL
		DROP TABLE ##missed_events

	CREATE TABLE ##missed_events 
	(
		service_date		DATE
		,trip_id			VARCHAR(255)
		,vehicle_id			VARCHAR(255)
		,route_type			INT
		,stop_id			VARCHAR(255)
		,stop_sequence		INT
		,stop_order_flag	INT
		,event_type			CHAR(3)
	)

	--Heavy Rail and Commuter Rail: finding missed heavy rail and CR events at the destination
	--Scheduled Trips
	INSERT INTO ##missed_events
	(
		service_date
		,trip_id
		,vehicle_id
		,route_type
		,stop_id
		,stop_sequence
		,stop_order_flag
		,event_type
	)

		SELECT DISTINCT
			st.service_date
			,st.trip_id
			,de.vehicle_id
			,st.route_type
			,st.stop_id
			,st.stop_sequence
			,st.stop_order_flag
			,de.event_type
		FROM	
			dbo.daily_stop_times_sec st
			,dbo.daily_event de
		WHERE
				de.trip_id = st.trip_id
			AND 
				de.service_date = st.service_date
			AND 
				st.route_type IN (1, 2)
			AND 
				st.stop_order_flag = 3
			AND
				de.event_type = 'ARR' --only arrival events at terminals

	--Bus: finding missed bus events at all stops
	INSERT INTO ##missed_events
	( 
        service_date
		,trip_id
		,vehicle_id
        ,route_type
        ,stop_id
        ,stop_sequence
		,stop_order_flag
		,event_type
    )

		SELECT DISTINCT
			st.service_date
			,st.trip_id--
			,de.vehicle_id     
			,st.route_type
			,st.stop_id
			,st.stop_sequence
			,st.stop_order_flag									
			,de.event_type
		FROM
			dbo.daily_stop_times_sec st
			,dbo.daily_event de
		WHERE
				de.trip_id = st.trip_id
			AND 
				de.service_date = st.service_date
			AND 
				st.route_type = 3
				
	--remove stops from missed events that have events in daily_event
	DELETE FROM ##missed_events
	FROM
		##missed_events me
		,dbo.daily_event de
	WHERE
			me.trip_id = de.trip_id
		AND 
			me.vehicle_id = de.vehicle_id
		AND 
			me.stop_id = de.stop_id
		AND 
			me.stop_sequence = de.stop_sequence
		AND 
			me.service_date = de.service_date
		AND
			de.event_type = 
				CASE 
					WHEN me.event_type IN ('ARR','DEP') THEN me.event_type
					WHEN me.event_type = 'PRA' THEN 'ARR'
					WHEN me.event_type = 'PRD' THEN 'DEP'
				END

	--add events from trip_updates into daily event---------------------
	IF OBJECT_ID('tempdb..##valid_trip_update_events','U') IS NOT NULL
		DROP TABLE ##valid_trip_update_events

	CREATE TABLE ##valid_trip_update_events (
		service_date			DATE			NOT NULL
		,file_time				INT				NOT NULL
		,route_id				VARCHAR(255)	NOT NULL
		,route_type				INT				NOT NULL
		,trip_id				VARCHAR(255)	NOT NULL
		,direction_id			INT				NOT NULL
		,stop_id				VARCHAR(255)	NOT NULL
		,stop_sequence			INT				NOT NULL
		,vehicle_id				VARCHAR(255)	NOT NULL
		,vehicle_label			VARCHAR(255)
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
		,vehicle_label
		,event_type
		,event_time
		,event_time_sec
		,event_processed_rt
		,event_processed_daily
	)
                                                              
	SELECT DISTINCT
		dtu.service_date
		,dtu.file_time
		,dtu.route_id
		,dtu.route_type
		,dtu.trip_id
		,dtu.direction_id
		,dtu.stop_id
		,dtu.stop_sequence
		,dtu.vehicle_id
		,dtu.vehicle_label
		,dtu.event_type
		,dtu.event_time
		,dtu.event_time_sec
		,dtu.event_processed_rt
		,dtu.event_processed_daily
	FROM
		dbo.daily_trip_updates dtu
		JOIN ##missed_events me
		ON
				dtu.trip_id = me.trip_id
			AND 
				dtu.stop_id = me.stop_id
			AND 
				dtu.stop_sequence = me.stop_sequence
			AND 
				dtu.service_date = me.service_date
			AND 
				dtu.event_type = 
					CASE
						WHEN me.event_type IN ('PRA','PRD') THEN me.event_type
						WHEN me.event_type = 'ARR' THEN 'PRA'
						WHEN me.event_type = 'DEP' THEN 'PRD'
					END  
			AND
				dtu.suspect_record = 0	

		----find the departure/arrival times at the "previous" stop x and the "next" stop z for the "current" stop y with a missed event 
	IF OBJECT_ID('dbo.daily_missed_events') IS NOT NULL
	DROP TABLE dbo.daily_missed_events

	CREATE TABLE dbo.daily_missed_events
	(
		record_id							INT IDENTITY PRIMARY KEY
		,service_date						DATE
		,trip_id							VARCHAR(255)
		,vehicle_id							VARCHAR(255)
		,route_type							INT
		,stop_sequence						INT
		,stop_id							VARCHAR(255)
		,event_type							CHAR(3)
		,predicted_event_time				INT
		,max_before_stop_sequence			INT
		,max_before_stop_id					VARCHAR(255)
		,max_before_event_type				CHAR(3)
		,max_before_event_time				INT
		,min_after_stop_sequence			INT
		,min_after_stop_id					VARCHAR(255)
		,min_after_event_type				CHAR(3)
		,min_after_event_time				INT
	)

	INSERT INTO dbo.daily_missed_events
	(
		service_date
		,trip_id
		,vehicle_id
		,route_type
		,stop_sequence
		,stop_id
		,event_type
		,predicted_event_time
		,max_before_stop_sequence
		,max_before_stop_id
		,max_before_event_type
		,max_before_event_time
		,min_after_stop_sequence
		,min_after_stop_id
		,min_after_event_type
		,min_after_event_time
	)

	SELECT 
		y.service_date
		,y.trip_id
		,y.vehicle_id
		,y.route_type
		,y.stop_sequence
		,y.stop_id
		,y.event_type
		,y.event_time
		,t1.max_before_stop_sequence
		,t1.max_before_stop_id
		,t1.max_before_event_type
		,t1.max_before_event_time
		,t2.min_after_stop_sequence
		,t2.min_after_stop_id
		,t2.min_after_event_type
		,t2.min_after_event_time
	FROM
		##valid_trip_update_events y
	LEFT JOIN
		(
			SELECT 
				y.service_date
				,y.trip_id
				,y.vehicle_id
				,y.route_type
				,y.stop_sequence
				,y.stop_id
				,y.event_type
				,y.event_time
				,x.stop_sequence as max_before_stop_sequence
				,x.stop_id as max_before_stop_id
				,x.event_type	as max_before_event_type
				,x.event_time as max_before_event_time
				,ROW_NUMBER () OVER ( -- Partition finds the most recent relevant "previous" stop on the trip
					PARTITION BY y.trip_id, y.stop_id, y.event_type ORDER BY  x.stop_sequence DESC) AS rn
			FROM
				##valid_trip_update_events y
				JOIN dbo.daily_event x --x is the most recent relevant "previous" 
				ON
						y.service_date = x.service_date
					AND 
						y.route_id = x.route_id
					AND 
						y.direction_id = x.direction_id
					AND 
						y.trip_id = x.trip_id
					AND 
						y.stop_sequence > x.stop_sequence --make sure x stop is before y stop
					AND 
						x.suspect_record = 0
					AND 
						x.event_type = 'DEP'
		) t1
		ON
				y.service_date = t1.service_date
			AND 
				y.trip_id = t1.trip_id
			AND 
				y.stop_id = t1.stop_id
			AND 
				y.stop_sequence = t1.stop_sequence
			AND 
				y.event_type = t1.event_type
			AND 
				t1.rn = 1
	LEFT JOIN
		(
			SELECT 
			y.service_date
			,y.trip_id
			,y.vehicle_id
			,y.route_type
			,y.stop_sequence
			,y.stop_id
			,y.event_type
			,y.event_time
			,z.stop_sequence as min_after_stop_sequence
			,z.stop_id as min_after_stop_id
			,z.event_type	as min_after_event_type
			,z.event_time as min_after_event_time
			,ROW_NUMBER () OVER ( -- Partition finds the most recent relevant "after" stop on the trip
				PARTITION BY y.trip_id, y.stop_id, y.event_type ORDER BY  z.stop_sequence asc) AS rn
			FROM
				##valid_trip_update_events y
				JOIN dbo.daily_event z --z is the most recent relevant "after" 
					ON
							y.service_date = z.service_date
						AND 
							y.route_id = z.route_id
						AND 
							y.direction_id = z.direction_id
						AND 
							y.trip_id = z.trip_id
						AND 
							y.stop_sequence < z.stop_sequence --make sure z stop is after y  event
						AND 
							z.suspect_record = 0
						AND 
							z.event_type = 'ARR'
		) t2
		ON
				y.service_date = t2.service_date
			AND 
				y.trip_id = t2.trip_id
			AND 
				y.stop_id = t2.stop_id
			AND 
				y.stop_sequence = t2.stop_sequence
			AND 
				y.event_type = t2.event_type
			AND 
				t2.rn = 1

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
		,vehicle_label
		,event_type
		,event_time
		,event_time_sec
		,event_processed_rt
		,event_processed_daily
	)

	SELECT
		tue.service_date
		,tue.file_time
		,tue.route_id
		,tue.route_type
		,tue.trip_id
		,tue.direction_id
		,tue.stop_id
		,tue.stop_sequence
		,tue.vehicle_id
		,tue.vehicle_label
		,tue.event_type
		,tue.event_time
		,tue.event_time_sec
		,tue.event_processed_rt
		,tue.event_processed_daily
	FROM
		##valid_trip_update_events tue
		JOIN daily_missed_events dme
			ON
					tue.service_date = dme.service_date
				AND 
					tue.trip_id = dme.trip_id
				AND 
					tue.stop_id = dme.stop_id
				AND 
					tue.stop_sequence = dme.stop_sequence
				AND 
					tue.event_type = dme.event_type
				AND 
					dme.predicted_event_time BETWEEN ISNULL(max_before_event_time, dme.predicted_event_time) AND ISNULL(min_after_event_time, dme.predicted_event_time)
				AND 
					dme.record_id NOT IN --check for stops that have one event
					(
						SELECT dme.record_id
						FROM
							daily_missed_events dme
							JOIN daily_event de
								ON
										dme.service_date = de.service_date
									AND 
										dme.trip_id = de.trip_id
									AND 
										dme.stop_id = de.stop_id
									AND 
										dme.stop_sequence = de.stop_sequence
						WHERE
							(	
									dme.event_type = 'PRD' 
								AND 
									de.event_type = 'ARR'
								AND 
									dme.predicted_event_time < de.event_time 
							)
							OR (
									dme.event_type = 'PRA'
								AND 
									de.event_type = 'DEP'
								AND 
									dme.predicted_event_time > de.event_time
                )
							)

	--MARK SUSPECT RECORDS

	--records where a trip-vehicle only had 2 or fewer records
	UPDATE dbo.daily_event
	SET suspect_record = 1
	FROM
		dbo.daily_event ed
		JOIN (
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
					AND 
						t.route_type IN (0,1,2,3) --MBTA specific
			) s
				ON			
						ed.service_date = s.service_date
					AND 
						ed.vehicle_id = s.vehicle_id
					AND 
						ed.trip_id = s.trip_id			

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
					AND route_type = 0
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
					AND 
						ed.trip_id = t.trip_id
					AND 
						ed.stop_sequence = t.stop_sequence
					AND 
						ed.stop_id = t.stop_id
					AND 
						ed.event_type = t.event_type
					)

	--Records where there are events at the same stop under the same trip_id but multiple route_ids (Green Line)
	UPDATE dbo.daily_event
		SET suspect_record = 1
		FROM dbo.daily_event de
			JOIN	
			(				
				SELECT
					de.service_date
					,de.trip_id
					,de.stop_id
					,s.stop_name
				FROM 
					dbo.daily_event de
				LEFT JOIN
					gtfs.stops s
					ON
						de.stop_id = s.stop_id
				WHERE
					de.route_type = 0
			) a
			ON
				de.service_date = a.service_date
				AND de.trip_id = a.trip_id
				AND de.stop_id = a.stop_id				
			JOIN
			(				
				SELECT
					service_date
					,trip_id
					,stop_name
					,COUNT(*) AS route_id_count
				FROM 
					(
						SELECT DISTINCT	
							de.service_date
							,de.route_id
							,de.trip_id
							,s.stop_name --stop_name bc of multiple stop_ids for some stops
						FROM	
							dbo.daily_event de
						LEFT JOIN	
							gtfs.stops s
							ON	
								de.stop_id = s.stop_id
						WHERE
							suspect_record = 0
					) a
				GROUP BY
					service_date
					,trip_id
					,stop_name
				HAVING COUNT(*) > 1
			) t
				ON
					a.service_date = t.service_date
					AND a.trip_id = t.trip_id
					AND a.stop_name = t.stop_name


	-- Begin extrapolation of missed stop times
		
	IF OBJECT_ID('dbo.daily_missed_stop_times', 'U') IS NOT NULL
	DROP TABLE dbo.daily_missed_stop_times

	CREATE TABLE dbo.daily_missed_stop_times
	(
		record_id								INT	IDENTITY PRIMARY KEY
		,service_date							DATE
		,route_id								VARCHAR(255)
		,route_type								VARCHAR(255)
		,trip_id								VARCHAR(255)
		,direction_id							INT
		,stop_id								VARCHAR(255)
		,stop_sequence							INT
		,vehicle_id								VARCHAR(255)
		,max_before_stop_sequence				INT
		,max_before_stop_id						VARCHAR(255)
		,max_before_file_time					INT
		,max_before_vehicle_label				VARCHAR(255)
		,max_before_event_time_arrival_sec		FLOAT
		,max_before_event_time_departure_sec	FLOAT		
		,min_after_stop_sequence				INT
		,min_after_stop_id						VARCHAR(255)
		,min_after_file_time					INT
		,min_after_vehicle_label				VARCHAR(255)		
		,min_after_event_time_arrival_sec		FLOAT
		,min_after_event_time_departure_sec		FLOAT		
		,config_arrival_time_sec				INT
		,config_departure_time_sec				INT
		,min_after_arrival_time_sec				FLOAT
		,min_after_departure_time_sec			FLOAT		
		,expected_arrival_time_sec				INT
		,expected_departure_time_sec			INT		
	)

	INSERT INTO dbo.daily_missed_stop_times
	(
		service_date
		,route_id
		,route_type
		,trip_id
		,direction_id
		,stop_id
		,stop_sequence
		,vehicle_id
	)

	SELECT DISTINCT
		de.service_date
		,de.route_id
		,de.route_type
		,de.trip_id
		,de.direction_id
		,rds1.stop_id
		,rds1.stop_order
		,de.vehicle_id		
	FROM 
		daily_event de		
	RIGHT JOIN 
		gtfs.route_direction_stop rds1
		ON	
			de.route_id = rds1.route_id
			AND	de.direction_id = rds1.direction_id
	WHERE
		de.route_type IN (0,1)
		AND de.suspect_record = 0


	--Begin MBTA-specific prevention of extrapolated stop events for various circumstances

	DECLARE @forward_step AS TABLE
		(
			ID						INT IDENTITY
			,input_route_id			VARCHAR(255)
			,input_direction_id		INT
			,input_stop_sequence	INT
		)

	INSERT INTO @forward_step
		(
			input_route_id
			,input_direction_id
			,input_stop_sequence
		)

	VALUES 
		 ('Red', 1, 20)		--Red Line Northbound: Do not interpolate past Quincy Center if no valid events were recorded after it
		,('Red', 0, 200)	--Red Line Southbound: Do not interpolate past Quincy Center if no valid events were recorded after it
		,('Red', 1, 100)	--Red Line Northbound: Do not interpolate past JFK/UMass if no valid events were recorded after it	
		,('Red', 0, 130)	--Red Line Southbound: Do not interpolate past JFK/UMass if no valid events were recorded after it
		,('Red', 1, 150)	--Red Line Northbound: Do not interpolate past Park St if no valid events were recorded after it		
		,('Red', 0, 60)		--Red Line Southbound: Do not interpolate past Charles/MGH if no valid events were recorded after it	
		,('Red', 1, 180)	--Red Line Northbound: Do not interpolate past Central if no valid events were recorded after it		
		,('Red', 0, 30)		--Red Line Southbound: Do not interpolate past Harvard if no valid events were recorded after it	
		,('Orange', 1, 170)	--Orange Line Northbound: Do not interpolate past Wellington if no valid events were recorded after it
		,('Orange', 0 ,20)	--Orange Line Southbound: Do not interpolate past Wellington if no valid events were recorded after it
		,('Orange', 1, 120)	--Orange Line Northbound: Do not interpolate past Haymarket if no valid events were recorded after it
		,('Orange', 0, 60)	--Orange Line Southbound: Do not interpolate past North Station if no valid events were recorded after it
		,('Orange', 1, 70)	--Orange Line Northbound: Do not interpolate past Back Bay if no valid events were recorded after it
		,('Orange', 0, 110)	--Orange Line Southbound: Do not interpolate past Tufts MC if no valid events were recorded after it
		,('Orange', 1, 50)	--Orange Line Northbound: Do not interpolate past Ruggles if no valid events were recorded after it
		,('Orange', 0, 130)	--Orange Line Southbound: Do not interpolate past Mass Ave if no valid events were recorded after it
		,('Orange', 1, 20)	--Orange Line Northbound: Do not interpolate past Stony Brook if no valid events were recorded after it
		,('Orange',0, 160)	--Orange Line Southbound: Do not interpolate past Jackson if no valid events were recorded after it	
		,('Blue', 1, 70)	--Blue Line Eastbound: Do not interpolate past Orient Heights if no valid events were recorded after it
		,('Blue', 0, 40)	--Blue Line Westbound: Do not interpolate past Orient Heights if no valid events were recorded after it
		,('Blue', 1, 40)	--Blue Line Eastbound: Do not interpolate past Maverick if no valid events were recorded after it		
		,('Blue', 0, 60)	--Blue Line Westbound: Do not interpolate past Airport if no valid events were recorded after it
		,('Green-B', 1,	550)--B Branch Eastbound: Do not interpolate past Kenmore if no valid events were recorded after it
		,('Green-B', 0,	130)--B Branch Westbound: Do not interpolate past Kenmore if no valid events were recorded after it
		,('Green-B', 1,	600)--B Branch Eastbound: Do not interpolate past Park St if no valid events were recorded after it
		,('Green-B', 0,	50)	--B Branch Westbound: Do not interpolate past Park St if no valid events were recorded after it			
		,('Green-B', 1,	610)--B Branch Eastbound: Do not interpolate past Government Center if no valid events were recorded after it
		,('Green-B', 0,	40)	--B Branch Westbound: Do not interpolate past Government Center if no valid events were recorded after it				
		,('Green-B', 1,	630)--B Branch Eastbound: Do not interpolate past North Station if no valid events were recorded after it
		,('Green-B', 0,	20)	--B Branch Westbound: Do not interpolate past North Station if no valid events were recorded after it					
		,('Green-B', 1,	170)--B Branch Eastbound: Do not interpolate past Blandford if no valid events were recorded after it
		,('Green-B', 0,	140)--B Branch Westbound: Do not interpolate past Blandford if no valid events were recorded after it
		,('Green-B', 1,	10)	--B Branch Eastbound: Do not interpolate past South St if no valid events were recorded after it
		,('Green-B', 0,	290)--B Branch Westbound: Do not interpolate past Chestnut Hill if no valid events were recorded after it
		,('Green-B', 0,	110)--B Branch Westbound: Do not interpolate past Copley if no valid events were recorded after it	
		,('Green-C', 1,	550)--C Branch Eastbound: Do not interpolate past Kenmore if no valid events were recorded after it
		,('Green-C', 0,	130)--C Branch Westbound: Do not interpolate past Kenmore if no valid events were recorded after it
		,('Green-C', 1,	600)--C Branch Eastbound: Do not interpolate past Park St if no valid events were recorded after it
		,('Green-C', 0, 60)	--C Branch Westbound: Do not interpolate past Park St if no valid events were recorded after it			
		,('Green-C', 1,	610)--C Branch Eastbound: Do not interpolate past Government Center if no valid events were recorded after it
		,('Green-C', 0,	40)	--C Branch Westbound: Do not interpolate past Government Center if no valid events were recorded after it				
		,('Green-C', 1,	630)--C Branch Eastbound: Do not interpolate past North Station if no valid events were recorded after it
		,('Green-C', 0,	20)	--C Branch Westbound: Do not interpolate past North Station if no valid events were recorded after it					
		,('Green-C', 1,	300)--C Branch Eastbound: Do not interpolate past St Marys if no valid events were recorded after it
		,('Green-C', 0,	320)--C Branch Westbound: Do not interpolate past St Marys if no valid events were recorded after it				
		,('Green-C', 0,	110)--C Branch Westbound: Do not interpolate past Copley if no valid events were recorded after it
		,('Green-D', 1,	550)--D Branch Eastbound: Do not interpolate past Kenmore if no valid events were recorded after it
		,('Green-D', 0,	130)--D Branch Westbound: Do not interpolate past Kenmore if no valid events were recorded after it
		,('Green-D', 1,	600)--D Branch Eastbound: Do not interpolate past Park St if no valid events were recorded after it
		,('Green-D', 0,	70)	--D Branch Westbound: Do not interpolate past Park St if no valid events were recorded after it			
		,('Green-D', 1,	610)--D Branch Eastbound: Do not interpolate past Government Center if no valid events were recorded after it
		,('Green-D', 0,	40)	--D Branch Westbound: Do not interpolate past Government Center if no valid events were recorded after it				
		,('Green-D', 1,	630)--D Branch Eastbound: Do not interpolate past North Station if no valid events were recorded after it
		,('Green-D', 0,	20)	--D Branch Westbound: Do not interpolate past North Station if no valid events were recorded after it					
		,('Green-D', 1,	430)--D Branch Eastbound: Do not interpolate past Fenway if no valid events were recorded after it
		,('Green-D', 0,	450)--D Branch Westbound: Do not interpolate past Fenway if no valid events were recorded after it
		,('Green-D', 1,	380)--D Branch Eastbound: Do not interpolate past Reservoir if no valid events were recorded after it
		,('Green-D', 0,	490)--D Branch Westbound: Do not interpolate past Beaconsfield if no valid events were recorded after it			
		,('Green-D', 0,	110)--D Branch Westbound: Do not interpolate past Copley if no valid events were recorded after it
		,('Green-E', 1,	570)--E Branch Eastbound: Do not interpolate past Copley if no valid events were recorded after it
		,('Green-E', 0,	110)--E Branch Westbound: Do not interpolate past Copley if no valid events were recorded after it
		,('Green-E', 1,	600)--E Branch Eastbound: Do not interpolate past Park St if no valid events were recorded after it
		,('Green-E', 0,	80)	--E Branch Westbound: Do not interpolate past Park St if no valid events were recorded after it			
		,('Green-E', 1,	610)--E Branch Eastbound: Do not interpolate past Government Center if no valid events were recorded after it
		,('Green-E', 0,	40)	--E Branch Westbound: Do not interpolate past Government Center if no valid events were recorded after it				
		,('Green-E', 1,	630)--E Branch Eastbound: Do not interpolate past North Station if no valid events were recorded after it
		,('Green-E', 0,	20)	--E Branch Westbound: Do not interpolate past North Station if no valid events were recorded after it					
		,('Green-E', 1,	540)--E Branch Eastbound: Do not interpolate past Prudential if no valid events were recorded after it
		,('Green-E', 0,	580)--E Branch Westbound: Do not interpolate past Prudential if no valid events were recorded after it
		,('Green-E', 1,	490)--E Branch Eastbound: Do not interpolate past Brigham if no valid events were recorded after it
		,('Green-E', 0,	630)--E Branch Westbound: Do not interpolate past Brigham if no valid events were recorded after it
		
	DECLARE @current_ID INT = 1

	WHILE @current_ID <= (SELECT MAX(ID) FROM @forward_step)

	BEGIN

		DECLARE @input_route_id_process_forward			VARCHAR(255) 	= (SELECT input_route_id FROM @forward_step WHERE ID = @current_ID)
		DECLARE @input_direction_id_process_forward		INT 			= (SELECT input_direction_id FROM @forward_step WHERE ID = @current_ID)
		DECLARE @input_stop_sequence_process_forward	INT 			= (SELECT input_stop_sequence FROM @forward_step WHERE ID = @current_ID)
				
		DELETE FROM dbo.daily_missed_stop_times
		FROM	
			dbo.daily_missed_stop_times mst
			,(
				SELECT
					de1.trip_id
					,de1.route_id
					,de1.direction_id
					,COUNT(DISTINCT de1.stop_id) as total_observed_stop_count
					,COUNT(DISTINCT de2.stop_id) as before_input_stop_count
				FROM
					dbo.daily_event de1
				JOIN
					dbo.daily_event de2
					ON
						de1.trip_id = de2.trip_id
						AND de1.route_id = de2.route_id
						AND de1.direction_id = de2.direction_id
				WHERE
					de1.route_id = @input_route_id_process_forward
					AND de1.direction_id = @input_direction_id_process_forward	
					AND de2.stop_sequence <= @input_stop_sequence_process_forward
					AND de1.suspect_record = 0
					AND de2.suspect_record = 0
				GROUP BY
					de1.trip_id
					,de1.route_id
					,de1.direction_id
				HAVING
					COUNT(DISTINCT de2.stop_id)/COUNT(DISTINCT de1.stop_id) = 1
			) trip_range
		WHERE
			mst.trip_id = trip_range.trip_id
			AND mst.route_id = trip_range.route_id
			AND mst.direction_id = trip_range.direction_id
			AND	mst.stop_sequence > @input_stop_sequence_process_forward	
	
	SET @current_ID = @current_ID + 1 		
	END
					
	DECLARE @backward_step AS TABLE
		(
			ID						INT IDENTITY
			,input_route_id			VARCHAR(255)
			,input_direction_id		INT
			,input_stop_sequence	INT
		)

	INSERT INTO @backward_step
		(
			input_route_id
			,input_direction_id
			,input_stop_sequence
		)

	VALUES 
		 ('Red', 1, 20)		--Red Line Northbound: Do not interpolate before Quincy Center if no valid events were recorded before it
		,('Red', 0, 200)	--Red Line Southbound: Do not interpolate before Quincy Center if no valid events were recorded before it
		,('Red', 1, 90)		--Red Line Northbound: Do not interpolate before JFK/UMass if no valid events were recorded before it
		,('Red', 0, 120)	--Red Line Southbound: Do not interpolate before JFK/UMass if no valid events were recorded before it
		,('Red', 1, 160)	--Red Line Northbound: Do not interpolate before Charles/MGH if no valid events were recorded before it				
		,('Red', 0, 70)		--Red Line Southbound: Do not interpolate before Park St if no valid events were recorded before it	
		,('Red', 1, 190)	--Red Line Northbound: Do not interpolate before Harvard if no valid events were recorded before it	
		,('Red', 0, 40)		--Red Line Southbound: Do not interpolate before Central if no valid events were recorded before it
		,('Orange', 0, 20)	--Orange Line Southbound: Do not interpolate before Wellington if no valid events were recorded before it		
		,('Orange', 1, 170)	--Orange Line Northbound: Do not interpolate before Wellington if no valid events were recorded before it
		,('Orange', 0, 70)	--Orange Line Southbound: Do not interpolate before Haymarket if no valid events were recorded before it		
		,('Orange',1, 130)	--Orange Line Northbound: Do not interpolate before North Station if no valid events were recorded before it
		,('Orange',0, 120)	--Orange Line Southbound: Do not interpolate before Back Bay if no valid events were recorded before it		
		,('Orange', 1, 80)	--Orange Line Northbound: Do not interpolate before Tufts MC if no valid events were recorded before it
		,('Orange', 0, 140)	--Orange Line Southbound: Do not interpolate before Ruggles if no valid events were recorded before it
		,('Orange', 1, 60)	--Orange Line Northbound: Do not interpolate before Mass Ave if no valid events were recorded before it
		,('Orange',0 ,170)	--Orange Line Southbound: Do not interpolate before Stony Brook if no valid events were recorded before it		
		,('Orange',1, 30)	--Orange Line Northbound: Do not interpolate before Jackson if no valid events were recorded before it
		,('Blue', 0, 40)	--Blue Line Westbound: Do not interpolate before Orient Heights if no valid events were recorded before it		
		,('Blue', 1, 70)	--Blue Line Eastbound: Do not interpolate before Orient Heights if no valid events were recorded before it
		,('Blue', 0, 70)	--Blue Line Westbound: Do not interpolate before Maverick if no valid events were recorded before it			
		,('Blue', 1, 50)	--Blue Line Eastbound: Do not interpolate before Airport if no valid events were recorded before it
		,('Green-B', 1,	550)--B Branch Eastbound: Do not interpolate before Kenmore if no valid events were recorded before it
		--,('Green-B', 0,	130)--B Branch Westbound: Do not interpolate before Kenmore if no valid events were recorded before it
		,('Green-B', 1,	600)--B Branch Eastbound: Do not interpolate before Park St if no valid events were recorded before it
		,('Green-B', 0,	50)	--B Branch Westbound: Do not interpolate before Park St if no valid events were recorded before it			
		,('Green-B', 1,	610)--B Branch Eastbound: Do not interpolate before Government Center if no valid events were recorded before it
		,('Green-B', 0,	40)	--B Branch Westbound: Do not interpolate before Government Center if no valid events were recorded before it				
		,('Green-B', 1,	630)--B Branch Eastbound: Do not interpolate before North Station if no valid events were recorded before it
		,('Green-B', 0,	20)	--B Branch Westbound: Do not interpolate before North Station if no valid events were recorded before it					
		,('Green-B', 1,	170)--B Branch Eastbound: Do not interpolate before Blandford if no valid events were recorded before it
		,('Green-B', 0,	140)--B Branch Westbound: Do not interpolate before Blandford if no valid events were recorded before it
		,('Green-B', 1,	20)	--B Branch Eastbound: Do not interpolate before Chestnut Hill if no valid events were recorded before it
		,('Green-B', 0,	300)--B Branch Westbound: Do not interpolate before South St if no valid events were recorded before it
		,('Green-B', 1,	570)--B Branch Eastbound: Do not interpolate before Copley if no valid events were recorded before it	
		,('Green-C', 1,	550)--C Branch Eastbound: Do not interpolate before Kenmore if no valid events were recorded before it
		--,('Green-C', 0,	130)--C Branch Westbound: Do not interpolate before Kenmore if no valid events were recorded before it
		,('Green-C', 1,	600)--C Branch Eastbound: Do not interpolate before Park St if no valid events were recorded before it
		,('Green-C', 0, 60)	--C Branch Westbound: Do not interpolate before Park St if no valid events were recorded before it			
		,('Green-C', 1,	610)--C Branch Eastbound: Do not interpolate before Government Center if no valid events were recorded before it
		,('Green-C', 0,	40)	--C Branch Westbound: Do not interpolate before Government Center if no valid events were recorded before it				
		,('Green-C', 1,	630)--C Branch Eastbound: Do not interpolate before North Station if no valid events were recorded before it
		,('Green-C', 0,	20)	--C Branch Westbound: Do not interpolate before North Station if no valid events were recorded before it					
		,('Green-C', 1,	300)--C Branch Eastbound: Do not interpolate before St Marys if no valid events were recorded before it
		,('Green-C', 0,	320)--C Branch Westbound: Do not interpolate before St Marys if no valid events were recorded before it	
		,('Green-C', 1,	570)--C Branch Eastbound: Do not interpolate before Copley if no valid events were recorded before it		
		,('Green-D', 1,	550)--D Branch Eastbound: Do not interpolate before Kenmore if no valid events were recorded before it
		--,('Green-D', 0,	130)--D Branch Westbound: Do not interpolate before Kenmore if no valid events were recorded before it
		,('Green-D', 1,	600)--D Branch Eastbound: Do not interpolate before Park St if no valid events were recorded before it
		,('Green-D', 0,	70)	--D Branch Westbound: Do not interpolate before Park St if no valid events were recorded before it			
		,('Green-D', 1,	610)--D Branch Eastbound: Do not interpolate before Government Center if no valid events were recorded before it
		,('Green-D', 0,	40)	--D Branch Westbound: Do not interpolate before Government Center if no valid events were recorded before it				
		,('Green-D', 1,	630)--D Branch Eastbound: Do not interpolate before North Station if no valid events were recorded before it
		,('Green-D', 0,	20)	--D Branch Westbound: Do not interpolate before North Station if no valid events were recorded before it					
		,('Green-D', 1,	430)--D Branch Eastbound: Do not interpolate before Fenway if no valid events were recorded before it
		,('Green-D', 0,	450)--D Branch Westbound: Do not interpolate before Fenway if no valid events were recorded before it
		,('Green-D', 1,	390)--D Branch Eastbound: Do not interpolate before Beaconsfield if no valid events were recorded before it
		,('Green-D', 0,	500)--D Branch Westbound: Do not interpolate before Reservoir if no valid events were recorded before it
		,('Green-D', 1,	570)--D Branch Eastbound: Do not interpolate before Copley if no valid events were recorded before it		
		,('Green-E', 1,	570)--E Branch Eastbound: Do not interpolate before Copley if no valid events were recorded before it
		,('Green-E', 0,	110)--E Branch Westbound: Do not interpolate before Copley if no valid events were recorded before it
		,('Green-E', 1,	600)--E Branch Eastbound: Do not interpolate before Park St if no valid events were recorded before it
		,('Green-E', 0,	80)	--E Branch Westbound: Do not interpolate before Park St if no valid events were recorded before it			
		,('Green-E', 1,	610)--E Branch Eastbound: Do not interpolate before Government Center if no valid events were recorded before it
		,('Green-E', 0,	40)	--E Branch Westbound: Do not interpolate before Government Center if no valid events were recorded before it				
		,('Green-E', 1,	630)--E Branch Eastbound: Do not interpolate before North Station if no valid events were recorded before it
		,('Green-E', 0,	20)	--E Branch Westbound: Do not interpolate before North Station if no valid events were recorded before it					
		,('Green-E', 1,	540)--E Branch Eastbound: Do not interpolate before Prudential if no valid events were recorded before it
		,('Green-E', 0,	580)--E Branch Westbound: Do not interpolate before Prudential if no valid events were recorded before it
		,('Green-E', 1,	490)--E Branch Eastbound: Do not interpolate before Brigham if no valid events were recorded before it
		,('Green-E', 0,	630)--E Branch Westbound: Do not interpolate before Brigham if no valid events were recorded before it

	DECLARE @current_ID_b INT = 1

	WHILE @current_ID_b <= (SELECT MAX(ID) FROM @backward_step)

	BEGIN

		DECLARE @input_route_id_process_backward			VARCHAR(255) 	= (SELECT input_route_id FROM @backward_step WHERE ID = @current_ID_b)
		DECLARE @input_direction_id_process_backward		INT 			= (SELECT input_direction_id FROM @backward_step WHERE ID = @current_ID_b)
		DECLARE @input_stop_sequence_process_backward		INT 			= (SELECT input_stop_sequence FROM @backward_step WHERE ID = @current_ID_b)			
			
		DELETE FROM dbo.daily_missed_stop_times
		FROM	
			dbo.daily_missed_stop_times mst
			,(
				SELECT
					de1.trip_id
					,de1.route_id
					,de1.direction_id
					,COUNT(DISTINCT de1.stop_id) as total_observed_stop_count
					,COUNT(DISTINCT de2.stop_id) as after_input_stop_count
				FROM dbo.daily_event de1
				JOIN dbo.daily_event de2
				ON
					de1.trip_id = de2.trip_id
					AND de1.route_id = de2.route_id
					AND de1.direction_id = de2.direction_id
				WHERE
					de1.route_id = @input_route_id_process_backward	
					AND de1.direction_id = @input_direction_id_process_backward
					AND de2.stop_sequence >= @input_stop_sequence_process_backward
					AND de1.suspect_record = 0
					AND de2.suspect_record = 0
				GROUP BY
					de1.trip_id
					,de1.route_id
					,de1.direction_id
				HAVING COUNT(DISTINCT de2.stop_id)/COUNT(DISTINCT de1.stop_id) = 1
			) trip_range
		WHERE
			mst.trip_id = trip_range.trip_id
			AND	mst.route_id = trip_range.route_id
			AND	mst.direction_id = trip_range.direction_id
			AND	mst.stop_sequence < @input_stop_sequence_process_backward
	
	SET @current_ID_b = @current_ID_b + 1 
	END

	--Red Line Northbound: Do not interpolate onto Braintree Branch if all activity is on Ashmont Branch
	DELETE FROM dbo.daily_missed_stop_times
	FROM	
		dbo.daily_missed_stop_times mst
		,(
			SELECT
				de1.trip_id
				,de1.route_id
				,de1.direction_id
				,COUNT(DISTINCT de1.stop_id) as total_stop_count
				,COUNT(DISTINCT de2.stop_id) as before_ashmont_stop_count
			FROM 
				dbo.daily_event de1
			JOIN 
				dbo.daily_event de2
				ON
					de1.trip_id = de2.trip_id
					AND de1.route_id = de2.route_id
					AND de1.direction_id = de2.direction_id
			WHERE
				de1.route_id = 'Red'
				AND de1.direction_id = 1
				AND de2.stop_sequence >= 50 --Ashmont
				AND de1.suspect_record = 0
				AND de2.suspect_record = 0				
			GROUP BY
				de1.trip_id
				,de1.route_id
				,de1.direction_id
			HAVING
				COUNT(DISTINCT de2.stop_id)/COUNT(DISTINCT de1.stop_id) = 1
		) trip_range
	WHERE
		mst.trip_id = trip_range.trip_id
		AND mst.route_id = trip_range.route_id
		AND	mst.direction_id = trip_range.direction_id
		AND	(
				mst.stop_sequence < 50 --Ashmont
				OR mst.stop_sequence = 100
			)

	--Red Line Northbound: Do not interpolate onto Ashmont Branch if all activity is on Braintree Branch
	DELETE FROM dbo.daily_missed_stop_times
	FROM	
		dbo.daily_missed_stop_times mst
		,(
			SELECT
				de1.trip_id
				,de1.route_id
				,de1.direction_id
				,COUNT(DISTINCT de1.stop_id) as total_stop_count
				,COUNT(DISTINCT de2.stop_id) as before_jfkumass_stop_count
			FROM 
				dbo.daily_event de1
			JOIN 
				dbo.daily_event de2
				ON
					de1.trip_id = de2.trip_id
					AND de1.route_id = de2.route_id
					AND de1.direction_id = de2.direction_id
			WHERE
				de1.route_id = 'Red'
				AND de1.direction_id = 1
				AND (
						de2.stop_sequence >= 90 --JFK/UMass
						OR de2.stop_sequence <= 40 --North Quincy
					)
				AND de1.suspect_record = 0
				AND de2.suspect_record = 0				
			GROUP BY
				de1.trip_id
				,de1.route_id
				,de1.direction_id
			HAVING 
				COUNT(DISTINCT de2.stop_id)/COUNT(DISTINCT de1.stop_id) = 1
		) trip_range
	WHERE
		mst.trip_id = trip_range.trip_id
		AND mst.route_id = trip_range.route_id
		AND mst.direction_id = trip_range.direction_id
		AND	mst.stop_sequence BETWEEN 50 AND 90 -- Ashmont and JFK/UMass

	--Red Line Southbound: Do not interpolate onto Braintree Branch if all activity is on Ashmont Branch
	DELETE FROM dbo.daily_missed_stop_times
	FROM	
		dbo.daily_missed_stop_times mst
		,(
			SELECT
				de1.trip_id
				,de1.route_id
				,de1.direction_id
				,COUNT(DISTINCT de1.stop_id) as total_stop_count
				,COUNT(DISTINCT de2.stop_id) as before_ashmont_stop_count
			FROM 
				dbo.daily_event de1
			JOIN 
				dbo.daily_event de2
				ON
					de1.trip_id = de2.trip_id
					AND de1.route_id = de2.route_id
					AND de1.direction_id = de2.direction_id
			WHERE
				de1.route_id = 'Red'
				AND de1.direction_id = 0
				AND de2.stop_sequence <= 170 --Ashmont
				AND de1.suspect_record = 0
				AND de2.suspect_record = 0				
			GROUP BY
				de1.trip_id
				,de1.route_id
				,de1.direction_id
			HAVING
				COUNT(DISTINCT de2.stop_id)/COUNT(DISTINCT de1.stop_id) = 1
		) trip_range
	WHERE
		mst.trip_id = trip_range.trip_id
		AND mst.route_id = trip_range.route_id
		AND	mst.direction_id = trip_range.direction_id
		AND	(
				mst.stop_sequence > 170 --Ashmont
				OR mst.stop_sequence = 120
			)

	--Red Line Southbound: Do not interpolate onto Ashmont Branch if all activity is on Braintree Branch
	DELETE FROM dbo.daily_missed_stop_times
	FROM	
		dbo.daily_missed_stop_times mst
		,(
			SELECT
				de1.trip_id
				,de1.route_id
				,de1.direction_id
				,COUNT(DISTINCT de1.stop_id) as total_stop_count
				,COUNT(DISTINCT de2.stop_id) as before_jfkumass_stop_count
			FROM 
				dbo.daily_event de1
			JOIN 
				dbo.daily_event de2
				ON
					de1.trip_id = de2.trip_id
					AND de1.route_id = de2.route_id
					AND de1.direction_id = de2.direction_id
			WHERE
				de1.route_id = 'Red'
				AND de1.direction_id = 0
				AND (
						de2.stop_sequence <= 130 --JFK/UMass
						OR de2.stop_sequence >= 180 --North Quincy
					)
				AND de1.suspect_record = 0
				AND de2.suspect_record = 0				
			GROUP BY
				de1.trip_id
				,de1.route_id
				,de1.direction_id
			HAVING
				COUNT(DISTINCT de2.stop_id)/COUNT(DISTINCT de1.stop_id) = 1
		) trip_range
	WHERE
		mst.trip_id = trip_range.trip_id
		AND mst.route_id = trip_range.route_id
		AND mst.direction_id = trip_range.direction_id
		AND	mst.stop_sequence BETWEEN 130 AND 170 --JFK/UMass and Ashmont


	--Do not extrapolate west of Beaconsfield due to complications from GLTS trip-matching issues
	DELETE FROM dbo.daily_missed_stop_times
	WHERE	
		route_id = 'Green-D'
		AND stop_id IN
			(
			'70177'		--Beaconsfield OB
			,'70175'	--Reservoir OB
			,'70173'	--Chestnut Hill OB
			,'70171'	--Newton Centre OB
			,'70169'	--Newton Highlands OB
			,'70167'	--Eliot OB
			,'70165'	--Waban OB
			,'70163'	--Woodland OB
			,'70161'	--Riverside OB
			,'70160'	--Riverside IB
			,'70162'	--Woodland IB
			,'70164'	--Waban IB
			,'70166'	--Eliot IB
			,'70168'	--Newton Highlands IB
			,'70170'	--Newton Centre IB
			,'70172'	--Chestnut Hill IB
			,'70174'	--Reservoir IB
			,'70176'	--Beaconsfield IB
			)

	--End MBTA-specific prevention of extrapolated stop events
	
	--Reduce to trip-stops where we did not get both an arrival and departure event from RTR
	DELETE FROM dbo.daily_missed_stop_times
	FROM	
		dbo.daily_missed_stop_times mst
		,(
			SELECT
				de.trip_id
				,de.route_id
				,de.stop_id
				,COUNT(*) AS count_events
			FROM 
				dbo.daily_event de
			WHERE
				de.event_type IN ('ARR', 'DEP')
				AND de.suspect_record = 0
			GROUP BY 
				de.trip_id
				,de.route_id
				,de.stop_id				
			HAVING
				COUNT(*) = 2
		) rtr
	WHERE
		mst.trip_id = rtr.trip_id
		AND mst.route_id = rtr.route_id
		AND mst.stop_id = rtr.stop_id

	--Remove missing trip-stops we filled with a prediction (heavy rail terminals)
	DELETE FROM dbo.daily_missed_stop_times
	FROM	
		dbo.daily_missed_stop_times mst
		,(
			SELECT
				de.trip_id
				,de.route_id
				,de.stop_id
				,COUNT(*) AS count_events
			FROM dbo.daily_event de
			WHERE
				de.event_type IN ('PRA', 'PRD')
				AND de.suspect_record = 0
			GROUP BY 
				de.trip_id
				,de.route_id
				,de.stop_id				
			HAVING COUNT(*) >= 1
		) tu
	WHERE
		mst.trip_id = tu.trip_id
		AND	mst.route_id = tu.route_id
		AND mst.stop_id = tu.stop_id			

	--Begin process of calculating extrapolated events
	
	UPDATE dbo.daily_missed_stop_times
		SET	
			min_after_stop_sequence = m.min_after_stop_sequence	
		FROM 
			dbo.daily_missed_stop_times mst
			,(
				SELECT
					mst.record_id
					,MIN(de.stop_sequence) AS min_after_stop_sequence
				FROM	
					dbo.daily_event de
					,dbo.daily_missed_stop_times mst
				WHERE
					mst.trip_id = de.trip_id
					AND mst.route_id = de.route_id
					AND mst.stop_sequence < de.stop_sequence
					AND de.suspect_record = 0
				GROUP BY
					mst.record_id
			) AS m
		WHERE
			mst.record_id = m.record_id	
	
	UPDATE dbo.daily_missed_stop_times
		SET	
			min_after_event_time_arrival_sec = m.min_event_time_sec
			,min_after_event_time_departure_sec = m.min_event_time_sec
			,min_after_file_time = m.min_file_time
		FROM 
			dbo.daily_missed_stop_times mst
			,(
				SELECT
					mst.record_id
					,MIN(de.event_time_sec) AS min_event_time_sec
					,MIN(de.file_time) AS min_file_time
				FROM	
					dbo.daily_event de
					,dbo.daily_missed_stop_times mst
				WHERE
					mst.trip_id = de.trip_id
					AND mst.route_id = de.route_id
					AND mst.min_after_stop_sequence = de.stop_sequence
					AND de.suspect_record = 0
				GROUP BY
					mst.record_id
			) AS m
		WHERE
			mst.record_id = m.record_id	
			
	UPDATE dbo.daily_missed_stop_times
		SET	
			min_after_stop_id = rds.stop_id
		FROM
			dbo.daily_missed_stop_times mst
			,gtfs.route_direction_stop rds
		WHERE
			mst.route_id = rds.route_id
			AND	mst.direction_id = rds.direction_id
			AND	mst.min_after_stop_sequence = rds.stop_order

	UPDATE dbo.daily_missed_stop_times
		SET	
			min_after_vehicle_label = de.vehicle_label
		FROM
			dbo.daily_missed_stop_times mst
			,dbo.daily_event de
		WHERE
			mst.vehicle_id = de.vehicle_id
			AND	mst.min_after_file_time = de.file_time
						
	UPDATE dbo.daily_missed_stop_times
		SET	
			max_before_stop_sequence = m.max_before_stop_sequence	
		FROM 
			dbo.daily_missed_stop_times mst
			,(
				SELECT
					mst.record_id
					,MAX(de.stop_sequence) max_before_stop_sequence
				FROM	
					dbo.daily_event de
					,dbo.daily_missed_stop_times mst
				WHERE
					mst.trip_id = de.trip_id
					AND mst.route_id = de.route_id
					AND mst.stop_sequence > de.stop_sequence
					AND de.suspect_record = 0
				GROUP BY
					mst.record_id
			) AS m
		WHERE
			mst.record_id = m.record_id				
				
	UPDATE dbo.daily_missed_stop_times
		SET	
			max_before_event_time_arrival_sec = m.max_event_time_sec
			,max_before_event_time_departure_sec = m.max_event_time_sec
			,max_before_file_time = m.max_file_time
		FROM 
			dbo.daily_missed_stop_times mst
			,(
				SELECT
					mst.record_id
					,MAX(de.event_time_sec) AS max_event_time_sec
					,MAX(de.file_time) AS max_file_time
				FROM	
					dbo.daily_event de
					,dbo.daily_missed_stop_times mst
				WHERE
					mst.trip_id = de.trip_id
					AND mst.route_id = de.route_id
					AND mst.max_before_stop_sequence = de.stop_sequence
					AND de.suspect_record = 0
				GROUP BY
					mst.record_id
			) AS m
		WHERE
			mst.record_id = m.record_id	

	UPDATE dbo.daily_missed_stop_times
		SET	
			max_before_stop_id = rds.stop_id
		FROM
			dbo.daily_missed_stop_times mst
			,gtfs.route_direction_stop rds
		WHERE
			mst.route_id = rds.route_id
			AND	mst.direction_id = rds.direction_id
			AND	mst.max_before_stop_sequence = rds.stop_order				

	UPDATE dbo.daily_missed_stop_times
		SET	
			max_before_vehicle_label = de.vehicle_label
		FROM
			dbo.daily_missed_stop_times mst
			,dbo.daily_event de
		WHERE
			mst.vehicle_id = de.vehicle_id
			AND	mst.max_before_file_time = de.file_time

	UPDATE dbo.daily_missed_stop_times
		SET 
			config_arrival_time_sec =  max_before_event_time_arrival_sec + ivt.scheduled_in_vehicle_time_sec
		FROM 
			dbo.daily_missed_stop_times mst									
		LEFT JOIN 
			dbo.service_date sd
			ON
				mst.service_date = sd.service_date		
		LEFT JOIN 
			dbo.config_time_period tp
			ON
				sd.day_type_id = tp.day_type_id
				AND	mst.max_before_event_time_arrival_sec >= tp.time_period_start_time_sec
				AND	mst.max_before_event_time_arrival_sec < tp.time_period_end_time_sec				
		LEFT JOIN
			dbo.daily_scheduled_in_vehicle_time ivt
			ON
				tp.time_period_id = ivt.time_period_id
				AND mst.route_id = ivt.route_id
				AND mst.stop_id = ivt.to_stop_id
				AND	mst.max_before_stop_id = ivt.from_stop_id			
				
	UPDATE dbo.daily_missed_stop_times
		SET 
			config_departure_time_sec =  max_before_event_time_departure_sec + ivt.scheduled_in_vehicle_time_sec
		FROM 
			dbo.daily_missed_stop_times mst									
		LEFT JOIN 
			dbo.service_date sd
			ON
				mst.service_date = sd.service_date		
		LEFT JOIN 
			dbo.config_time_period tp
			ON
				sd.day_type_id = tp.day_type_id
				AND	mst.max_before_event_time_arrival_sec >= tp.time_period_start_time_sec -- use arrival time so the time slice is consistent
				AND	mst.max_before_event_time_arrival_sec < tp.time_period_end_time_sec -- use arrival time so the time slice is consistent				
		LEFT JOIN 
			dbo.daily_scheduled_in_vehicle_time ivt
			ON
				tp.time_period_id = ivt.time_period_id
				AND mst.route_id = ivt.route_id
				AND mst.stop_id = ivt.to_stop_id
				AND	mst.max_before_stop_id = ivt.from_stop_id						
				
	UPDATE dbo.daily_missed_stop_times
		SET 
			min_after_arrival_time_sec =  max_before_event_time_arrival_sec + ivt.scheduled_in_vehicle_time_sec
		FROM 
			dbo.daily_missed_stop_times mst
			,dbo.daily_scheduled_in_vehicle_time ivt
			,dbo.config_time_period tp
			,dbo.service_date sd
		WHERE
			mst.max_before_event_time_arrival_sec >= tp.time_period_start_time_sec
			AND	mst.max_before_event_time_arrival_sec < tp.time_period_end_time_sec
			AND	mst.service_date = sd.service_date
			AND	sd.day_type_id = tp.day_type_id
			AND	tp.time_period_id = ivt.time_period_id
			AND mst.route_id = ivt.route_id
			AND mst.min_after_stop_id = ivt.to_stop_id
			AND	mst.max_before_stop_id = ivt.from_stop_id
				
	UPDATE dbo.daily_missed_stop_times
		SET 
			min_after_departure_time_sec =  max_before_event_time_departure_sec + ivt.scheduled_in_vehicle_time_sec
		FROM 
			dbo.daily_missed_stop_times mst
			,dbo.daily_scheduled_in_vehicle_time ivt
			,dbo.config_time_period tp
			,dbo.service_date sd
		WHERE
			mst.max_before_event_time_arrival_sec >= tp.time_period_start_time_sec  -- use arrival time so the time slice is consistent
			AND	mst.max_before_event_time_arrival_sec < tp.time_period_end_time_sec  -- use arrival time so the time slice is consistent
			AND	mst.service_date = sd.service_date
			AND	sd.day_type_id = tp.day_type_id
			AND	tp.time_period_id = ivt.time_period_id
			AND mst.route_id = ivt.route_id
			AND mst.min_after_stop_id = ivt.to_stop_id
			AND	mst.max_before_stop_id = ivt.from_stop_id				
								
	UPDATE dbo.daily_missed_stop_times
		SET 
			expected_arrival_time_sec = CAST((config_arrival_time_sec - max_before_event_time_arrival_sec) / (min_after_arrival_time_sec - max_before_event_time_arrival_sec) * (min_after_event_time_arrival_sec - max_before_event_time_arrival_sec) + max_before_event_time_arrival_sec AS INT)
		FROM 
			dbo.daily_missed_stop_times mst
		WHERE
			mst.max_before_stop_sequence IS NOT NULL
			AND mst.min_after_stop_sequence IS NOT NULL
			AND mst.min_after_arrival_time_sec IS NOT NULL

	UPDATE dbo.daily_missed_stop_times
		SET 
			expected_departure_time_sec = CAST((config_departure_time_sec - max_before_event_time_departure_sec) / (min_after_departure_time_sec - max_before_event_time_departure_sec) * (min_after_event_time_departure_sec - max_before_event_time_departure_sec) + max_before_event_time_departure_sec AS INT)
		FROM 
			dbo.daily_missed_stop_times mst
		WHERE
			mst.max_before_stop_sequence IS NOT NULL
			AND mst.min_after_stop_sequence IS NOT NULL
			AND mst.min_after_departure_time_sec IS NOT NULL

	--For a string of events that are missed at the start of the trip
	UPDATE dbo.daily_missed_stop_times
		SET 
			expected_arrival_time_sec =  min_after_event_time_arrival_sec - ivt.scheduled_in_vehicle_time_sec
		FROM 
			dbo.daily_missed_stop_times mst									
		LEFT JOIN 
			dbo.service_date sd
			ON
				mst.service_date = sd.service_date		
		LEFT JOIN 
			dbo.config_time_period tp
			ON
				sd.day_type_id = tp.day_type_id
				AND	mst.min_after_event_time_arrival_sec >= tp.time_period_start_time_sec
				AND	mst.min_after_event_time_arrival_sec < tp.time_period_end_time_sec				
		LEFT JOIN 
			dbo.daily_scheduled_in_vehicle_time ivt
			ON
				tp.time_period_id = ivt.time_period_id
				AND mst.stop_id = ivt.from_stop_id
				AND	mst.min_after_stop_id = ivt.to_stop_id				
		WHERE
			mst.expected_arrival_time_sec IS NULL	

	UPDATE dbo.daily_missed_stop_times
		SET 
			expected_departure_time_sec =  min_after_event_time_departure_sec - ivt.scheduled_in_vehicle_time_sec
		FROM 
			dbo.daily_missed_stop_times mst									
		LEFT JOIN 
			dbo.service_date sd
			ON
				mst.service_date = sd.service_date		
		LEFT JOIN 
			dbo.config_time_period tp
			ON
				sd.day_type_id = tp.day_type_id
				AND	mst.min_after_event_time_arrival_sec >= tp.time_period_start_time_sec  -- use arrival time so the time slice is consistent
				AND	mst.min_after_event_time_arrival_sec < tp.time_period_end_time_sec -- use arrival time so the time slice is consistent				
		LEFT JOIN dbo.daily_scheduled_in_vehicle_time ivt
			ON
				tp.time_period_id = ivt.time_period_id
				AND mst.stop_id = ivt.from_stop_id
				AND	mst.min_after_stop_id = ivt.to_stop_id				
		WHERE
			mst.expected_departure_time_sec IS NULL			

	--For a string of events that are missed at the end of the trip
	UPDATE dbo.daily_missed_stop_times
		SET 
			expected_arrival_time_sec =  max_before_event_time_arrival_sec + ivt.scheduled_in_vehicle_time_sec
		FROM 
			dbo.daily_missed_stop_times mst									
		LEFT JOIN 
			dbo.service_date sd
			ON
				mst.service_date = sd.service_date		
		LEFT JOIN 
			dbo.config_time_period tp
			ON
				sd.day_type_id = tp.day_type_id
				AND	mst.max_before_event_time_arrival_sec >= tp.time_period_start_time_sec
				AND	mst.max_before_event_time_arrival_sec < tp.time_period_end_time_sec				
		LEFT JOIN 
			dbo.daily_scheduled_in_vehicle_time ivt
			ON
				tp.time_period_id = ivt.time_period_id
				AND mst.stop_id = ivt.to_stop_id
				AND	mst.max_before_stop_id = ivt.from_stop_id				
		WHERE
			mst.expected_arrival_time_sec IS NULL	
			
	UPDATE dbo.daily_missed_stop_times
		SET 
			expected_departure_time_sec =  max_before_event_time_departure_sec + ivt.scheduled_in_vehicle_time_sec
		FROM 
			dbo.daily_missed_stop_times mst									
		LEFT JOIN 
			dbo.service_date sd
			ON
				mst.service_date = sd.service_date		
		LEFT JOIN 
			dbo.config_time_period tp
			ON
				sd.day_type_id = tp.day_type_id
				AND	mst.max_before_event_time_departure_sec >= tp.time_period_start_time_sec
				AND	mst.max_before_event_time_departure_sec < tp.time_period_end_time_sec				
		LEFT JOIN 
			dbo.daily_scheduled_in_vehicle_time ivt
			ON
				tp.time_period_id = ivt.time_period_id
				AND mst.stop_id = ivt.to_stop_id
				AND	mst.max_before_stop_id = ivt.from_stop_id				
		WHERE
			mst.expected_departure_time_sec IS NULL				

	-- delete where we saw an event within 15 minutes for the same vehicle at the stop (but under a different trip_id, route_id, or stop_id)
	DELETE FROM dbo.daily_missed_stop_times
	FROM	
		dbo.daily_missed_stop_times mst
		,(
			SELECT 
				mst.trip_id
				,mst.route_id
				,mst.direction_id
				,mst.vehicle_id
				,mst.stop_id
				,s.stop_name
				,mst.expected_arrival_time_sec
				,mst.expected_departure_time_sec
			FROM
				dbo.daily_missed_stop_times mst
			JOIN 
				gtfs.stops s
				ON
					mst.stop_id = s.stop_id
					AND	mst.stop_id <> '70260'	-- Heath Street Outbound
		) a
		,(
			SELECT 
				de.trip_id
				,de.route_id
				,de.direction_id
				,de.vehicle_id
				,s.stop_name
				,de.event_time_sec
			FROM
				daily_event de
			JOIN 
				gtfs.stops s
				ON
					de.stop_id = s.stop_id
			WHERE
				de.event_type = 'ARR'
				AND de.suspect_record = 0
		) b
		,(
			SELECT 
				de.trip_id
				,de.route_id
				,de.direction_id
				,de.vehicle_id
				,s.stop_name
				,de.event_time_sec
			FROM
				daily_event de
			JOIN 
				gtfs.stops s
				ON
					de.stop_id = s.stop_id
			WHERE
				de.event_type = 'DEP'
				AND de.suspect_record = 0
		) c			
	WHERE
		mst.trip_id = a.trip_id
		AND mst.route_id = a.route_id
		AND mst.direction_id = a.direction_id
		AND mst.stop_id = a.stop_id
		AND	a.direction_id = b.direction_id
		AND	a.vehicle_id = b.vehicle_id
		AND	a.stop_name = b.stop_name
		AND	a.direction_id = c.direction_id
		AND	a.vehicle_id = c.vehicle_id
		AND	a.stop_name = c.stop_name
		AND a.trip_id <> b.trip_id
		AND	a.trip_id <> c.trip_id
		AND (
				ABS(a.expected_arrival_time_sec - b.event_time_sec) <= 15*60
				OR ABS(a.expected_departure_time_sec - c.event_time_sec) <= 15*60
			)
			
	--Insert extrapolated events into daily_event
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
		,vehicle_label
		,event_type
		,event_time
		,event_time_sec
		,event_processed_rt
		,event_processed_daily
		,suspect_record
	)

	SELECT
		mst.service_date
		,CASE
			WHEN max_before_file_time IS NULL THEN min_after_file_time
			ELSE max_before_file_time
			END
		,mst.route_id
		,mst.route_type
		,mst.trip_id
		,mst.direction_id
		,mst.stop_id
		,mst.stop_sequence
		,mst.vehicle_id
		,CASE
			WHEN max_before_vehicle_label IS NULL THEN min_after_vehicle_label
			ELSE max_before_vehicle_label
			END
		,'EXA'
		,dbo.fnConvertDateTimeToEpoch(service_date) + mst.expected_arrival_time_sec
		,mst.expected_arrival_time_sec
		,0
		,0
		,0
	FROM			
		dbo.daily_missed_stop_times mst
	WHERE
		mst.expected_arrival_time_sec IS NOT NULL
			
	UNION ALL

	SELECT
		mst.service_date
		,CASE
			WHEN max_before_file_time IS NULL THEN min_after_file_time
			ELSE max_before_file_time
			END
		,mst.route_id
		,mst.route_type
		,mst.trip_id
		,mst.direction_id
		,mst.stop_id
		,mst.stop_sequence
		,mst.vehicle_id
		,CASE
			WHEN max_before_vehicle_label IS NULL THEN min_after_vehicle_label
			ELSE max_before_vehicle_label
			END
		,'EXD'
		,dbo.fnConvertDateTimeToEpoch(service_date) + mst.expected_departure_time_sec
		,mst.expected_departure_time_sec
		,0
		,0
		,0
	FROM			
		dbo.daily_missed_stop_times mst	
	WHERE
		mst.expected_departure_time_sec IS NOT NULL

	-- delete any lingering extrapolations where we have actuals
	DELETE FROM dbo.daily_event
	FROM	
		dbo.daily_event de
		,(
			SELECT 
				de.trip_id
				,de.stop_id
				,s.stop_name
			FROM
				dbo.daily_event de
			JOIN
				gtfs.stops s
				ON
					de.stop_id = s.stop_id	
			WHERE
				de.event_type = 'EXA'
		) a
		,(
			SELECT 
				de.trip_id
				,de.stop_id
				,s.stop_name
			FROM
				dbo.daily_event de
			JOIN 
				gtfs.stops s
				ON
					de.stop_id = s.stop_id
			WHERE
				de.event_type = 'ARR'
				AND de.suspect_record = 0
		) b		
	WHERE
		de.event_type = 'EXA'
		AND de.trip_id = a.trip_id
		AND de.stop_id = a.stop_id
		AND	a.trip_id = b.trip_id
		AND a.stop_name = b.stop_name --not stop_id bc there are multiple stop_ids for some stops

	DELETE FROM dbo.daily_event
	FROM	
		dbo.daily_event de
		,(
			SELECT 
				de.trip_id
				,de.stop_id
				,s.stop_name
			FROM
				dbo.daily_event de
			JOIN 
				gtfs.stops s
				ON
					de.stop_id = s.stop_id				
			WHERE
				de.event_type = 'EXD'
		) a
		,(
			SELECT 
				de.trip_id
				,de.stop_id
				,s.stop_name
			FROM
				dbo.daily_event de
			JOIN 
				gtfs.stops s
				ON
					de.stop_id = s.stop_id
			WHERE
				de.event_type = 'DEP'
				AND de.suspect_record = 0
		) b		
	WHERE
		de.event_type = 'EXD'
		AND	de.trip_id = a.trip_id
		AND de.stop_id = a.stop_id
		AND	a.trip_id = b.trip_id
		AND a.stop_name = b.stop_name --not stop_id bc there are multiple stop_ids for some stops

	--Delete any extrapolated departures for the last stop of each trip
	DELETE FROM dbo.daily_event
	FROM	
		dbo.daily_event de
		,(
			SELECT 
				de.trip_id
				,de.stop_sequence
			FROM
				dbo.daily_event de			
			WHERE
				event_type = 'EXD'
		) a
		,(
			SELECT 
				de.trip_id
				,MAX(de.stop_sequence) AS max_stop_sequence
			FROM
				dbo.daily_event de
			GROUP BY
				de.trip_id					
		) b		
	WHERE
		de.event_type = 'EXD'
		AND	de.trip_id = a.trip_id
		AND de.stop_sequence = a.stop_sequence
		AND	a.trip_id = b.trip_id
		AND a.stop_sequence = b.max_stop_sequence

	--Set extrapolated times equal to actual times at locations where a single ARR or DEP was recorded 
	UPDATE dbo.daily_event
		SET 
			event_time_sec =  de2.event_time_sec
		FROM 
			dbo.daily_event de									
		JOIN 
			(
				SELECT
					de.route_id
					,de.trip_id
					,de.stop_id
					,de.event_type
					,de.event_time_sec
				FROM
					dbo.daily_event de
				JOIN
					(
						SELECT
							de.trip_id
							,de.route_id
							,de.stop_id
							,COUNT(*) AS count_events
						FROM 
							dbo.daily_event de
						WHERE
							de.event_type IN ('ARR', 'DEP')
							AND de.suspect_record = 0
						GROUP BY 
							de.trip_id
							,de.route_id
							,de.stop_id				
						HAVING
							COUNT(*) = 1
					) a
					ON
						de.route_id = a.route_id
						AND de.trip_id = a.trip_id
						AND de.stop_id = a.stop_id
			) de2
			ON
				de.route_id = de2.route_id
				AND de.trip_id = de2.trip_id
				AND de.stop_id = de2.stop_id
		WHERE
			de.event_type = 'EXD'
			AND de2.event_type = 'ARR'

	UPDATE dbo.daily_event
		SET 
			event_time_sec =  de2.event_time_sec
		FROM 
			dbo.daily_event de									
		JOIN 
			(
				SELECT
					de.route_id
					,de.trip_id
					,de.stop_id
					,de.event_type
					,de.event_time_sec
				FROM
					dbo.daily_event de
				JOIN
					(
						SELECT
							de.trip_id
							,de.route_id
							,de.stop_id
							,COUNT(*) AS count_events
						FROM 
							dbo.daily_event de
						WHERE
							de.event_type IN ('ARR', 'DEP')
							AND de.suspect_record = 0
						GROUP BY 
							de.trip_id
							,de.route_id
							,de.stop_id				
						HAVING
							COUNT(*) = 1
					) a
					ON
						de.route_id = a.route_id
						AND de.trip_id = a.trip_id
						AND de.stop_id = a.stop_id
			) de2
			ON
				de.route_id = de2.route_id
				AND de.trip_id = de2.trip_id
				AND de.stop_id = de2.stop_id
		WHERE
			de.event_type = 'EXA'
			AND de2.event_type = 'DEP'

	--Fix overlaps at boundary locations
	DELETE FROM dbo.daily_event
	FROM
		dbo.daily_event de
		,(
			SELECT 
				de.trip_id
				,de.route_id
				,de.direction_id
				,de.stop_id
				,s.stop_name
				,de.event_time_sec
			FROM
				dbo.daily_event de
			JOIN 
				gtfs.stops s
				ON
					de.stop_id = s.stop_id				
			WHERE
				de.event_type = 'EXA'
		) a
		,(
			SELECT 
				de.trip_id
				,de.route_id
				,de.direction_id
				,de.stop_id
				,s.stop_name
				,MIN(de.event_time_sec) as min_event_time_sec
			FROM
				dbo.daily_event de
			JOIN 
				gtfs.stops s
				ON
					de.stop_id = s.stop_id				
			WHERE
				de.event_type = 'EXA'
				AND de.suspect_record = 0
			GROUP BY
				de.trip_id
				,de.route_id
				,de.direction_id
				,de.stop_id
				,s.stop_name
		) b		
	WHERE
		de.event_type = 'EXA'
		AND	de.trip_id = a.trip_id
		AND de.route_id = a.route_id
		AND de.direction_id = a.direction_id
		AND de.stop_id = a.stop_id
		AND	a.trip_id = b.trip_id
		AND a.route_id = b.route_id
		AND a.direction_id = b.direction_id
		AND a.stop_name = b.stop_name --not stop_id bc there are multiple stop_ids for some stops
		AND a.event_time_sec > b.min_event_time_sec

	--Continue fixing overlaps at boundary locations		
	DELETE FROM dbo.daily_event
	FROM
		dbo.daily_event de
		,(
			SELECT 
				de.trip_id
				,de.route_id
				,de.direction_id
				,de.stop_id
				,s.stop_name
				,de.event_time_sec
			FROM
				dbo.daily_event de
			JOIN 
				gtfs.stops s
				ON
					de.stop_id = s.stop_id				
			WHERE
				de.event_type = 'EXD'
		) a
		,(
			SELECT 
				de.trip_id
				,de.route_id
				,de.direction_id
				,de.stop_id
				,s.stop_name
				,MIN(de.event_time_sec) as min_event_time_sec
			FROM
				dbo.daily_event de
			JOIN 
				gtfs.stops s
				ON
					de.stop_id = s.stop_id
			WHERE
				de.event_type = 'EXD'
				AND de.suspect_record = 0
			GROUP BY
				de.trip_id
				,de.route_id
				,de.direction_id
				,de.stop_id
				,s.stop_name
		) b		
	WHERE
		de.event_type = 'EXD'
		AND	de.trip_id = a.trip_id
		AND de.route_id = a.route_id
		AND de.direction_id = a.direction_id
		AND de.stop_id = a.stop_id
		AND	a.trip_id = b.trip_id
		AND a.route_id = b.route_id
		AND a.direction_id = b.direction_id
		AND a.stop_name = b.stop_name --not stop_id bc there are multiple stop_ids for some stops
		AND a.event_time_sec > b.min_event_time_sec


	--Continue fixing overlaps at boundary locations
	UPDATE dbo.daily_event
		SET 
			event_time_sec =  b.event_time_sec
		FROM 
			dbo.daily_event de
		,(
			SELECT 
				de.trip_id
				,de.direction_id
				,de.stop_id
				,s.stop_name
				,de.event_time_sec
			FROM
				dbo.daily_event de
			JOIN 
				gtfs.stops s
				ON
					de.stop_id = s.stop_id				
			WHERE
				de.event_type = 'EXA'
		) a
		,(
			SELECT 
				de.trip_id
				,de.direction_id
				,de.stop_id
				,s.stop_name
				,event_time_sec
			FROM
				dbo.daily_event de
			JOIN 
				gtfs.stops s
				ON
					de.stop_id = s.stop_id
			WHERE
				de.event_type = 'EXD'
				AND de.suspect_record = 0
		) b		
	WHERE
		de.event_type = 'EXA'
		AND	de.trip_id = a.trip_id
		AND de.direction_id = a.direction_id
		AND de.stop_id = a.stop_id
		AND	a.trip_id = b.trip_id
		AND a.direction_id = b.direction_id
		AND a.stop_name = b.stop_name --not stop_id bc there are multiple stop_ids for some stops

		
	--Continue fixing overlaps at boundary locations
	UPDATE dbo.daily_event
		SET 
			event_time_sec =  b.event_time_sec
		FROM 
			dbo.daily_event de
		,(
			SELECT 
				de.route_id
				,de.direction_id
				,de.vehicle_id
				,de.stop_id
				,s.stop_name
				,de.event_time_sec
			FROM
				dbo.daily_event de
			JOIN 
				gtfs.stops s
				ON
					de.stop_id = s.stop_id				
			WHERE
				de.event_type = 'EXA'
		) a
		,(
			SELECT 
				de.route_id
				,de.direction_id
				,de.vehicle_id
				,de.stop_id
				,s.stop_name
				,event_time_sec
			FROM
				dbo.daily_event de
			JOIN 
				gtfs.stops s
				ON
					de.stop_id = s.stop_id
			WHERE
				de.event_type = 'EXD'
				AND de.suspect_record = 0
		) b		
	WHERE
		de.event_type = 'EXA'
		AND	de.route_id = a.route_id
		AND de.direction_id = a.direction_id
		AND de.vehicle_id = a.vehicle_id
		AND de.stop_id = a.stop_id
		AND	a.route_id = b.route_id
		AND a.direction_id = b.direction_id
		AND a.vehicle_id = b.vehicle_id
		AND a.stop_name = b.stop_name --not stop_id bc there are multiple stop_ids for some stops
		AND ABS(de.event_time_sec - b.event_time_sec) <= 15*60


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
					edc.event_type IN ('ARR','PRA','EXA')
				AND 
					edd.event_type IN ('DEP','PRD','EXD')
				AND 
					edc.service_date = edd.service_date
				AND 
					edc.stop_id = edd.stop_id
				AND 
					edc.stop_sequence = edd.stop_sequence
				AND 
					edc.direction_id = edd.direction_id
				AND 
					edc.vehicle_id = edd.vehicle_id
				AND 
					edc.trip_id = edd.trip_id
				AND 
					edd.event_time_sec >= edc.event_time_sec
	WHERE
			edd.suspect_record = 0
		AND 
			edc.suspect_record = 0

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
					edd.event_type IN ('DEP','PRD','EXD')
				AND 
					ede.event_type IN ('ARR','PRA','EXA')
				AND 
					ede.service_date = edd.service_date
				AND 
					ede.vehicle_id = edd.vehicle_id
				AND 
					ede.trip_id = edd.trip_id
				AND 
					ede.direction_id = edd.direction_id
				AND 
					ede.stop_sequence > edd.stop_sequence
				AND 
					CASE
						WHEN ede.event_time_sec > edd.event_time_sec THEN 1
						WHEN ede.event_time_sec = edd.event_time_sec THEN 1
						ELSE 0
					END = 1
				)
		WHERE
				ede.suspect_record = 0
			AND 
				edd.suspect_record = 0

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
				de.d_record_id = cd.d_record_id

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
				,x .cde_vehicle_id AS ab_vehicle_id
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
					PARTITION BY y.c_record_id, y.d_record_id, y.e_record_id ORDER BY x.d_time_sec DESC) AS rn

			FROM ##daily_cde_time y --y is the "current" trip

				JOIN ##daily_cde_time x --x is the most recent relevant "previous" trip

					ON
							y.service_date = x.service_date
						AND 
							y.cd_stop_id = x.cd_stop_id
						AND 
							y.e_stop_id = x.e_stop_id
						AND 
							y.cde_direction_id = x.cde_direction_id
						AND 
							y.cde_vehicle_id <> x.cde_vehicle_id
						AND 
							y.cde_trip_id <> x.cde_trip_id
						AND 
							CASE
								WHEN
										/*y.cde_route_id IN ('Green-B','Green-C','Green-D','Green-E')
									OR */
									(
											y.cd_stop_id IN (SELECT stop_id FROM @multiple_berths)
										AND 
											y.cde_direction_id IN (SELECT DISTINCT direction_id FROM @multiple_berths WHERE stop_id = y.cd_stop_id)
										AND 
											y.cde_route_id IN (SELECT DISTINCT route_id FROM @multiple_berths WHERE stop_id = y.cd_stop_id)
									)
								THEN y.d_time_sec
								ELSE y.c_time_sec
							END > x.d_time_sec --the arrival time of the current trip should be later than the departure time of the previous trip
							--BUT compare departure times only for subway/Green Line terminals and Park Street (in both directions)
						--, but not by more than 30 minutes, as determined by the next statement
						AND 
							CASE
								WHEN y.cde_route_type <>3 THEN 2700
								ELSE 3600
							END >= y.c_time_sec - x.d_time_sec
		) temp
	WHERE rn = 1

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
				,ROW_NUMBER() OVER (PARTITION BY y.record_id ORDER BY x.event_time_sec DESC) AS rn
			
			FROM dbo.daily_event y --y is the "current" trip
				
				JOIN dbo.daily_event x --x is the most recent "previous" trip

					ON

							y.event_type IN ('DEP','PRD','EXD')
						AND 
							x.event_type IN ('DEP','PRD','EXD')
						AND 
							y.service_date = x.service_date
						AND 
							y.stop_id = x.stop_id
						AND 
							y.direction_id = x.direction_id
						AND 
							y.vehicle_id <> x.vehicle_id
						AND 
							y.trip_id <> x.trip_id
						--AND y.route_id =x.route_id
						AND 
							y.event_time_sec >= x.event_time_sec --Green Line at park can have two with exactly the same time
						AND 
							CASE
								WHEN y.route_type <>3 THEN 2700
								ELSE 3600
							END >= y.event_time_sec - x.event_time_sec
			WHERE
					y.suspect_record = 0
				AND 
					x.suspect_record = 0
		) temp
	WHERE rn = 1

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
				,ROW_NUMBER() OVER (PARTITION BY y.record_id ORDER BY x.event_time_sec DESC) AS rn
			FROM
				(
					SELECT d.*, st.checkpoint_id
					FROM dbo.daily_event d
					LEFT JOIN dbo.daily_stop_times_sec st
						ON 
								d.service_date = st.service_date
							AND 
								d.route_id = st.route_id
							AND 
								d.direction_id = st.direction_id
							AND 
								d.trip_id = st.trip_id
							AND 
								d.stop_id = st.stop_id
							AND 
								d.route_type = st.route_type
							AND 
								d.stop_sequence = st.stop_sequence
				) y --y is the most recent "previous" trip
				JOIN
				(
					SELECT d.*, st.checkpoint_id
					FROM dbo.daily_event d
					LEFT JOIN dbo.daily_stop_times_sec st
						ON 
								d.service_date = st.service_date
							AND 
								d.route_id = st.route_id
							AND 
								d.direction_id = st.direction_id
							AND 
								d.trip_id = st.trip_id
							AND 
								d.stop_id = st.stop_id
							AND 
								d.route_type = st.route_type
							AND 
								d.stop_sequence = st.stop_sequence
				) x --x is the "current" trip
					ON
							y.event_type IN ('DEP','PRD','EXD')
						AND 
							x.event_type IN ('DEP','PRD','EXD')
						AND 
							y.service_date = x.service_date
						AND 
							CASE
								WHEN y.route_type = 3 AND @use_checkpoints_only = 0 AND y.stop_id = x.stop_id THEN 1
								WHEN y.route_type = 3 AND @use_checkpoints_only = 1 AND y.checkpoint_id = x.checkpoint_id THEN 1
								WHEN y.route_type <> 3 AND y.stop_id = x.stop_id THEN 1
								ELSE 0
							END = 1
						AND 
							y.direction_id = x.direction_id
						--AND y.vehicle_id <> x.vehicle_id
						AND 
							CASE
								WHEN y.route_type <>3 AND y.route_id <> 'Mattapan' THEN y.vehicle_id
								ELSE '0'
							END <> x.vehicle_id
						AND 
							y.trip_id <> x.trip_id
						AND 
							y.route_type = x.route_type
						AND 
							y.route_id = x.route_id --for routes that are the same
						AND 
							y.event_time_sec >= x.event_time_sec --Green Line at park can have two with exactly the same time
						AND 
							CASE
								WHEN y.route_type <>3 THEN 2700
								ELSE 3600
							END >= y.event_time_sec - x.event_time_sec
			WHERE 
					y.suspect_record = 0
				AND 
					x.suspect_record = 0
		) temp
	WHERE rn = 1

	IF OBJECT_ID('tempdb..##daily_ac_sr_same_time','u') IS NOT NULL
		DROP TABLE ##daily_ac_sr_same_time

	CREATE TABLE ##daily_ac_sr_same_time
	(
		service_date		VARCHAR(255)	NOT NULL
		,ac_stop_id			VARCHAR(255)	NOT NULL
		,a_stop_sequence	INT				NOT NULL
		,c_stop_sequence	INT				NOT NULL
		,ac_route_id		VARCHAR(255)	NOT NULL
		,ac_route_type		INT				NOT NULL
		,ac_direction_id	INT				NOT NULL
		,a_trip_id			VARCHAR(255)	NOT NULL
		,c_trip_id			VARCHAR(255)	NOT NULL
		,a_vehicle_id		VARCHAR(255)	NOT NULL
		,c_vehicle_id		VARCHAR(255)	NOT NULL
		,a_record_id		INT				NOT NULL
		,c_record_id		INT				NOT NULL
		,a_time_sec			INT				NOT NULL
		,c_time_sec			INT				NOT NULL
		,ac_time_sec		INT				NOT NULL
	)
	;

	INSERT INTO ##daily_ac_sr_same_time
	(
		service_date
		,ac_stop_id
		,a_stop_sequence
		,c_stop_sequence
		,ac_route_id
		,ac_route_type
		,ac_direction_id
		,a_trip_id
		,c_trip_id
		,a_vehicle_id
		,c_vehicle_id
		,a_record_id
		,c_record_id
		,a_time_sec
		,c_time_sec
		,ac_time_sec
	)
	
	SELECT
		service_date
		,ac_stop_id
		,a_stop_sequence
		,c_stop_sequence
		,ac_route_id
		,ac_route_type
		,ac_direction_id
		,a_trip_id
		,c_trip_id
		,a_vehicle_id
		,c_vehicle_id
		,a_record_id
		,c_record_id
		,a_time_sec
		,c_time_sec
		,c_time_sec - a_time_sec AS ac_time_sec
	FROM
	(
		SELECT
			d.service_date
			,d.stop_id AS ac_stop_id
			,CASE
				WHEN @use_checkpoints_only = 0 OR d.route_type <> 3 THEN LAG(d.stop_sequence, 1) OVER (PARTITION BY d.service_date, d.route_id, d.direction_id, d.stop_id ORDER BY d.event_time_sec)
				WHEN @use_checkpoints_only = 1 AND d.route_type = 3 THEN LAG(d.stop_sequence, 1) OVER (PARTITION BY d.service_date, d.route_id, d.direction_id, st.checkpoint_id ORDER BY d.event_time_sec)
			END AS a_stop_sequence
			,d.stop_sequence AS c_stop_sequence
			,d.route_id AS ac_route_id
			,d.route_type AS ac_route_type
			,d.direction_id AS ac_direction_id
			,CASE
				WHEN @use_checkpoints_only = 0 OR d.route_type <> 3 THEN LAG(d.trip_id, 1) OVER (PARTITION BY d.service_date, d.route_id, d.direction_id, d.stop_id ORDER BY d.event_time_sec)
				WHEN @use_checkpoints_only = 1 AND d.route_type = 3 THEN LAG(d.trip_id, 1) OVER (PARTITION BY d.service_date, d.route_id, d.direction_id, st.checkpoint_id ORDER BY d.event_time_sec)
			END AS a_trip_id
			,d.trip_id AS c_trip_id
			,CASE
				WHEN @use_checkpoints_only = 0 OR d.route_type <> 3 THEN LAG(d.vehicle_id, 1) OVER (PARTITION BY d.service_date, d.route_id, d.direction_id, d.stop_id ORDER BY d.event_time_sec)
				WHEN @use_checkpoints_only = 1 AND d.route_type = 3 THEN LAG(d.vehicle_id, 1) OVER (PARTITION BY d.service_date, d.route_id, d.direction_id, st.checkpoint_id ORDER BY d.event_time_sec)
			END AS a_vehicle_id
			,d.vehicle_id AS c_vehicle_id
			,CASE
				WHEN @use_checkpoints_only = 0 OR d.route_type <> 3 THEN LAG(d.record_id, 1) OVER (PARTITION BY d.service_date, d.route_id, d.direction_id, d.stop_id ORDER BY d.event_time_sec)
				WHEN @use_checkpoints_only = 1 AND d.route_type = 3 THEN LAG(d.record_id, 1) OVER (PARTITION BY d.service_date, d.route_id, d.direction_id, st.checkpoint_id ORDER BY d.event_time_sec)
			END AS a_record_id
			,d.record_id AS c_record_id
			,CASE
				WHEN @use_checkpoints_only = 0 OR d.route_type <> 3 THEN LAG(d.event_time_sec, 1) OVER (PARTITION BY d.service_date, d.route_id, d.direction_id, d.stop_id ORDER BY d.event_time_sec)
				WHEN @use_checkpoints_only = 1 AND d.route_type = 3 THEN LAG(d.event_time_sec, 1) OVER (PARTITION BY d.service_date, d.route_id, d.direction_id, st.checkpoint_id ORDER BY d.event_time_sec)
			END AS a_time_sec
			,d.event_time_sec AS c_time_sec
			,st.checkpoint_id
			,d.route_type
		FROM
			dbo.daily_event d
				LEFT JOIN dbo.daily_stop_times_sec st
					ON
							d.service_date = st.service_date 
						AND 
							d.route_id = st.route_id
						AND 
							d.direction_id = st.direction_id
						AND 
							d.trip_id = st.trip_id 
						AND 
							d.stop_id = st.stop_id
						AND 
							d.route_type = st.route_type
						AND 
							d.stop_sequence = st.stop_sequence
						AND 
							d.event_type IN ('ARR','PRA','EXA')
						AND 
							d.suspect_record = 0
	) t
	WHERE
			CASE
				WHEN route_type <> 3 THEN 2700
				ELSE 3600
			END >= c_time_sec - a_time_sec
		AND 
			CASE 
				WHEN @use_checkpoints_only = 1 AND route_type = 3 THEN checkpoint_id 
				ELSE '0'
			END IS NOT NULL

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
		,checkpoint_id				VARCHAR(255)
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
		,checkpoint_id
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
		,ds.checkpoint_id
	FROM
		dbo.daily_stop_times_sec ds
		,dbo.daily_event rea
	WHERE
			rea.event_type IN ('ARR','PRA','EXA')
		AND 
			rea.service_date = ds.service_date
		AND 
			rea.trip_id = ds.trip_id
		AND 
			rea.stop_id = ds.stop_id
		AND 
			rea.stop_sequence = ds.stop_sequence
		AND 
			rea.suspect_record = 0

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
		,checkpoint_id					VARCHAR(255)
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
		,checkpoint_id
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
		,ds.checkpoint_id
	FROM
		dbo.daily_stop_times_sec ds
		,dbo.daily_event red
	WHERE
			red.event_type IN ('DEP','PRD','EXD')
		AND 
			red.service_date = ds.service_date
		AND 
			red.trip_id = ds.trip_id
		AND 
			red.stop_id = ds.stop_id
		AND 
			red.stop_sequence = ds.stop_sequence
		AND 
			red.suspect_record = 0

	--------------------------------------------------------------------schedule adherence inputs part ends---------------------------------------------------


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
		,time_period_id									VARCHAR(255)	NOT NULL
		,time_period_type								VARCHAR(255)	NOT NULL
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
		,time_period_id
		,time_period_type				 			   
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
		,ttt.time_period_id
		,ttt.time_period_type					  				
		,ttt.threshold_id AS threshold_id
		,ttt.threshold_historical_median_travel_time_sec AS threshold_historical_median_travel_time_sec
		,ttt.threshold_scheduled_median_travel_time_sec AS threshold_scheduled_median_travel_time_sec
		,ttt.threshold_historical_average_travel_time_sec AS threshold_historical_average_travel_time_sec
		,ttt.threshold_scheduled_average_travel_time_sec AS threshold_scheduled_average_travel_time_sec
		,(abcde.d_time_sec - abcde.b_time_sec) * par.passenger_arrival_rate AS denominator_pax
		,CASE 
			WHEN((abcde.e_time_sec - abcde.d_time_sec) - ttt.threshold_historical_median_travel_time_sec > 0) THEN  (abcde.d_time_sec - abcde.b_time_sec) * par.passenger_arrival_rate
			WHEN((abcde.e_time_sec - abcde.d_time_sec) - ttt.threshold_historical_median_travel_time_sec <= 0) THEN 0
			ELSE 0
		END AS historical_threshold_numerator_pax
		,CASE 
			WHEN((abcde.e_time_sec - abcde.d_time_sec) - ttt.threshold_scheduled_average_travel_time_sec > 0) THEN  (abcde.d_time_sec - abcde.b_time_sec) * par.passenger_arrival_rate
			WHEN((abcde.e_time_sec - abcde.d_time_sec) - ttt.threshold_scheduled_average_travel_time_sec <= 0) THEN 0
			ELSE 0
		END AS scheduled_threshold_numerator_pax 
	FROM
		##daily_abcde_time abcde
		JOIN dbo.config_time_slice ts
			ON
					abcde.e_time_sec >= ts.time_slice_start_sec
				AND 
					abcde.e_time_sec < ts.time_slice_end_sec
		JOIN dbo.service_date sd
			ON 
				abcde.service_date = sd.service_date
		LEFT JOIN dbo.config_passenger_arrival_rate par --changed to LEFT JOIN to still count trip-stops with no passenger rates
			ON
					par.day_type_id = sd.day_type_id
				AND 
					ts.time_slice_id = par.time_slice_id
				AND 
					abcde.abcd_stop_id = par.from_stop_id
				AND 
					abcde.e_stop_id = par.to_stop_id
		JOIN dbo.daily_travel_time_threshold ttt
			ON
					abcde.service_date = ttt.service_date
				AND 
					abcde.abcde_direction_id = ttt.direction_id
				AND 
					abcde.abcd_stop_id = ttt.from_stop_id
				AND 
					abcde.e_stop_id = ttt.to_stop_id
				AND 
					ts.time_slice_id = ttt.time_slice_id
				AND 
					ttt.route_type IN (0,1) --(ttt.route_type = 1 OR ttt.route_type = 0) --subway and green line passenger weighted numbers
				AND 
					abcde.cde_route_id = ttt.route_id --added for multiple routes visiting the same stops, green line

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
		,dtt.time_period_id
		,dtt.time_period_type					  					
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
	FROM
		##daily_de_time dat
		JOIN dbo.config_time_slice ts
			ON
					dat.e_time_sec >= ts.time_slice_start_sec
				AND 
					dat.e_time_sec < ts.time_slice_end_sec
		JOIN dbo.service_date sd
			ON 
				dat.service_date = sd.service_date
		JOIN dbo.daily_travel_time_threshold dtt
			ON
					dat.service_date = dtt.service_date
				AND 
					dat.de_direction_id = dtt.direction_id
				AND 
					dat.de_trip_id = dat.de_trip_id
				AND 
					dat.d_stop_id = dtt.from_stop_id
				AND 
					dat.e_stop_id = dtt.to_stop_id
				AND 
					ts.time_slice_id = dtt.time_slice_id
				AND 
					dtt.route_type = 2 --commuter rail passenger weighted numbers
				AND 
					dat.de_route_id = dtt.route_id --added for multiple routes visiting the same stops
		LEFT JOIN dbo.config_passenger_od_load_CR po --changed to LEFT JOIN to still count trip-stops with no passenger rates
			ON
					dat.de_trip_id = po.trip_id
				AND 
					dat.d_stop_id = po.from_stop_id
				AND 
					dat.e_stop_id = po.to_stop_id

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
		,time_period_id								VARCHAR(255)	NOT NULL
		,time_period_type							VARCHAR(255)	NOT NULL											  									   
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
		,time_period_id
		,time_period_type				 			   
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
		,wtt.time_period_id
		,wtt.time_period_type					  					
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
			WHEN ((abcde.c_time_sec - abcde.b_time_sec) - wtt.threshold_scheduled_average_wait_time_sec > 0) THEN ((abcde.c_time_sec - abcde.b_time_sec) - wtt.threshold_scheduled_average_wait_time_sec) * par.passenger_arrival_rate
			WHEN ((abcde.c_time_sec - abcde.b_time_sec) - wtt.threshold_scheduled_average_wait_time_sec <= 0) THEN 0
			ELSE 0
		END AS scheduled_threshold_numerator_pax

	FROM 
		##daily_abcde_time abcde
		JOIN dbo.config_time_slice ts
			ON
					abcde.d_time_sec >= ts.time_slice_start_sec
				AND 
					abcde.d_time_sec < ts.time_slice_end_sec
		JOIN dbo.service_date sd
			ON 
				abcde.service_date = sd.service_date
		LEFT JOIN dbo.config_passenger_arrival_rate par --changed to LEFT JOIN to still count trips where no pax rates
			ON
					par.day_type_id = sd.day_type_id -- will need to account for exceptions
				AND 
					ts.time_slice_id = par.time_slice_id
				AND 
					abcde.abcd_stop_id = par.from_stop_id
				AND 
					abcde.e_stop_id = par.to_stop_id
		JOIN dbo.daily_wait_time_od_threshold wtt
			ON
					abcde.service_date = wtt.service_date
				AND 
					abcde.abcde_direction_id = wtt.direction_id
				AND 
					abcde.abcd_stop_id = wtt.stop_id
				AND 
					abcde.e_stop_id = wtt.to_stop_id
				AND 
					ts.time_slice_id = wtt.time_slice_id
				AND 
					wtt.route_type IN (0,1) --(wtt.route_type = 1 OR wtt.route_type = 0) --subway and green line passenger weighted numbers only
	--save headway trip metrics

	IF OBJECT_ID('dbo.daily_headway_time_threshold_trip','U') IS NOT NULL
		DROP TABLE dbo.daily_headway_time_threshold_trip
	
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
		,time_period_id									VARCHAR(255)	NOT NULL
		,time_period_type								VARCHAR(255)	NOT NULL											   											
		,threshold_id									VARCHAR(255)	NOT NULL
		,threshold_id_lower								VARCHAR(255)	NULL
		,threshold_id_upper								VARCHAR(255)	NULL
		,threshold_lower_scheduled_median_headway_time_sec	INT			NULL
		,threshold_upper_scheduled_median_headway_time_sec	INT			NULL
		,threshold_lower_scheduled_average_headway_time_sec	INT			NULL
		,threshold_upper_scheduled_average_headway_time_sec	INT			NULL
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
		,time_period_id
		,time_period_type				 			   
		,threshold_id
		,threshold_id_lower
		,threshold_id_upper
		,threshold_lower_scheduled_median_headway_time_sec
		,threshold_upper_scheduled_median_headway_time_sec
		,threshold_lower_scheduled_average_headway_time_sec
		,threshold_upper_scheduled_average_headway_time_sec
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
		,wtt.time_period_id
		,wtt.time_period_type					  					
		,wtt.threshold_id AS threshold_id
		,wtt.threshold_id_lower
		,wtt.threshold_id_upper
		,wtt.threshold_lower_scheduled_median_headway_time_sec --AS threshold_scheduled_median_headway_time_sec
		,wtt.threshold_upper_scheduled_median_headway_time_sec
		,wtt.threshold_lower_scheduled_average_headway_time_sec --AS threshold_scheduled_average_headway_time_sec
		,wtt.threshold_upper_scheduled_average_headway_time_sec
		,1 AS denominator_trip
		,CASE
			WHEN threshold_lower_scheduled_average_headway_time_sec IS NOT NULL AND threshold_upper_scheduled_average_headway_time_sec IS NOT NULL 
				THEN
					CASE 
						WHEN (bd.d_time_sec - bd.b_time_sec) <= threshold_lower_scheduled_average_headway_time_sec OR (bd.d_time_sec - bd.b_time_sec) > threshold_upper_scheduled_average_headway_time_sec THEN 1
						ELSE 0
					END
			WHEN threshold_lower_scheduled_average_headway_time_sec IS NULL AND threshold_upper_scheduled_average_headway_time_sec IS NOT NULL
				THEN 
					CASE
						WHEN (bd.d_time_sec - bd.b_time_sec) > threshold_upper_scheduled_average_headway_time_sec THEN 1
						ELSE 0
					END
			WHEN threshold_lower_scheduled_average_headway_time_sec IS NOT NULL AND threshold_upper_scheduled_average_headway_time_sec IS NULL
				THEN
					CASE
						WHEN (bd.d_time_sec - bd.b_time_sec) < threshold_lower_scheduled_average_headway_time_sec THEN 1
						ELSE 0
					END
			ELSE 0
		END AS scheduled_threshold_numerator_trip
	FROM
		##daily_bd_sr_all_time bd
		JOIN dbo.config_time_slice ts
			ON
					bd.d_time_sec >= ts.time_slice_start_sec
				AND 
					bd.d_time_sec < ts.time_slice_end_sec
		JOIN dbo.service_date sd
			ON 
				bd.service_date = sd.service_date
		JOIN dbo.daily_headway_time_threshold wtt
			ON
					bd.service_date = wtt.service_date
				AND 
					bd.bd_direction_id = wtt.direction_id
				AND 
					bd.bd_stop_id = wtt.stop_id
				AND 
					ts.time_slice_id = wtt.time_slice_id
				AND 
					(wtt.route_type = 1) --subway numbers only
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
		,checkpoint_id					VARCHAR(255)
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
		,checkpoint_id
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
		,da.checkpoint_id
	FROM
		#daily_arrival_time_sec da
		FULL OUTER JOIN #daily_departure_time_sec dd
			ON
					da.service_date = dd.service_date
				AND 
					da.route_id = dd.route_id
				AND 
					da.trip_id = dd.trip_id
				AND 
					da.stop_id = dd.stop_id
				AND 
					da.vehicle_id = dd.vehicle_id
	WHERE da.stop_order_flag IN (1,2) --TO NOT DOUBLE-COUNT ENDPOINTS

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
		,checkpoint_id
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
		,da.checkpoint_id
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
		,checkpoint_id						VARCHAR(255)
		,threshold_id						VARCHAR(255)	NOT NULL
		,threshold_id_lower					VARCHAR(255)
		,threshold_id_upper					VARCHAR(255)
		,threshold_value_lower				INT
		,threshold_value_upper				INT
		,time_period_type					VARCHAR(255)
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
		,checkpoint_id
		,threshold_id
		,threshold_id_lower
		,threshold_id_upper
		,threshold_value_lower
		,threshold_value_upper
		,time_period_type
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
		,sad.checkpoint_id
		,th.threshold_id
		,th.threshold_id_lower
		,th.threshold_id_upper
		,thc1.add_to as threshold_value_lower
		,thc2.add_to as threshold_value_upper
		,ctp.time_period_type as time_period_type
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
	FROM
		daily_schedule_adherence_disaggregate sad
		JOIN dbo.service_date sd
			ON 
				sad.service_date = sd.service_date
		CROSS JOIN (
			SELECT
				ct.threshold_id
				,ct.threshold_name
				,ct.threshold_type
				,ct1.threshold_id as threshold_id_lower
				,ct2.threshold_id as threshold_id_upper
			FROM
				config_threshold ct
				LEFT JOIN config_threshold ct1
					ON
							ct.threshold_id = 
								CASE 
									WHEN ct1.parent_child = 0 THEN ct1.threshold_id
									WHEN ct1.parent_child = 2 THEN ct1.parent_threshold_id
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
			WHERE ct.parent_child <> 2
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
		--Added to determine metrics category (headway- or schedule-based)
		JOIN dbo.daily_stop_times_sec cap
			ON
				sad.service_date = cap.service_date
				AND sad.route_type = cap.route_type
				AND sad.route_id = cap.route_id
				AND sad.direction_id = cap.direction_id
				AND sad.trip_id = cap.trip_id
				AND sad.stop_id = cap.stop_id
		JOIN dbo.daily_stop_times_headway_same_sec dsth
			ON
				cap.service_date = dsth.service_date
				AND cap.route_type = dsth.route_type
				AND cap.route_id = dsth.route_id
				AND cap.direction_id = dsth.direction_id
				AND cap.trip_id = dsth.cd_trip_id
				AND cap.stop_id = dsth.cd_stop_id
		JOIN dbo.service_date s
		ON
			s.service_date = cap.service_date
		JOIN dbo.config_time_period ctp
		ON
				cap.departure_time_sec >= ctp.time_period_start_time_sec
			AND
				cap.departure_time_sec < ctp.time_period_end_time_sec
			AND
				ctp.day_type_id = s.day_type_id	
	WHERE
			sad.route_type = 3 --bus only
		AND 
			th.threshold_type = 'wait_time_schedule_based'
		--Added to determine metrics category (headway- or schedule-based)
		AND ((@use_checkpoints_only = 1 AND cap.checkpoint_id IS NOT NULL) OR @use_checkpoints_only = 0)
		AND ((cap.route_id NOT IN (SELECT route_id FROM @kbr) AND dsth.scheduled_arrival_headway_time_sec > 900) OR cap.trip_order = 1) --For schedule-based trips

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
		,checkpoint_id
		,th.threshold_id
		,th.threshold_id_lower
		,th.threshold_id_upper
		,thc1.add_to as threshold_value_lower
		,thc2.add_to as threshold_value_upper
		,NULL as time_period_type
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
	FROM
		daily_schedule_adherence_disaggregate sad
		LEFT JOIN dbo.config_passenger_od_load_CR po --left join to include trip-stops where there are no pax rates
			ON
					sad.route_id = po.route_id
				AND 
					sad.trip_id = po.trip_id
				AND 
					sad.stop_id = po.from_stop_id
		CROSS JOIN (
			SELECT
				ct.threshold_id
				,ct.threshold_name
				,ct.threshold_type
				,ct1.threshold_id as threshold_id_lower
				,ct2.threshold_id as threshold_id_upper
			FROM
				config_threshold ct
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
			WHERE ct.parent_child <> 2		
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
		,route_id										VARCHAR(255)	NOT NULL
		,route_type										INT				NOT NULL
		,direction_id									INT				NOT NULL
		,trip_id										VARCHAR(255)	NOT NULL
		,stop_id										VARCHAR(255)	NOT NULL
		,stop_order_flag								INT				NOT NULL
		,checkpoint_id									VARCHAR(255)
		,start_time_sec									INT				NOT NULL
		,end_time_sec									INT				NOT NULL
		,actual_headway_time_sec						INT				
		,scheduled_headway_time_sec						INT
		,scheduled_arrival_time_sec						INT
		,scheduled_departure_time_sec					INT
		,threshold_id									VARCHAR(255)	NOT NULL
		,threshold_id_lower								VARCHAR(255)	NULL
		,threshold_id_upper								VARCHAR(255)	NULL
		,threshold_value_lower							VARCHAR(255)	NULL
		,threshold_value_upper							VARCHAR(255)	NULL
		,time_period_type								VARCHAR(255)
		,denominator_pax								FLOAT			NULL
		,scheduled_threshold_numerator_pax				FLOAT			NULL
	)
	
	CREATE NONCLUSTERED INDEX IX_daily_headway_adherence_threshold_pax_stop_id ON daily_headway_adherence_threshold_pax (stop_id);
	
	CREATE NONCLUSTERED INDEX IX_daily_headway_adherence_threshold_pax_route_id ON daily_headway_adherence_threshold_pax (route_id);
	
	CREATE NONCLUSTERED INDEX IX_daily_headway_adherence_threshold_pax_direction_id ON daily_headway_adherence_threshold_pax (direction_id);

	INSERT INTO daily_headway_adherence_threshold_pax
	(
		service_date
		,route_id
		,route_type
		,direction_id
		,trip_id
		,stop_id
		,stop_order_flag
		,checkpoint_id
		,start_time_sec
		,end_time_sec
		,actual_headway_time_sec
		,scheduled_headway_time_sec
		,scheduled_arrival_time_sec
		,scheduled_departure_time_sec
		,threshold_id
		,threshold_id_lower
		,threshold_id_upper
		,threshold_value_lower
		,threshold_value_upper
		,time_period_type
		,denominator_pax
		,scheduled_threshold_numerator_pax
	)
	
	SELECT
		CASE 
			WHEN st.cd_pickup_type = 0 THEN acbd.bd_service_date 
			ELSE acbd.ac_service_date
		END AS service_date
		,CASE 
			WHEN st.cd_pickup_type = 0 THEN acbd.bd_route_id 
			ELSE acbd.ac_route_id
		END AS route_id
		,CASE 
			WHEN st.cd_pickup_type = 0 THEN acbd.bd_route_type
			ELSE acbd.ac_route_type
		END AS route_type
		,CASE 
			WHEN st.cd_pickup_type = 0 THEN acbd.bd_direction_id
			ELSE acbd.ac_direction_id
		END AS direction_id
		,CASE 
			WHEN st.cd_pickup_type = 0 THEN acbd.d_trip_id
			ELSE acbd.c_trip_id
		END AS trip_id
		,CASE 
			WHEN st.cd_pickup_type = 0 THEN acbd.bd_stop_id
			ELSE acbd.ac_stop_id
		END AS stop_id
		,st.cd_stop_order_flag
		,st.checkpoint_id
		,CASE 
			WHEN st.cd_pickup_type = 0 THEN acbd.b_time_sec
			ELSE acbd.a_time_sec
		END AS start_time_sec
		,CASE 
			WHEN st.cd_pickup_type = 0 THEN acbd.d_time_sec
			ELSE acbd.c_time_sec
		END AS end_time_sec
		,CASE 
			WHEN st.cd_pickup_type = 0 THEN acbd.bd_time_sec
			ELSE acbd.ac_time_sec 
		END AS actual_headway_time_sec
		,CASE 
			WHEN st.cd_pickup_type = 0 THEN st.scheduled_departure_headway_time_sec
			ELSE st.scheduled_arrival_headway_time_sec
		END AS scheduled_headway_time_sec
		,st.c_time_sec AS scheduled_arrival_time_sec
		,st.d_time_sec AS scheduled_departure_time_sec
		,th.threshold_id
		,th.threshold_id_lower
		,th.threshold_id_upper
		,CASE
			WHEN st.cd_pickup_type = 0 THEN st.scheduled_departure_headway_time_sec * thc1.multiply_by + thc1.add_to
			ELSE st.scheduled_arrival_headway_time_sec * thc1.multiply_by + thc1.add_to
		END as threshold_value_lower
		,CASE
			WHEN st.cd_pickup_type = 0 THEN st.scheduled_departure_headway_time_sec * thc2.multiply_by + thc2.add_to
			ELSE st.scheduled_arrival_headway_time_sec * thc2.multiply_by + thc2.add_to
		END as threshold_value_upper
		,ctp.time_period_type as time_period_type
		,1 as denominator_pax
		,CASE
			WHEN st.cd_pickup_type = 0 THEN
				CASE
					WHEN acbd.bd_time_sec NOT BETWEEN ISNULL(st.scheduled_departure_headway_time_sec * thc1.multiply_by + thc1.add_to, acbd.bd_time_sec) 
							AND ISNULL(st.scheduled_departure_headway_time_sec * thc2.multiply_by + thc2.add_to, acbd.bd_time_sec) THEN 1 --par.passenger_arrival_rate
					WHEN acbd.bd_time_sec BETWEEN ISNULL(st.scheduled_departure_headway_time_sec * thc1.multiply_by + thc1.add_to, acbd.bd_time_sec) 
							AND ISNULL(st.scheduled_departure_headway_time_sec * thc2.multiply_by + thc2.add_to, acbd.bd_time_sec) THEN 0 --par.passenger_arrival_rate
				END
			WHEN st.cd_pickup_type <> 0 THEN
				CASE
					WHEN acbd.ac_time_sec NOT BETWEEN ISNULL(st.scheduled_arrival_headway_time_sec * thc1.multiply_by + thc1.add_to, acbd.ac_time_sec) 
							AND ISNULL(st.scheduled_arrival_headway_time_sec * thc2.multiply_by + thc2.add_to, acbd.ac_time_sec) THEN 1 --par.passenger_arrival_rate
					WHEN acbd.ac_time_sec BETWEEN ISNULL(st.scheduled_arrival_headway_time_sec * thc1.multiply_by + thc1.add_to, acbd.ac_time_sec) 
							AND ISNULL(st.scheduled_arrival_headway_time_sec * thc2.multiply_by + thc2.add_to, acbd.ac_time_sec) THEN 0 --par.passenger_arrival_rate
				END
		END as scheduled_threshold_numerator_pax
	FROM
		(
			SELECT
				bd.service_date as bd_service_date
				,bd.bd_stop_id
				,bd.b_stop_sequence
				,bd.d_stop_sequence
				,bd.bd_route_id
				,bd.bd_route_type
				,bd.bd_direction_id
				,bd.b_trip_id
				,bd.d_trip_id
				,bd.b_vehicle_id
				,bd.d_vehicle_id
				,bd.b_record_id
				,bd.d_record_id
				,bd.b_time_sec
				,bd.d_time_sec
				,bd.bd_time_sec
				,ac.service_date as ac_service_date
				,ac.ac_stop_id
				,ac.a_stop_sequence
				,ac.c_stop_sequence
				,ac.ac_route_id
				,ac.ac_route_type
				,ac.ac_direction_id
				,ac.a_trip_id
				,ac.c_trip_id
				,ac.a_vehicle_id
				,ac.c_vehicle_id
				,ac.a_record_id
				,ac.c_record_id
				,ac.a_time_sec
				,ac.c_time_sec
				,ac.ac_time_sec
			FROM
				##daily_bd_sr_same_time bd
				FULL OUTER JOIN ##daily_ac_sr_same_time ac
					ON 
							bd.service_date = ac.service_date
						AND 
							bd.bd_route_type = ac.ac_route_type
						AND 
							bd.bd_route_id = ac.ac_route_id
						AND 
							bd.bd_direction_id = ac.ac_direction_id
						AND 
							bd.d_trip_id = ac.c_trip_id
						AND 
							bd.d_stop_sequence = ac.c_stop_sequence
						AND 
							bd.bd_stop_id = ac.ac_stop_id
						AND 
							bd.d_vehicle_id = ac.c_vehicle_id
		) acbd
		JOIN dbo.service_date sd
			ON 
					acbd.bd_service_date = sd.service_date
				OR 
					acbd.ac_service_date = sd.service_date
		JOIN dbo.daily_stop_times_headway_same_sec st
			ON
					(acbd.bd_service_date = st.service_date OR acbd.ac_service_date = st.service_date)
				AND 
					(acbd.d_trip_id = st.cd_trip_id OR acbd.c_trip_id = st.cd_trip_id)
				AND 
					(acbd.bd_stop_id = st.cd_stop_id OR acbd.ac_stop_id = st.cd_stop_id)
				AND 
					(acbd.d_stop_sequence = st.cd_stop_sequence OR acbd.c_stop_sequence = st.cd_stop_sequence)
				AND 
					(
						(st.cd_pickup_type = 0 AND acbd.bd_time_sec IS NOT NULL) 
					OR 
						(st.cd_pickup_type <> 0 and acbd.ac_time_sec IS NOT NULL)
					)
		CROSS JOIN 
			(
				SELECT
					ct.threshold_id
					,ct.threshold_name
					,ct.threshold_type
					,ct1.threshold_id as threshold_id_lower
					,ct1.min_max_equal as min_max_equal_lower
					,ct2.threshold_id as threshold_id_upper
					,ct2.min_max_equal as min_max_equal_upper
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
				WHERE ct.parent_child <> 2			
			) th
		JOIN config_stop_order_flag_threshold sth
			ON
					st.cd_stop_order_flag = sth.stop_order_flag
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
					(mt.route_type = acbd.bd_route_type OR mt.route_type = acbd.ac_route_type)
		LEFT JOIN config_passenger_arrival_rate par
			ON
					par.day_type_id = sd.day_type_id
				AND 
					(acbd.bd_stop_id = par.from_stop_id OR acbd.ac_stop_id = par.from_stop_id)
		--Added to determine metrics category (headway- or schedule-based)
		JOIN dbo.daily_stop_times_sec cap
			ON
				cap.service_date = st.service_date
				AND cap.route_type = st.route_type
				AND cap.route_id = st.route_id
				AND cap.direction_id = st.direction_id
				AND cap.trip_id = st.cd_trip_id
				AND cap.stop_id = st.cd_stop_id
				AND cap.stop_order_flag = st.cd_stop_order_flag
				AND (cap.checkpoint_id = st.checkpoint_id OR (cap.checkpoint_id IS NULL AND st.checkpoint_id IS NULL))
		JOIN dbo.service_date s
		ON
			s.service_date = cap.service_date
		JOIN dbo.config_time_period ctp
		ON
				cap.departure_time_sec >= ctp.time_period_start_time_sec
			AND
				cap.departure_time_sec < ctp.time_period_end_time_sec
			AND
				ctp.day_type_id = s.day_type_id						   
	WHERE
			(acbd.bd_route_type = 3 OR acbd.ac_route_type = 3 )--bus only
		AND 
			th.threshold_type = 'wait_time_headway_based'
		--Added to determine metrics category (headway- or schedule-based)
		AND ((@use_checkpoints_only = 1 AND cap.checkpoint_id IS NOT NULL) OR @use_checkpoints_only = 0)
		AND (cap.route_id IN (SELECT route_id FROM @kbr) OR st.scheduled_arrival_headway_time_sec <= 900)
		AND cap.trip_order <> 1

	--Create table for travel time adherence weighted by passengers and trips ----
	-- add pax numbers later ---
	IF OBJECT_ID('dbo.daily_trip_run_time_adherence_threshold_pax','U') IS NOT NULL
		DROP TABLE dbo.daily_trip_run_time_adherence_threshold_pax

	CREATE TABLE dbo.daily_trip_run_time_adherence_threshold_pax
	(
		service_date									VARCHAR(255)	NOT NULL
		,route_id										VARCHAR(255)	NOT NULL
		,route_type										INT				NOT NULL
		,direction_id									INT				NOT NULL
		,trip_id										VARCHAR(255)	NOT NULL
		,stop_id										VARCHAR(255)	NOT NULL
		,stop_order_flag								INT				NOT NULL
		,checkpoint_id									VARCHAR(255)
		,start_time_sec									INT				NOT NULL
		,end_time_sec									INT				NOT NULL
		,actual_run_time_sec							INT				NOT NULL
		,scheduled_run_time_sec							INT				NOT NULL
		,scheduled_arrival_time_sec						INT
		,scheduled_departure_time_sec					INT
		,threshold_id									VARCHAR(255)	NOT NULL
		,threshold_id_lower								VARCHAR(255)	NULL
		,threshold_id_upper								VARCHAR(255)	NULL
		,threshold_value_lower							VARCHAR(255)	NULL
		,threshold_value_upper							VARCHAR(255)	NULL
		,time_period_type								VARCHAR(255)
		,denominator_pax								FLOAT			NULL
		,scheduled_threshold_numerator_pax				FLOAT			NULL
	)
	
	CREATE NONCLUSTERED INDEX IX_daily_travel_time_adherence_threshold_pax_route_id ON daily_trip_run_time_adherence_threshold_pax (route_id);
	
	CREATE NONCLUSTERED INDEX IX_daily_travel_time_adherence_threshold_pax_direction_id ON daily_trip_run_time_adherence_threshold_pax (direction_id);

	INSERT INTO daily_trip_run_time_adherence_threshold_pax
	(
		service_date
		,route_id
		,route_type
		,direction_id
		,trip_id
		,stop_id
		,stop_order_flag
		,checkpoint_id
		,start_time_sec
		,end_time_sec
		,actual_run_time_sec
		,scheduled_run_time_sec
		,scheduled_arrival_time_sec
		,scheduled_departure_time_sec
		,threshold_id
		,threshold_id_lower
		,threshold_id_upper
		,threshold_value_lower
		,threshold_value_upper
		,time_period_type
		,denominator_pax
		,scheduled_threshold_numerator_pax
	)
	
	SELECT
		de.service_date AS service_date
		,de.de_route_id AS route_id
		,de.de_route_type AS route_type
		,de.de_direction_id AS direction_id
		,de.de_trip_id AS trip_id
		,st.stop_id AS stop_id
		,st.stop_order_flag AS stop_order_flag
		,st.checkpoint_id AS checkpoint_id
		,de.d_time_sec AS start_time_sec
		,de.e_time_sec AS end_time_sec
		,de.de_time_sec as actual_run_time_sec
		,st.trip_end_time_sec - st.trip_start_time_sec as scheduled_run_time_sec
		,st.arrival_time_sec AS scheduled_arrival_time_sec
		,st.departure_time_sec AS scheduled_departure_time_sec
		,th.threshold_id
		,th.threshold_id_lower
		,th.threshold_id_upper
		,(st.trip_end_time_sec - st.trip_start_time_sec) * thc1.multiply_by + thc1.add_to as threshold_value_lower
		,(st.trip_end_time_sec - st.trip_start_time_sec) * thc2.multiply_by + thc2.add_to as threshold_value_upper
		,ctp.time_period_type as time_period_type
		,1 as denominator_pax
		,CASE
			WHEN de.de_time_sec NOT BETWEEN ISNULL((st.trip_end_time_sec - st.trip_start_time_sec) * thc1.multiply_by + thc1.add_to,de.de_time_sec)
					AND ISNULL((st.trip_end_time_sec - st.trip_start_time_sec) * thc2.multiply_by + thc2.add_to, de.de_time_sec) THEN 1 --par.passenger_arrival_rate
			WHEN de.de_time_sec BETWEEN ISNULL((st.trip_end_time_sec - st.trip_start_time_sec) * thc1.multiply_by + thc1.add_to,de.de_time_sec)
					AND ISNULL((st.trip_end_time_sec - st.trip_start_time_sec) * thc2.multiply_by + thc2.add_to, de.de_time_sec) THEN 0 --par.passenger_arrival_rate
		END as scheduled_threshold_numerator_pax
	FROM
		##daily_de_time de
		JOIN dbo.service_date sd
			ON 
				de.service_date = sd.service_date
		LEFT JOIN daily_stop_times_sec st
			ON
					de.service_date = st.service_date
				AND 
					de.de_route_id = st.route_id
				AND 
					de.de_route_type = st.route_type
				AND 
					de.de_direction_id = st.direction_id
				AND 
					de.de_trip_id = st.trip_id
				AND 
					de.d_stop_id = st.trip_first_stop_id
				AND 
					de.d_stop_sequence = st.trip_first_stop_sequence
				AND 
					de.e_stop_id = st.trip_last_stop_id
				AND 
					de.e_stop_sequence = st.trip_last_stop_sequence
				AND 
					de.e_stop_id = st.stop_id
				AND 
					de.e_stop_sequence = st.stop_sequence
		CROSS JOIN
		(
			SELECT
				ct.threshold_id
				,ct.threshold_name
				,ct.threshold_type
				,ct1.threshold_id as threshold_id_lower
				,ct1.min_max_equal as min_max_equal_lower
				,ct2.threshold_id as threshold_id_upper
				,ct2.min_max_equal as min_max_equal_upper
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
			WHERE ct.parent_child <> 2
		) th
		JOIN config_stop_order_flag_threshold sth
			ON
					st.stop_order_flag = sth.stop_order_flag
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
					mt.route_type = de.de_route_type
		--Added to determine metrics category (headway- or schedule-based)
		JOIN dbo.daily_stop_times_headway_same_sec dsth
			ON
				st.service_date = dsth.service_date
				AND st.route_type = dsth.route_type
				AND st.route_id = dsth.route_id
				AND st.direction_id = dsth.direction_id
				AND st.trip_id = dsth.cd_trip_id
				AND st.stop_id = dsth.cd_stop_id
		JOIN dbo.service_date s
		ON
			s.service_date = st.service_date
		JOIN dbo.config_time_period ctp
		ON
				st.arrival_time_sec >= ctp.time_period_start_time_sec
			AND
				st.arrival_time_sec < ctp.time_period_end_time_sec
			AND
				ctp.day_type_id = s.day_type_id				   
	WHERE
			de.de_route_type = 3 --bus only
		AND 
			th.threshold_type = 'travel_time'
		--Added to determine metrics category (headway- or schedule-based)
		AND ((@use_checkpoints_only = 1 AND st.checkpoint_id IS NOT NULL) OR @use_checkpoints_only = 0)
		AND (st.route_id IN (SELECT route_id FROM @kbr) OR dsth.scheduled_arrival_headway_time_sec <= 900)
		AND st.trip_order <> 1

	EXEC ExcessJourneyTimeUsingAB  
			@service_date_process

	EXEC ExcessJourneyTimeUsingCD   
			@service_date_process			
	
	EXEC ExcessJourneyTime
			@service_date_process		

	--save daily metrics for each route	
	IF OBJECT_ID('dbo.daily_metrics','U') IS NOT NULL
		DROP TABLE dbo.daily_metrics
	
	CREATE TABLE dbo.daily_metrics
	(
		route_id			VARCHAR(255)	NOT NULL
		,threshold_id		VARCHAR(255)	NOT NULL
		,threshold_name		VARCHAR(255)	NOT NULL
		,threshold_type		VARCHAR(255)	NOT NULL
		,time_period_type	VARCHAR(255)									  
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
		VALUES 
		('Red'),('Blue'),('Orange'),('Green-B'),('Green-C'),('Green-D'),('Green-E')																			 
		,('CR-Fairmount'),('CR-Fitchburg'),('CR-Franklin'),('CR-Greenbush'),('CR-Haverhill'),('CR-Kingston'),('CR-Lowell'),('CR-Middleborough')
		,('CR-Needham'),('CR-Newburyport'),('CR-Providence'),('CR-Worcester'),('712'),('713')

	INSERT INTO dbo.daily_metrics
	(
		route_id
		,threshold_id
		,threshold_name
		,threshold_type
		,time_period_type		   
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
		,dwt.time_period_type												   
		,1 - SUM(scheduled_threshold_numerator_pax) / SUM(denominator_pax) AS metric_result
		,NULL
		,SUM(scheduled_threshold_numerator_pax) AS numerator_pax
		,SUM(denominator_pax) AS denominator_pax
		,NULL
		,NULL
		
	FROM
		dbo.daily_wait_time_od_threshold_pax dwt
		,dbo.config_threshold ct
	WHERE
			ct.threshold_id = dwt.threshold_id
		AND 
			ct.parent_child = 0
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
		,dwt.time_period_type

	UNION

	SELECT
		route_id
		,ct.threshold_id
		,ct.threshold_name
		,ct.threshold_type
		,dtt.time_period_type					
		,1 - SUM(scheduled_threshold_numerator_pax) / SUM(denominator_pax) AS metric_result
		,NULL
		,SUM(scheduled_threshold_numerator_pax) AS numerator_pax
		,SUM(denominator_pax) AS denominator_pax
		,NULL
		,NULL
	FROM
		dbo.daily_travel_time_threshold_pax dtt
		,dbo.config_threshold ct
	WHERE
			ct.threshold_id = dtt.threshold_id
		AND	
			ct.parent_child = 0
		AND 
			(
					(SELECT COUNT(stop_id) FROM @from_stop_ids) = 0
				OR 
					from_stop_id IN (SELECT stop_id FROM @from_stop_ids)
			)
		AND 
			(
					(SELECT COUNT(stop_id) FROM @to_stop_ids) = 0
				OR 
					to_stop_id IN (SELECT stop_id FROM @to_stop_ids)
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
 
	GROUP BY
		route_id
		,ct.threshold_id
		,ct.threshold_name
		,ct.threshold_type
		,dtt.time_period_type
	
	UNION
		
	SELECT
		route_id
		,ct.threshold_id
		,ct.threshold_name
		,ct.threshold_type
		,NULL
		,1-SUM(scheduled_threshold_numerator_pax)/SUM(denominator_pax) AS metric_result 
		,NULL
		,SUM(scheduled_threshold_numerator_pax) AS numerator_pax 
		,SUM(denominator_pax) AS denominator_pax 
		,NULL
		,NULL
	
	FROM
		dbo.daily_schedule_adherence_threshold_pax cap
		,dbo.config_threshold ct
	WHERE
			ct.threshold_id = cap.threshold_id
		AND 
			ct.parent_child = 0
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
			
	UNION
	
	SELECT
		route_id
		,ct.threshold_id
		,ct.threshold_name
		,ct.threshold_type
		,dtt.time_period_type				
		,NULL
		,1 - SUM(scheduled_threshold_numerator_trip) / SUM(denominator_trip) AS metric_result_trip
		,NULL
		,NULL
		,SUM(scheduled_threshold_numerator_trip) AS numerator_trip
		,SUM(denominator_trip) AS denominator_trip
	FROM
		dbo.daily_headway_time_threshold_trip dtt
		,dbo.config_threshold ct
	WHERE
			ct.threshold_id = dtt.threshold_id
		AND 
			ct.parent_child = 0
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

	GROUP BY
		route_id
		,ct.threshold_id
		,ct.threshold_name
		,ct.threshold_type
		,dtt.time_period_type

	UNION

	SELECT
		route_id
		,ct.threshold_id
		,ct.threshold_name
		,ct.threshold_type
		,djt.time_period_type					
		,SUM(numerator_pax) / SUM(denominator_pax) AS metric_result
		,NULL
		,SUM(numerator_pax) AS numerator_pax
		,SUM(denominator_pax) AS denominator_pax
		,NULL
		,NULL
	FROM
		dbo.daily_journey_time_disaggregate_threshold_pax djt
		,dbo.config_threshold ct
	WHERE
			ct.threshold_id = djt.threshold_id
		AND	
			ct.parent_child = 0
		AND 
			(
					(SELECT COUNT(stop_id) FROM @from_stop_ids) = 0
				OR 
					from_stop_id IN (SELECT stop_id FROM @from_stop_ids)
			)
		AND 
			(
					(SELECT COUNT(stop_id) FROM @to_stop_ids) = 0
				OR 
					to_stop_id IN (SELECT stop_id FROM @to_stop_ids)
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

	GROUP BY
		route_id
		,ct.threshold_id
		,ct.threshold_name
		,ct.threshold_type
		,djt.time_period_type


	UNION
	
	--bus reliability metrics
	SELECT
		r.route_id
		,ct2.threshold_id
		,ct2.threshold_name
		,ct2.threshold_type
		,r.time_period_type			 
		,1 - SUM(r.scheduled_threshold_numerator_pax) / COUNT(*) AS metric_result
		,NULL AS metric_result_trip
		,SUM(r.scheduled_threshold_numerator_pax) AS numerator_pax
		,COUNT(*) as denominator_pax
		,NULL
		,NULL
	FROM
		(
			--For schedule threshold
			SELECT
				cap.service_date
				,cap.route_id
				,cap.direction_id
				,cap.trip_id
				,cap.stop_id
				,cap.checkpoint_id
				,cap.time_period_type
				,cap.threshold_id
				,cap.denominator_pax
				,cap.scheduled_threshold_numerator_pax
			FROM
				dbo.daily_schedule_adherence_threshold_pax cap
			WHERE
				((SELECT COUNT(stop_id) FROM @from_stop_ids) = 0 OR cap.stop_id IN (SELECT stop_id FROM @from_stop_ids))
				AND ((SELECT COUNT(direction_id) FROM @direction_ids) = 0 OR cap.direction_id IN (SELECT direction_id FROM @direction_ids))
				AND ((SELECT COUNT(route_id) FROM @route_ids) = 0 OR cap.route_id IN (SELECT route_id FROM @route_ids))
				AND cap.route_type = 3

			UNION

			--For headway threshold
			SELECT
				dh.service_date
				,dh.route_id
				,dh.direction_id
				,dh.trip_id
				,dh.stop_id
				,dh.checkpoint_id
				,dh.time_period_type		 
				,dh.threshold_id
				,dh.denominator_pax
				,dh.scheduled_threshold_numerator_pax
			FROM
				dbo.daily_headway_adherence_threshold_pax dh
			WHERE
				((SELECT COUNT(stop_id) FROM @from_stop_ids) = 0 OR dh.stop_id IN (SELECT stop_id FROM @from_stop_ids))
				AND ((SELECT COUNT(direction_id) FROM @direction_ids) = 0 OR dh.direction_id IN (SELECT direction_id FROM @direction_ids))
				AND ((SELECT COUNT(route_id) FROM @route_ids) = 0 OR dh.route_id IN (SELECT route_id FROM @route_ids))
				AND dh.route_type = 3 --Not needed, because already selected in creating table

			UNION

			--For travel time threshold
			SELECT
				dtt.service_date
				,dtt.route_id
				,dtt.direction_id
				,dtt.trip_id
				,dtt.stop_id
				,dtt.checkpoint_id
				,dtt.time_period_type		 
				,dtt.threshold_id
				,dtt.denominator_pax
				,dtt.scheduled_threshold_numerator_pax
			FROM
				dbo.daily_trip_run_time_adherence_threshold_pax dtt
			WHERE
				((SELECT COUNT(stop_id) FROM @from_stop_ids) = 0 OR dtt.stop_id IN (SELECT stop_id FROM @from_stop_ids))
				AND ((SELECT COUNT(direction_id) FROM @direction_ids) = 0 OR dtt.direction_id IN (SELECT direction_id FROM @direction_ids))
				AND ((SELECT COUNT(route_id) FROM @route_ids) = 0 OR dtt.route_id IN (SELECT route_id FROM @route_ids))
				AND dtt.route_type = 3 --Not needed, because already selected in creating table
		) r
		JOIN config_threshold ct1
			ON r.threshold_id = ct1.threshold_id
		JOIN config_threshold ct2
			ON ct1.parent_threshold_id = ct2.threshold_id
	GROUP BY
		r.route_id
		,ct2.threshold_id
		,ct2.threshold_type
		,ct2.threshold_name
		,r.time_period_type
	ORDER BY
		route_id, threshold_id

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
	FROM
		##daily_de_time htt
		JOIN dbo.config_time_slice ts
			ON
					htt.e_time_sec >= ts.time_slice_start_sec
				AND 
					htt.e_time_sec < ts.time_slice_end_sec
		JOIN dbo.daily_travel_time_benchmark dtb
			ON
					htt.de_direction_id = dtb.direction_id
				AND 
					htt.d_stop_id = dtb.from_stop_id
				AND 
					htt.e_stop_id = dtb.to_stop_id
				AND 
					htt.de_route_id = dtb.route_id --added because of green line
				AND 
					ts.time_slice_id = dtb.time_slice_id

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
	FROM
		##daily_abcde_time htt
		JOIN dbo.config_time_slice ts
			ON
					htt.d_time_sec >= ts.time_slice_start_sec
				AND 
					htt.d_time_sec < ts.time_slice_end_sec
		JOIN dbo.daily_headway_time_od_benchmark dtb
			ON
					htt.abcde_direction_id = dtb.direction_id
				AND 
					htt.abcd_stop_id = dtb.stop_id
				AND 
					htt.e_stop_id = dtb.to_stop_id
				AND 
					ts.time_slice_id = dtb.time_slice_id


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
	FROM
		##daily_bd_sr_all_time htt
		JOIN dbo.config_time_slice ts
			ON			
					htt.d_time_sec >= ts.time_slice_start_sec
				AND 
					htt.d_time_sec < ts.time_slice_end_sec
		JOIN dbo.daily_headway_time_sr_all_benchmark dtb
			ON
					htt.bd_direction_id = dtb.direction_id
				AND 
					htt.bd_stop_id = dtb.stop_id
				AND 
					ts.time_slice_id = dtb.time_slice_id

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
	FROM
		##daily_bd_sr_same_time htt
		JOIN dbo.config_time_slice ts
			ON
					htt.d_time_sec >= ts.time_slice_start_sec
				AND 
					htt.d_time_sec < ts.time_slice_end_sec
		JOIN dbo.daily_headway_time_sr_same_benchmark dtb
			ON
					htt.bd_route_id = dtb.route_id
				AND 
					htt.bd_direction_id = dtb.direction_id
				AND 
					htt.bd_stop_id = dtb.stop_id
				AND 
					ts.time_slice_id = dtb.time_slice_id


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
		,vehicle_label
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
		,vehicle_label
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
		,threshold_value_upper
		,denominator_pax
		,scheduled_threshold_numerator_pax
		,denominator_trip
		,scheduled_threshold_numerator_trip
		,threshold_value_lower
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
		,threshold_value_upper
		,denominator_pax
		,scheduled_threshold_numerator_pax
		,NULL
		,NULL
		,threshold_value_lower
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
		,time_period_id
		,time_period_type				 
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
		,time_period_id
		,time_period_type						  
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
		,time_period_id
		,time_period_type					 
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
		,time_period_id
		,time_period_type				  
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
		,threshold_upper_scheduled_median_headway_time_sec
		,threshold_upper_scheduled_average_headway_time_sec
		,denominator_trip
		,scheduled_threshold_numerator_trip
		,time_period_id
		,time_period_type
		,threshold_lower_scheduled_median_headway_time_sec
		,threshold_lower_scheduled_average_headway_time_sec	
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
		,threshold_upper_scheduled_median_headway_time_sec
		,threshold_upper_scheduled_average_headway_time_sec
		,denominator_trip
		,scheduled_threshold_numerator_trip
		,time_period_id
		,time_period_type
		,threshold_lower_scheduled_median_headway_time_sec
		,threshold_lower_scheduled_average_headway_time_sec		
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
		,time_period_type				   
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
		,dm.time_period_type					   

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

	IF
		(
			SELECT 
				COUNT(*)
			FROM dbo.historical_headway_adherence_threshold_pax
			WHERE 
				service_date = @service_date_process
		)
		> 0

		DELETE FROM dbo.historical_headway_adherence_threshold_pax
		WHERE 
			service_date = @service_date_process
	
	INSERT INTO dbo.historical_headway_adherence_threshold_pax
	(
		service_date
		,route_id
		,route_type
		,direction_id
		,trip_id
		,stop_id
		,stop_order_flag
		,checkpoint_id
		,start_time_sec
		,end_time_sec
		,actual_headway_time_sec
		,scheduled_headway_time_sec
		,threshold_id
		,threshold_id_lower
		,threshold_id_upper
		,threshold_value_lower
		,threshold_value_upper
		,denominator_pax
		,scheduled_threshold_numerator_pax
	)
	SELECT
		service_date
		,route_id
		,route_type
		,direction_id
		,trip_id
		,stop_id
		,stop_order_flag
		,checkpoint_id
		,start_time_sec
		,end_time_sec
		,actual_headway_time_sec
		,scheduled_headway_time_sec
		,threshold_id
		,threshold_id_lower
		,threshold_id_upper
		,threshold_value_lower
		,threshold_value_upper
		,denominator_pax
		,scheduled_threshold_numerator_pax
	FROM dbo.daily_headway_adherence_threshold_pax

	IF
	(
		SELECT
			COUNT(*)
		FROM dbo.historical_trip_run_time_adherence_threshold_pax
		WHERE 
			service_date = @service_date_process
	)
		> 0

		DELETE FROM dbo.historical_trip_run_time_adherence_threshold_pax
		WHERE 
			service_date = @service_date_process

	INSERT INTO dbo.historical_trip_run_time_adherence_threshold_pax
	(
		service_date
		,route_id
		,route_type
		,direction_id
		,trip_id
		,start_time_sec
		,end_time_sec
		,actual_run_time_sec
		,scheduled_run_time_sec
		,threshold_id
		,threshold_id_lower
		,threshold_id_upper
		,threshold_value_lower
		,threshold_value_upper
		,denominator_pax
		,scheduled_threshold_numerator_pax
	)
	SELECT
		service_date
		,route_id
		,route_type
		,direction_id
		,trip_id
		,start_time_sec
		,end_time_sec
		,actual_run_time_sec
		,scheduled_run_time_sec
		,threshold_id
		,threshold_id_lower
		,threshold_id_upper
		,threshold_value_lower
		,threshold_value_upper
		,denominator_pax
		,scheduled_threshold_numerator_pax
	FROM dbo.daily_trip_run_time_adherence_threshold_pax

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

	IF OBJECT_ID('tempdb..##daily_ac_sr_same_time','u') IS NOT NULL
		DROP TABLE ##daily_ac_sr_same_time

	IF OBJECT_ID('tempdb..#daily_arrival_time_sec','U') IS NOT NULL
		DROP TABLE #daily_arrival_time_sec

	IF OBJECT_ID('tempdb..#daily_departure_time_sec','U') IS NOT NULL
		DROP TABLE #daily_departure_time_sec

END
GO
