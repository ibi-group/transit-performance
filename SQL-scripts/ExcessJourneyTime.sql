
---run this script in the transit-performance database
--USE transit_performance
--GO

IF OBJECT_ID('ExcessJourneyTime','P') IS NOT NULL
	DROP PROCEDURE dbo.ExcessJourneyTime

GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE dbo.ExcessJourneyTime

	@service_date_process DATE

--Script Version: Master - 1.0.0.0	

AS

BEGIN
	SET NOCOUNT ON;
	EXEC ExcessJourneyTime  
			@service_date_process


IF OBJECT_ID('tempdb..##daily_journey_time_disaggregate_threshold', 'U') IS NOT NULL
DROP TABLE ##daily_journey_time_disaggregate_threshold

CREATE TABLE ##daily_journey_time_disaggregate_threshold
	(
		service_date									VARCHAR(255)
		,from_stop_id									VARCHAR(255)
		,to_stop_id										VARCHAR(255)
		,route_type										INT
		,route_id										VARCHAR(255)
		,direction_id									INT
		,trip_id										VARCHAR(255)
		,time_period_id									VARCHAR(255)
		,time_period_type								VARCHAR(255)
		,threshold_id									VARCHAR(255)
		,threshold_id_lower								VARCHAR(255)
		,threshold_id_upper								VARCHAR(255)
		,threshold_lower_sec							INT
		,threshold_upper_sec							INT
	)

INSERT INTO ##daily_journey_time_disaggregate_threshold
	(
		service_date
		,from_stop_id
		,to_stop_id
		,route_type
		,route_id
		,direction_id
		,trip_id
		,time_period_id
		,time_period_type
		,threshold_id
		,threshold_id_lower
		,threshold_id_upper
		,threshold_lower_sec
		,threshold_upper_sec
	)

SELECT DISTINCT
		abcde.service_date
		,abcde.abcd_stop_id 			AS from_stop_id
		,abcde.e_stop_id 				AS to_stop_id
		,r.route_type
		,abcde.cde_route_id 			AS route_id
		,abcde.abcde_direction_id 		AS direction_id
		,abcde.cde_trip_id				AS trip_id	
		,tp.time_period_id
		,tp.time_period_type
		,th.threshold_id
		,th.threshold_id_lower
		,th.threshold_id_upper
		,CASE
			WHEN th.min_max_equal = 'min' AND th.threshold_id_lower IS NOT NULL THEN thc1.add_to
			WHEN th.min_max_equal = 'max' AND th.threshold_id_lower IS NOT NULL THEN thc1.add_to
			WHEN th.min_max_equal = 'equal' AND th.threshold_id_lower IS NOT NULL THEN thc1.add_to
			ELSE NULL
		END AS threshold_lower_sec
		,CASE
			WHEN th.min_max_equal = 'min' AND th.threshold_id_upper IS NOT NULL THEN thc2.add_to
			WHEN th.min_max_equal = 'max' AND th.threshold_id_upper IS NOT NULL THEN thc2.add_to
			WHEN th.min_max_equal = 'equal' AND th.threshold_id_upper IS NOT NULL THEN thc2.add_to
			ELSE NULL
		END AS threshold_upper_sec
FROM	
	##daily_abcde_time abcde
	,(
		SELECT
			ct.threshold_id
			,ct.threshold_name
			,ct.threshold_type
			,ct.min_max_equal
			,ct1.threshold_id as threshold_id_lower
			,ct2.threshold_id as threshold_id_upper
		FROM
			dbo.config_threshold ct
			LEFT JOIN dbo.config_threshold ct1
				ON
						ct.threshold_id = 
							CASE 
								WHEN ct1.parent_child = 0 THEN ct1.threshold_id
								WHEN ct1.parent_child = 2 THEN ct1.parent_threshold_id
							END
					AND 
						ct1.upper_lower = 'lower'
			LEFT JOIN dbo.config_threshold ct2
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
	,dbo.config_threshold_calculation thc1
	,dbo.config_threshold_calculation thc2
	,dbo.config_mode_threshold mt
	,dbo.config_time_period tp
	,dbo.service_date sd
	,gtfs.routes r	
		 

	WHERE
		abcde.service_date = @service_date_process
		AND abcde.service_date = sd.service_date
		AND sd.day_type_id = tp.day_type_id
		AND abcde.c_time_sec >= tp.time_period_start_time_sec
		AND abcde.c_time_sec < tp.time_period_end_time_sec	
		AND abcde.cde_route_id = r.route_id	
		AND r.route_type = mt.route_type
		AND mt.threshold_id = th.threshold_id
		AND th.threshold_type = 'excess_journey_time'
		AND ISNULL(th.threshold_id_lower, th.threshold_id) = thc1.threshold_id
		AND ISNULL(th.threshold_id_upper, th.threshold_id) = thc2.threshold_id


	
IF OBJECT_ID('dbo.daily_journey_time_disaggregate_threshold_pax', 'U') IS NOT NULL
DROP TABLE dbo.daily_journey_time_disaggregate_threshold_pax

CREATE TABLE dbo.daily_journey_time_disaggregate_threshold_pax
	(
		service_date									VARCHAR(255)
		,from_stop_id									VARCHAR(255)
		,to_stop_id										VARCHAR(255)
		,route_type										INT
		,route_id										VARCHAR(255)
		,direction_id									INT
		,trip_id										VARCHAR(255)
		,expected_wait_time_sec							FLOAT
		,expected_in_vehicle_time_sec					FLOAT
		,expected_journey_time_sec						FLOAT
		,total_excess_wait_time_sec						FLOAT
		,total_excess_in_vehicle_time_sec				FLOAT
		,total_excess_journey_time_sec					FLOAT
		,total_expected_journey_time_sec				FLOAT
		,excess_journey_time_per_passenger_sec			FLOAT
		,maximum_wait_time_sec							INT
		,maximum_in_vehicle_time_sec					INT
		,maximum_journey_time_sec						INT
		,time_period_id									VARCHAR(255)
		,time_period_type								VARCHAR(255)
		,threshold_id									VARCHAR(255)
		,threshold_id_lower								VARCHAR(255)
		,threshold_id_upper								VARCHAR(255)
		,threshold_lower_sec							INT
		,threshold_upper_sec							INT
		,denominator_pax								FLOAT
		,numerator_pax									FLOAT
	)

INSERT INTO dbo.daily_journey_time_disaggregate_threshold_pax
	(
		service_date
		,from_stop_id
		,to_stop_id
		,route_type
		,route_id
		,direction_id
		,trip_id
		,expected_wait_time_sec
		,expected_in_vehicle_time_sec
		,expected_journey_time_sec
		,total_excess_wait_time_sec
		,total_excess_in_vehicle_time_sec
		,total_excess_journey_time_sec
		,total_expected_journey_time_sec
		,excess_journey_time_per_passenger_sec
		,maximum_wait_time_sec
		,maximum_in_vehicle_time_sec
		,maximum_journey_time_sec
		,time_period_id
		,time_period_type
		,threshold_id
		,threshold_id_lower
		,threshold_id_upper
		,threshold_lower_sec
		,threshold_upper_sec
		,denominator_pax
		,numerator_pax
	)

SELECT DISTINCT
	j.service_date
	,j.from_stop_id
	,j.to_stop_id
	,j.route_type
	,j.route_id
	,j.direction_id
	,trip_id
	,wt.expected_wait_time_sec																									AS expected_wait_time_sec
	,ivt.expected_in_vehicle_time_sec																							AS expected_in_vehicle_time_sec
	,(wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) 															AS expected_journey_time_sec
	,(			
		(
			(
				(c_time_sec - a_time_sec - wt.expected_wait_time_sec)
				+ (0 - wt.expected_wait_time_sec)
			) * 0.5
		) * (par.passenger_arrival_rate * (c_time_sec - a_time_sec))
	 ) 																															AS total_excess_wait_time_sec
	,(
		(e_time_sec - c_time_sec - ivt.expected_in_vehicle_time_sec)
		* (par.passenger_arrival_rate * (c_time_sec - a_time_sec))
	 )																															AS total_excess_in_vehicle_time_sec
	,CASE	
		WHEN 	(e_time_sec - a_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) <= 0					-- when max_excess_journey_time <= 0
		THEN 	0																												-- then total_excess_journey_time = 0
		WHEN 	(e_time_sec - a_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) > 0
		AND 	(e_time_sec - c_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) <= 0					-- when max_excess_journey_time > 0 and min_excess_journey_time <= 0
		THEN
				(
					(e_time_sec - a_time_sec) 
					- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
				)
				* 0.5																											-- average_excess_journey_time
				*
				(
					(e_time_sec - a_time_sec) 
					- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
				)
				* par.passenger_arrival_rate																					-- passengers_with_excess_journey_time
		ELSE																													-- else max_excess_journey_time > 0 and min_excess_journey_time > 0
				(
					(
						(e_time_sec - a_time_sec) 
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
					) 
					+ 
					(
						(e_time_sec - c_time_sec)
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
					)
				)
				* 0.5																											-- average_excess_journey_time
				*
				(
					(
						(e_time_sec - a_time_sec) 
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
					) 
					- 
					(
						(e_time_sec - c_time_sec)
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
					)
				) * par.passenger_arrival_rate																					-- passengers_with_excess_journey_time
		END																														AS total_excess_journey_time_sec
	,(wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) * (par.passenger_arrival_rate * (c_time_sec - a_time_sec))	AS total_expected_journey_time_sec
	,CASE	
		WHEN	(e_time_sec - a_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) <= 0					-- when max_excess_journey_time <= 0
		OR 		(par.passenger_arrival_rate * (c_time_sec - a_time_sec)) = 0	 
		THEN 	0																												-- then total_excess_journey_time = 0
		WHEN 	(e_time_sec - a_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) > 0
		AND 	(e_time_sec - c_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) <= 0					-- when max_excess_journey_time > 0 and min_excess_journey_time <= 0				
		THEN
				(
					(e_time_sec - a_time_sec) 
					- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
				)
				* 0.5																											-- average_excess_journey_time
				*
				(
					(e_time_sec - a_time_sec) 
					- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
				)
				* par.passenger_arrival_rate																					-- passengers_with_excess_journey_time
				/ (par.passenger_arrival_rate * (c_time_sec - a_time_sec))														--total_passengers
				/ 60
		ELSE																													-- else max_excess_journey_time > 0 and min_excess_journey_time <= 0
				(
					(
						(e_time_sec - a_time_sec) 
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
					) 
					- 
					(
						(e_time_sec - c_time_sec)
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
					)
				)
				* 0.5																											-- average_excess_journey_time
				*
				(
					(
						(e_time_sec - a_time_sec) 
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
					) 
					- 
					(
						(e_time_sec - c_time_sec)
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
					)
				) * par.passenger_arrival_rate																					-- passengers_with_excess_journey_time
				/ (par.passenger_arrival_rate * (c_time_sec - a_time_sec))														--total_passengers
				/ 60
		END																														AS excess_journey_time_per_passenger_sec
	,c_time_sec - a_time_sec																									AS maximum_wait_time_sec
	,e_time_sec - c_time_sec																									AS maximum_in_vehicle_time_sec
	,e_time_sec - a_time_sec																									AS maximum_journey_time_sec
	,j.time_period_id
	,j.time_period_type
	,j.threshold_id
	,j.threshold_id_lower
	,j.threshold_id_upper
	,j.threshold_lower_sec
	,j.threshold_upper_sec
	,par.passenger_arrival_rate * (c_time_sec - a_time_sec)																		AS denominator_pax
	,CASE
		WHEN threshold_lower_sec IS NOT NULL AND threshold_upper_sec IS NOT NULL 
			THEN
				(
					CASE	
						WHEN 	(e_time_sec - a_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - threshold_lower_sec <= 0		-- when max_excess_journey_time <= 0
						THEN 	0																												-- then passengers_with_excess_journey_time = 0
						WHEN 	(e_time_sec - a_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - threshold_lower_sec  > 0
						AND 	(e_time_sec - c_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - threshold_lower_sec  <= 0		-- when max_excess_journey_time > 0 and min_excess_journey_time <= 0
						THEN																											
								(
									(e_time_sec - a_time_sec) 
									- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)  - threshold_lower_sec 
								)
								* par.passenger_arrival_rate																					-- then passengers_with_excess_journey_time =  max_excess_journey_time * passenger_arrival_rate
						ELSE																													-- else max_excess_journey_time > 0 and min_excess_journey_time <= 0
								(
									(
										(e_time_sec - a_time_sec) 
										- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - threshold_lower_sec 
									) 
									- 
									(
										(e_time_sec - c_time_sec)
										- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - threshold_lower_sec 
									)
								) * par.passenger_arrival_rate																					-- and passengers_with_excess_journey_time = (max_excess_journey_time - min_excess_journey_time) * passenger_arrival_rate
						END
				)
				-
				(
					CASE	
						WHEN 	(e_time_sec - a_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - threshold_upper_sec <= 0		-- when max_excess_journey_time <= 0
						THEN 	0																												-- then passengers_with_excess_journey_time = 0
						WHEN 	(e_time_sec - a_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - threshold_upper_sec  > 0
						AND 	(e_time_sec - c_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - threshold_upper_sec  <= 0		-- when max_excess_journey_time > 0 and min_excess_journey_time <= 0
						THEN																											
								(
									(e_time_sec - a_time_sec) 
									- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - threshold_upper_sec 
								)
								* par.passenger_arrival_rate																					-- then passengers_with_excess_journey_time =  max_excess_journey_time * passenger_arrival_rate
						ELSE																													-- else max_excess_journey_time > 0 and min_excess_journey_time <= 0
								(
									(
										(e_time_sec - a_time_sec) 
										- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - threshold_upper_sec
									) 
									- 
									(
										(e_time_sec - c_time_sec)
										- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - threshold_upper_sec 
									)
								) * par.passenger_arrival_rate																					-- and passengers_with_excess_journey_time = (max_excess_journey_time - min_excess_journey_time) * passenger_arrival_rate
						END
					)						
		WHEN threshold_lower_sec IS NULL AND threshold_upper_sec IS NOT NULL
			THEN
				CASE	
					WHEN	(e_time_sec - a_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - threshold_upper_sec <= 0					-- when max_excess_journey_time <= 0
					THEN	(par.passenger_arrival_rate * (c_time_sec - a_time_sec))														-- then passengers_with_excess_journey_time = 0
					WHEN	(e_time_sec - a_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - threshold_upper_sec > 0
					AND		(e_time_sec - c_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - threshold_upper_sec <= 0					-- when max_excess_journey_time > 0 and min_excess_journey_time <= 0
					THEN
							(par.passenger_arrival_rate * (c_time_sec - a_time_sec))
							-
							(
								(
									(e_time_sec - a_time_sec) 
									- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - threshold_upper_sec
								)
							* par.passenger_arrival_rate
							)																												-- then passengers_with_excess_journey_time =  max_excess_journey_time * passenger_arrival_rate
					ELSE																													-- else max_excess_journey_time > 0 and min_excess_journey_time > 0
							(par.passenger_arrival_rate * (c_time_sec - a_time_sec))
							-
							(
								(
									(
										(e_time_sec - a_time_sec) 
										- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - threshold_upper_sec
									) 
									- 
									(
										(e_time_sec - c_time_sec)
										- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - threshold_upper_sec
									)
								) 
							* par.passenger_arrival_rate
							)																												-- and passengers_with_excess_journey_time = (max_excess_journey_time - min_excess_journey_time) * passenger_arrival_rate
					END																														
		WHEN threshold_lower_sec IS NOT NULL AND threshold_upper_sec IS NULL
			THEN
				CASE	
					WHEN 	(e_time_sec - a_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - threshold_lower_sec <= 0		-- when max_excess_journey_time <= 0
					THEN 	0																												-- then passengers_with_excess_journey_time = 0
					WHEN 	(e_time_sec - a_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - threshold_lower_sec  > 0
					AND 	(e_time_sec - c_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - threshold_lower_sec  <= 0		-- when max_excess_journey_time > 0 and min_excess_journey_time <= 0
					THEN																											
							(
								(e_time_sec - a_time_sec) 
								- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - threshold_lower_sec 
							)
							* par.passenger_arrival_rate																					-- then passengers_with_excess_journey_time =  max_excess_journey_time * passenger_arrival_rate
					ELSE																													-- else max_excess_journey_time > 0 and min_excess_journey_time <= 0
							(
								(
									(e_time_sec - a_time_sec) 
									- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - threshold_lower_sec
								) 
								- 
								(
									(e_time_sec - c_time_sec)
									- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - threshold_lower_sec 
								)
							) * par.passenger_arrival_rate																					-- and passengers_with_excess_journey_time = (max_excess_journey_time - min_excess_journey_time) * passenger_arrival_rate
					END	
		ELSE 0
	END

FROM	
	##daily_journey_time_disaggregate_threshold	j
	
LEFT JOIN ##daily_abcde_time abcde
ON
		abcde.service_date = j.service_date
		AND abcde.abcd_stop_id = j.from_stop_id
		AND abcde.e_stop_id = j.to_stop_id
		AND abcde.cde_route_id = j.route_id
		AND abcde.abcde_direction_id = j.direction_id
		AND abcde.cde_trip_id = j.trip_id		
        
LEFT JOIN dbo.config_time_slice ts
ON
	abcde.c_time_sec >= ts.time_slice_start_sec
	AND abcde.c_time_sec < ts.time_slice_end_sec

LEFT JOIN dbo.service_date sd
ON 
	abcde.service_date = sd.service_date

LEFT JOIN dbo.config_passenger_arrival_rate_b_branch_consolidated par
ON
	par.day_type_id = sd.day_type_id
	AND ts.time_slice_id = par.time_slice_id
	AND abcde.abcd_stop_id = par.from_stop_id
	AND abcde.e_stop_id = par.to_stop_id
			
JOIN gtfs.routes r
ON	
	abcde.cde_route_id = r.route_id
		
LEFT JOIN dbo.config_expected_wait_time wt
ON
	abcde.abcd_stop_id = wt.from_stop_id
	AND abcde.e_stop_id = wt.to_stop_id
	AND ts.time_slice_id = wt.time_slice_id
	AND sd.day_type_id = wt.day_type_id
		
LEFT JOIN dbo.config_expected_in_vehicle_time ivt
ON	
	sd.day_type_id = ivt.day_type_id
	AND abcde.abcd_stop_id = ivt.from_stop_id
	AND abcde.e_stop_id = ivt.to_stop_id
	AND r.route_type = ivt.route_type
	AND abcde.cde_route_id = ivt.route_id
	AND abcde.abcde_direction_id = ivt.direction_id
	AND ts.time_slice_id = ivt.time_slice_id


IF
	(
		SELECT 
			COUNT(*)
		FROM 
			dbo.historical_journey_time_disaggregate_threshold_pax
		WHERE 
			service_date = @service_date_process
	)
		> 0

DELETE FROM dbo.historical_journey_time_disaggregate_threshold_pax
WHERE 
	service_date = @service_date_process	
	

INSERT INTO dbo.historical_journey_time_disaggregate_threshold_pax
	(	
		service_date
		,from_stop_id
		,to_stop_id
		,route_type
		,route_id
		,direction_id
		,trip_id
		,expected_wait_time_sec
		,expected_in_vehicle_time_sec
		,expected_journey_time_sec
		,total_excess_wait_time_sec
		,total_excess_in_vehicle_time_sec
		,total_excess_journey_time_sec
		,total_expected_journey_time_sec
		,excess_journey_time_per_passenger_sec
		,maximum_wait_time_sec
		,maximum_in_vehicle_time_sec
		,maximum_journey_time_sec
		,time_period_id
		,time_period_type
		,threshold_id
		,threshold_id_lower
		,threshold_id_upper
		,threshold_lower_sec
		,threshold_upper_sec
		,denominator_pax
		,numerator_pax
	)	
	
SELECT
		service_date
		,from_stop_id
		,to_stop_id
		,route_type
		,route_id
		,direction_id
		,trip_id
		,expected_wait_time_sec
		,expected_in_vehicle_time_sec
		,expected_journey_time_sec
		,total_excess_wait_time_sec
		,total_excess_in_vehicle_time_sec
		,total_excess_journey_time_sec
		,total_expected_journey_time_sec
		,excess_journey_time_per_passenger_sec
		,maximum_wait_time_sec
		,maximum_in_vehicle_time_sec
		,maximum_journey_time_sec
		,time_period_id
		,time_period_type
		,threshold_id
		,threshold_id_lower
		,threshold_id_upper
		,threshold_lower_sec
		,threshold_upper_sec
		,denominator_pax
		,numerator_pax
FROM
	dbo.daily_journey_time_disaggregate_threshold_pax
	
	
END