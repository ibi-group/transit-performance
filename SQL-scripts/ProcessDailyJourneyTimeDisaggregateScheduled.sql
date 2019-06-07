
---run this script in the transit-performance database
--USE transit_performance
--GO

--This procedure processes all of the scheduled events for the service_date being processed. It runs during PreProcessDaily.

IF OBJECT_ID('dbo.ProcessDailyJourneyTimeDisaggregateScheduled ','P') IS NOT NULL
	DROP PROCEDURE dbo.ProcessDailyJourneyTimeDisaggregateScheduled

GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE dbo.ProcessDailyJourneyTimeDisaggregateScheduled

	@service_date_process DATE

AS

BEGIN
	SET NOCOUNT ON;

IF OBJECT_ID('dbo.daily_journey_time_disaggregate_scheduled', 'U') IS NOT NULL
	DROP TABLE dbo.daily_journey_time_disaggregate_scheduled


CREATE TABLE dbo.daily_journey_time_disaggregate_scheduled
	(
	service_date													VARCHAR(255)	
	,from_stop_id													VARCHAR(255)	
	,to_stop_id														VARCHAR(255)	
	--,to_stop_sequence												INT				-- temporary, for validation only				
	,route_type														INT				
	,route_id														VARCHAR(255)	
	,direction_id													INT				
	,trip_id														VARCHAR(255)	
	,expected_wait_time_sec											FLOAT				
	,expected_in_vehicle_time_sec									FLOAT				
	,expected_journey_time_sec										FLOAT				
	--,passenger_arrival_rate										FLOAT			-- temporary, for validation only
	--,bc_passengers												FLOAT			-- temporary, for validation only
	--,bc_max_wait_time												FLOAT			-- temporary, for validation only
	--,bc_min_wait_time												FLOAT			-- temporary, for validation only
	--,bc_max_excess_wait_time										FLOAT			-- temporary, for validation only
	--,bc_min_excess_wait_time										FLOAT			-- temporary, for validation only
	--,bc_in_vehicle_time											FLOAT			-- temporary, for validation only	
	--,bc_excess_in_vehicle_time									FLOAT			-- temporary, for validation only
	--,bc_total_excess_wait_time									FLOAT			-- temporary, for validation only
	--,bc_total_excess_in_vehicle_time								FLOAT			-- temporary, for validation only	
	--,cd_passengers												FLOAT			-- temporary, for validation only
	--,cd_wait_time													FLOAT			-- temporary, for validation only
	--,cd_excess_wait_time											FLOAT			-- temporary, for validation only
	--,cd_max_in_vehicle_time										FLOAT			-- temporary, for validation only
	--,cd_min_in_vehicle_time										FLOAT			-- temporary, for validation only
	--,cd_max_excess_in_vehicle_time								FLOAT			-- temporary, for validation only
	--,cd_min_excess_in_vehicle_time								FLOAT			-- temporary, for validation only
	--,cd_total_excess_wait_time									FLOAT			-- temporary, for validation only
	--,cd_total_excess_in_vehicle_time								FLOAT			-- temporary, for validation only
	--,max_journey_time												FLOAT			-- temporary, for validation only
	--,min_journey_time												FLOAT			-- temporary, for validation only
	--,max_excess_journey_time										FLOAT			-- temporary, for validation only
	--,min_excess_journey_time										FLOAT			-- temporary, for validation only
	,total_excess_wait_time_sec										FLOAT
	,total_excess_in_vehicle_time_sec								FLOAT
	,total_excess_journey_time_sec									FLOAT
	,total_expected_journey_time_sec								FLOAT
	,excess_journey_time_per_passenger_sec							FLOAT
	,passengers														FLOAT
	,passengers_with_excess_journey_time							FLOAT
	,passengers_with_excess_journey_time_greater_than_five_min		FLOAT
	,passengers_with_excess_journey_time_greater_than_ten_min		FLOAT
	,passengers_with_excess_journey_time_greater_than_fifteen_min	FLOAT
	,passengers_with_excess_journey_time_greater_than_twenty_min	FLOAT
	)

INSERT INTO dbo.daily_journey_time_disaggregate_scheduled
	(
	service_date
	,from_stop_id
	,to_stop_id
	--,to_stop_sequence
	,route_type
	,route_id
	,direction_id
	,trip_id
	,expected_wait_time_sec
	,expected_in_vehicle_time_sec
	,expected_journey_time_sec
	-- ,passenger_arrival_rate
	-- ,bc_passengers
	-- ,bc_max_wait_time
	-- ,bc_min_wait_time
	-- ,bc_max_excess_wait_time
	-- ,bc_min_excess_wait_time
	-- ,bc_in_vehicle_time
	-- ,bc_excess_in_vehicle_time
	-- ,bc_total_excess_wait_time
	-- ,bc_total_excess_in_vehicle_time	
	-- ,cd_passengers
	-- ,cd_wait_time
	-- ,cd_excess_wait_time
	-- ,cd_max_in_vehicle_time
	-- ,cd_min_in_vehicle_time
	-- ,cd_max_excess_in_vehicle_time
	-- ,cd_min_excess_in_vehicle_time
	-- ,cd_total_excess_wait_time
	-- ,cd_total_excess_in_vehicle_time	
	-- ,max_journey_time
	-- ,min_journey_time
	-- ,max_excess_journey_time
	-- ,min_excess_journey_time
	,total_excess_wait_time_sec
	,total_excess_in_vehicle_time_sec
	,total_excess_journey_time_sec
	,total_expected_journey_time_sec
	,excess_journey_time_per_passenger_sec
	,passengers
	,passengers_with_excess_journey_time
	,passengers_with_excess_journey_time_greater_than_five_min
	,passengers_with_excess_journey_time_greater_than_ten_min
	,passengers_with_excess_journey_time_greater_than_fifteen_min
	,passengers_with_excess_journey_time_greater_than_twenty_min
	
	)

SELECT
	abcde.service_date
	,abcd_stop_id 																												AS from_stop_id
	,e_stop_id 																													AS to_stop_id
	--,e_stop_sequence																											AS to_stop_sequence
	,r.route_type
	,cde_route_id 																												AS route_id
	,abcde_direction_id 																										AS direction_id
	,cde_trip_id																												AS trip_id
	,wt.expected_wait_time_sec																									AS expected_wait_time_sec
	,ivt.expected_in_vehicle_time_sec																							AS expected_in_vehicle_time_sec
	,(wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) 															AS expected_journey_time_sec
	-- ,par.passenger_arrival_rate
	-- ,(par.passenger_arrival_rate * (c_time_sec - b_time_sec)) 																AS bc_passengers
	-- ,(c_time_sec - b_time_sec) 																								AS bc_max_wait_time
	-- ,0 																														AS bc_min_wait_time
	-- ,(c_time_sec - b_time_sec - wt.expected_wait_time) 																		AS bc_max_excess_wait_time
	-- ,(0 - wt.expected_wait_time) 																							AS bc_min_excess_wait_time
	-- ,(e_time_sec - c_time_sec) 																								AS bc_in_vehicle_time
	-- ,(e_time_sec - c_time_sec - ivt.expected_in_vehicle_time) 																AS bc_excess_in_vehicle_time
	-- ,( 
		-- (
			-- (
				-- (c_time_sec - b_time_sec - wt.expected_wait_time) 															-- bc_max_excess_wait_time
				-- + (0 - wt.expected_wait_time) 																				-- bc_min_excess_wait_time
			-- ) * 0.5
		-- ) * (par.passenger_arrival_rate * (c_time_sec - b_time_sec)) 														-- bc_passengers
	 -- )																														AS bc_total_excess_wait_time
	-- ,(
		-- (e_time_sec - c_time_sec - ivt.expected_in_vehicle_time) 															-- bc_excess_in_vehicle_time
		-- * (par.passenger_arrival_rate * (c_time_sec - b_time_sec)) 															-- bc_passengers
	 -- )																														AS bc_total_excess_in_vehicle_time	
	-- ,(par.passenger_arrival_rate * (d_time_sec - c_time_sec)) 																AS cd_passengers
	-- ,0																														AS cd_wait_time	
	-- ,(0 - wt.expected_wait_time)																								AS cd_excess_wait_time
	-- ,(e_time_sec - c_time_sec)																								AS cd_max_in_vehicle_time
	-- ,(e_time_sec - d_time_sec)																								AS cd_min_in_vehicle_time
	-- ,(e_time_sec - c_time_sec - ivt.expected_in_vehicle_time)																AS cd_max_excess_in_vehicle_time
	-- ,(e_time_sec - d_time_sec - ivt.expected_in_vehicle_time)																AS cd_min_excess_in_vehicle_time
	-- ,(
		-- (0 - wt.expected_wait_time)																							-- cd_excess_wait_time
		-- * (par.passenger_arrival_rate * (d_time_sec - c_time_sec)) 															-- cd_passengers
	 -- )																														AS cd_total_excess_wait_time
	-- ,( 
		-- (
			-- (
				-- (e_time_sec - c_time_sec - ivt.expected_in_vehicle_time)														-- cd_max_excess_in_vehicle_time
				-- + (e_time_sec - d_time_sec - ivt.expected_in_vehicle_time)													-- cd_min_excess_in_vehicle_time
			-- ) * 0.5
		-- ) * (par.passenger_arrival_rate * (d_time_sec - c_time_sec)) 														-- cd_passengers
	 -- )																														AS cd_total_excess_in_vehicle_time 
	
	
	
	-- ,(e_time_sec - b_time_sec)																								AS max_journey_time
	-- ,(e_time_sec - d_time_sec)																								AS min_journey_time
	-- ,(
		-- (e_time_sec - b_time_sec)																							-- max_journey_time
		-- - (wt.expected_wait_time + ivt.expected_in_vehicle_time) 															-- expected_journey_time
	 -- )																														AS max_excess_journey_time
	-- ,(
		-- (e_time_sec - d_time_sec)																							-- min_journey_time
		-- - (wt.expected_wait_time + ivt.expected_in_vehicle_time) 															-- expected_journey_time
	 -- )																														AS min_excess_journey_time

	,(			
		( 
			(
				(
					(c_time_sec - b_time_sec - wt.expected_wait_time_sec)
					+ (0 - wt.expected_wait_time_sec)
				) * 0.5
			) * (par.passenger_arrival_rate * (c_time_sec - b_time_sec))
		) 																														-- bc_total_excess_wait_time
		+
		(
			(0 - wt.expected_wait_time_sec)
			* (par.passenger_arrival_rate * (d_time_sec - c_time_sec))
		)																														-- cd_total_excess_wait_time
	 ) 																															AS total_excess_wait_time_sec
	,(
		(
			(e_time_sec - c_time_sec - ivt.expected_in_vehicle_time_sec)
			* (par.passenger_arrival_rate * (c_time_sec - b_time_sec))
		)																														-- bc_total_excess_in_vehicle_time
		+
		( 
			(
				(
					(e_time_sec - c_time_sec - ivt.expected_in_vehicle_time_sec)
					+ (e_time_sec - d_time_sec - ivt.expected_in_vehicle_time_sec)
				) * 0.5
			) * (par.passenger_arrival_rate * (d_time_sec - c_time_sec))
		)																														-- cd_total_excess_in_vehicle_time 
	 )																															AS total_excess_in_vehicle_time_sec
	,CASE	
		WHEN 
			(e_time_sec - b_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) <= 0						-- when max_excess_journey_time <= 0
				THEN 0																											-- then total_excess_journey_time = 0
		WHEN 
			(e_time_sec - b_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) > 0
			AND (e_time_sec - d_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) <= 0					-- when max_excess_journey_time > 0 and min_excess_journey_time <= 0
				THEN
					(
						(e_time_sec - b_time_sec) 
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
					)
					* 0.5																										-- average_excess_journey_time
					*
					(
						(e_time_sec - b_time_sec) 
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
					)
					* par.passenger_arrival_rate																				-- passengers_with_excess_journey_time
		ELSE																													-- else max_excess_journey_time > 0 and min_excess_journey_time <= 0
			(
				(
					(e_time_sec - b_time_sec) 
					- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
				) 
				- 
				(
					(e_time_sec - d_time_sec)
					- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
				)
			)
			* 0.5																												-- average_excess_journey_time
			*
			(
				(
					(e_time_sec - b_time_sec) 
					- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
				) 
				- 
				(
					(e_time_sec - d_time_sec)
					- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
				)
			) * par.passenger_arrival_rate																						-- passengers_with_excess_journey_time
		END																														AS total_excess_journey_time_sec
	,(wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) * (par.passenger_arrival_rate * (d_time_sec - b_time_sec))	AS total_expected_journey_time_sec
	,CASE	
		WHEN 
			(e_time_sec - b_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) <= 0						-- when max_excess_journey_time <= 0
			OR (par.passenger_arrival_rate * (d_time_sec - b_time_sec)) = 0	 
				THEN 0																											-- then total_excess_journey_time = 0
		WHEN 
			(e_time_sec - b_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) > 0
			AND (e_time_sec - d_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) <= 0					-- when max_excess_journey_time > 0 and min_excess_journey_time <= 0				
				THEN
					(
						(e_time_sec - b_time_sec) 
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
					)
					* 0.5																										-- average_excess_journey_time
					*
					(
						(e_time_sec - b_time_sec) 
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
					)
					* par.passenger_arrival_rate																				-- passengers_with_excess_journey_time
					/ (par.passenger_arrival_rate * (d_time_sec - b_time_sec))													--total_passengers
					/ 60
			ELSE																												-- else max_excess_journey_time > 0 and min_excess_journey_time <= 0
			(
				(
					(e_time_sec - b_time_sec) 
					- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
				) 
				- 
				(
					(e_time_sec - d_time_sec)
					- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
				)
			)
			* 0.5																												-- average_excess_journey_time
			*
			(
				(
					(e_time_sec - b_time_sec) 
					- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
				) 
				- 
				(
					(e_time_sec - d_time_sec)
					- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
				)
			) * par.passenger_arrival_rate																						-- passengers_with_excess_journey_time
			/ (par.passenger_arrival_rate * (d_time_sec - b_time_sec))															--total_passengers
		END																														AS excess_journey_time_per_passenger_sec
	,(par.passenger_arrival_rate * (d_time_sec - b_time_sec))																	AS passengers
	,CASE	
		WHEN 
			(e_time_sec - b_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) <= 0							-- when max_excess_journey_time <= 0
				THEN 0																										-- then passengers_with_excess_journey_time = 0
		WHEN 
			(e_time_sec - b_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) > 0
			AND(e_time_sec - d_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) <= 0						-- when max_excess_journey_time > 0 and min_excess_journey_time <= 0
				THEN																											
					(
						(e_time_sec - b_time_sec) 
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
					)
					* par.passenger_arrival_rate																				-- then passengers_with_excess_journey_time =  max_excess_journey_time * passenger_arrival_rate
		ELSE																													-- else max_excess_journey_time > 0 and min_excess_journey_time <= 0
			(
				(
					(e_time_sec - b_time_sec) 
					- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
				) 
				- 
				(
					(e_time_sec - d_time_sec)
					- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
				)
			) * par.passenger_arrival_rate																					-- and passengers_with_excess_journey_time = (max_excess_journey_time - min_excess_journey_time) * passenger_arrival_rate
		END																													AS passengers_with_excess_journey_time
	,CASE	
		WHEN 
			(e_time_sec - b_time_sec - (5*60)) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) <= 0				-- when max_excess_journey_time <= 0
				THEN 0																										-- then passengers_with_excess_journey_time = 0
		WHEN 
			(e_time_sec - b_time_sec - (5*60)) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) > 0
			AND(e_time_sec - d_time_sec - (5*60)) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) <= 0				-- when max_excess_journey_time > 0 and min_excess_journey_time <= 0
				THEN																											
					(
						(e_time_sec - b_time_sec - (5*60)) 
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
					)
					* par.passenger_arrival_rate																			-- then passengers_with_excess_journey_time =  max_excess_journey_time * passenger_arrival_rate
		ELSE																												-- else max_excess_journey_time > 0 and min_excess_journey_time <= 0
			(
				(
					(e_time_sec - b_time_sec - (5*60)) 
					- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
				) 
				- 
				(
					(e_time_sec - d_time_sec - (5*60))
					- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
				)
			) * par.passenger_arrival_rate																					-- and passengers_with_excess_journey_time = (max_excess_journey_time - min_excess_journey_time) * passenger_arrival_rate
		END																													AS passengers_with_excess_journey_time_greater_than_five_min		
	,CASE	
		WHEN 
			(e_time_sec - b_time_sec - (10*60)) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) <= 0				-- when max_excess_journey_time <= 0
				THEN 0																										-- then passengers_with_excess_journey_time = 0
		WHEN 
			(e_time_sec - b_time_sec - (10*60)) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) > 0
			AND(e_time_sec - d_time_sec - (10*60)) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) <= 0				-- when max_excess_journey_time > 0 and min_excess_journey_time <= 0
				THEN																											
					(
						(e_time_sec - b_time_sec - (10*60)) 
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
					)
					* par.passenger_arrival_rate																			-- then passengers_with_excess_journey_time =  max_excess_journey_time * passenger_arrival_rate
		ELSE																												-- else max_excess_journey_time > 0 and min_excess_journey_time <= 0
			(
				(
					(e_time_sec - b_time_sec - (10*60)) 
					- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
				) 
				- 
				(
					(e_time_sec - d_time_sec - (10*60))
					- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
				)
			) * par.passenger_arrival_rate																					-- and passengers_with_excess_journey_time = (max_excess_journey_time - min_excess_journey_time) * passenger_arrival_rate
		END																													AS passengers_with_excess_journey_time_greater_than_ten_min	
	,CASE	
		WHEN 
			(e_time_sec - b_time_sec - (15*60)) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) <= 0				-- when max_excess_journey_time <= 0
				THEN 0																										-- then passengers_with_excess_journey_time = 0
		WHEN 
			(e_time_sec - b_time_sec - (15*60)) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) > 0
			AND(e_time_sec - d_time_sec - (15*60)) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) <= 0				-- when max_excess_journey_time > 0 and min_excess_journey_time <= 0
				THEN																											
					(
						(e_time_sec - b_time_sec - (15*60)) 
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
					)
					* par.passenger_arrival_rate																			-- then passengers_with_excess_journey_time =  max_excess_journey_time * passenger_arrival_rate
		ELSE																												-- else max_excess_journey_time > 0 and min_excess_journey_time <= 0
			(
				(
					(e_time_sec - b_time_sec - (15*60)) 
					- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
				) 
				- 
				(
					(e_time_sec - d_time_sec - (15*60))
					- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
				)
			) * par.passenger_arrival_rate																					-- and passengers_with_excess_journey_time = (max_excess_journey_time - min_excess_journey_time) * passenger_arrival_rate
		END																													AS passengers_with_excess_journey_time_greater_than_fifteen_min			
	,CASE	
		WHEN 
			(e_time_sec - b_time_sec - (20*60)) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) <= 0				-- when max_excess_journey_time <= 0
				THEN 0																										-- then passengers_with_excess_journey_time = 0
		WHEN 
			(e_time_sec - b_time_sec - (20*60)) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) > 0
			AND(e_time_sec - d_time_sec - (20*60)) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) <= 0				-- when max_excess_journey_time > 0 and min_excess_journey_time <= 0
				THEN																											
					(
						(e_time_sec - b_time_sec - (20*60)) 
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
					)
					* par.passenger_arrival_rate																			-- then passengers_with_excess_journey_time =  max_excess_journey_time * passenger_arrival_rate
		ELSE																												-- else max_excess_journey_time > 0 and min_excess_journey_time <= 0
			(
				(
					(e_time_sec - b_time_sec - (20*60)) 
					- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
				) 
				- 
				(
					(e_time_sec - d_time_sec - (20*60))
					- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
				)
			) * par.passenger_arrival_rate																					-- and passengers_with_excess_journey_time = (max_excess_journey_time - min_excess_journey_time) * passenger_arrival_rate
		END																													AS passengers_with_excess_journey_time_greater_than_twenty_min		
FROM	
	#daily_abcde_time_scheduled abcde
        
		LEFT JOIN dbo.config_time_slice ts
		ON
			abcde.d_time_sec >= ts.time_slice_start_sec
			AND abcde.d_time_sec < ts.time_slice_end_sec
        
		LEFT JOIN dbo.service_date sd
		ON 
			abcde.service_date = sd.service_date
        
		LEFT JOIN dbo.config_passenger_arrival_rate par
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
			r.route_type = wt.route_type
				
		LEFT JOIN dbo.config_expected_in_vehicle_time ivt
		ON	
			sd.day_type_id = ivt.day_type_id
			AND abcde.abcd_stop_id = ivt.from_stop_id
			AND abcde.e_stop_id = ivt.to_stop_id
			AND r.route_type = ivt.route_type
			AND cde_route_id = ivt.route_id
			AND abcde_direction_id = ivt.direction_id
			AND ts.time_slice_id = ivt.time_slice_id
				
WHERE
	abcde.service_date = @service_date_process
	AND r.route_type IN (0, 1)

END