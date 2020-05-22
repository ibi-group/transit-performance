
---run this script in the transit-performance database
--USE transit_performance
--GO

IF OBJECT_ID('ExcessJourneyTimeUsingCD','P') IS NOT NULL
	DROP PROCEDURE dbo.ExcessJourneyTimeUsingCD

GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE dbo.ExcessJourneyTimeUsingCD

	@service_date_process DATE

--Script Version: Master - 1.0.0.0	
	
AS

BEGIN
	SET NOCOUNT ON;

IF OBJECT_ID('dbo.daily_journey_time_disaggregate_using_cd_time', 'U') IS NOT NULL
DROP TABLE dbo.daily_journey_time_disaggregate_using_cd_time

CREATE TABLE dbo.daily_journey_time_disaggregate_using_cd_time
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
		,total_passengers								FLOAT
		,passengers_with_zero_ejt						FLOAT
		,passengers_with_ejt_greater_than_zero_min		FLOAT
		,passengers_with_ejt_greater_than_one_min		FLOAT
		,passengers_with_ejt_greater_than_two_min		FLOAT
		,passengers_with_ejt_greater_than_three_min		FLOAT
		,passengers_with_ejt_greater_than_four_min		FLOAT
		,passengers_with_ejt_greater_than_five_min		FLOAT
		,passengers_with_ejt_greater_than_six_min		FLOAT
		,passengers_with_ejt_greater_than_seven_min		FLOAT
		,passengers_with_ejt_greater_than_eight_min		FLOAT
		,passengers_with_ejt_greater_than_nine_min		FLOAT
		,passengers_with_ejt_greater_than_ten_min		FLOAT
	)

INSERT INTO dbo.daily_journey_time_disaggregate_using_cd_time
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
		,total_passengers
		,passengers_with_zero_ejt
		,passengers_with_ejt_greater_than_zero_min
		,passengers_with_ejt_greater_than_one_min
		,passengers_with_ejt_greater_than_two_min
		,passengers_with_ejt_greater_than_three_min
		,passengers_with_ejt_greater_than_four_min
		,passengers_with_ejt_greater_than_five_min
		,passengers_with_ejt_greater_than_six_min
		,passengers_with_ejt_greater_than_seven_min
		,passengers_with_ejt_greater_than_eight_min
		,passengers_with_ejt_greater_than_nine_min
		,passengers_with_ejt_greater_than_ten_min
	)

SELECT DISTINCT
	abcde.service_date
	,abcd_stop_id 																												AS from_stop_id
	,e_stop_id 																													AS to_stop_id
	,r.route_type
	,cde_route_id 																												AS route_id
	,abcde_direction_id 																										AS direction_id
	,cde_trip_id																												AS trip_id
	,wt.expected_wait_time_sec																									AS expected_wait_time_sec
	,ivt.expected_in_vehicle_time_sec																							AS expected_in_vehicle_time_sec
	,(wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) 															AS expected_journey_time_sec
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
		WHEN 	(e_time_sec - b_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) <= 0					-- when max_excess_journey_time <= 0
		THEN 	0																												-- then total_excess_journey_time = 0
		WHEN 	(e_time_sec - b_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) > 0
		AND 	(e_time_sec - d_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) <= 0					-- when max_excess_journey_time > 0 and min_excess_journey_time <= 0
		THEN
				(
					(e_time_sec - b_time_sec) 
					- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
				)
				* 0.5																											-- average_excess_journey_time
				*
				(
					(e_time_sec - b_time_sec) 
					- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
				)
				* par.passenger_arrival_rate																					-- passengers_with_excess_journey_time
		ELSE																													-- else max_excess_journey_time > 0 and min_excess_journey_time > 0
				(
					(
						(e_time_sec - b_time_sec) 
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
					) 
					+ 
					(
						(e_time_sec - d_time_sec)
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
					)
				)
				* 0.5																											-- average_excess_journey_time
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
				) * par.passenger_arrival_rate																					-- passengers_with_excess_journey_time
		END																														AS total_excess_journey_time_sec
	,(wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) * (par.passenger_arrival_rate * (d_time_sec - b_time_sec))	AS total_expected_journey_time_sec
	,CASE	
		WHEN	(e_time_sec - b_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) <= 0						-- when max_excess_journey_time <= 0
		OR 		(par.passenger_arrival_rate * (d_time_sec - b_time_sec)) = 0	 
		THEN 	0																											-- then total_excess_journey_time = 0
		WHEN 	(e_time_sec - b_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) > 0
		AND 	(e_time_sec - d_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) <= 0					-- when max_excess_journey_time > 0 and min_excess_journey_time <= 0				
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
				/ 60
		END																														AS excess_journey_time_per_passenger_sec
	,c_time_sec - b_time_sec																									AS maximum_wait_time_sec
	,e_time_sec - c_time_sec																									AS maximum_in_vehicle_time_sec
	,e_time_sec - b_time_sec																									AS maximum_journey_time_sec
	,par.passenger_arrival_rate * (d_time_sec - b_time_sec)																		AS total_passengers
	,CASE	
		WHEN	(e_time_sec - b_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) <= 0					-- when max_excess_journey_time <= 0
		THEN	(par.passenger_arrival_rate * (d_time_sec - b_time_sec))														-- then passengers_with_excess_journey_time = 0
		WHEN	(e_time_sec - b_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) > 0
		AND		(e_time_sec - d_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) <= 0					-- when max_excess_journey_time > 0 and min_excess_journey_time <= 0
		THEN
				(par.passenger_arrival_rate * (d_time_sec - b_time_sec))
				-
				(
					(
						(e_time_sec - b_time_sec) 
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
					)
				* par.passenger_arrival_rate
				)																												-- then passengers_with_excess_journey_time =  max_excess_journey_time * passenger_arrival_rate
		ELSE																													-- else max_excess_journey_time > 0 and min_excess_journey_time <= 0
				(par.passenger_arrival_rate * (d_time_sec - b_time_sec))
				-
				(
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
				* par.passenger_arrival_rate
				)																												-- and passengers_with_excess_journey_time = (max_excess_journey_time - min_excess_journey_time) * passenger_arrival_rate
		END																														AS passengers_with_zero_ejt
	,CASE	
		WHEN	(e_time_sec - b_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) <= 0					-- when max_excess_journey_time <= 0
		THEN 	0																												-- then passengers_with_excess_journey_time = 0
		WHEN 	(e_time_sec - b_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) > 0
		AND 	(e_time_sec - d_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) <= 0					-- when max_excess_journey_time > 0 and min_excess_journey_time <= 0
		THEN																											
				(
					(e_time_sec - b_time_sec) 
					- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
				)
				* par.passenger_arrival_rate																					-- then passengers_with_excess_journey_time =  max_excess_journey_time * passenger_arrival_rate
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
		END																														AS passengers_with_ejt_greater_than_zero_min
	,CASE	
		WHEN 	(e_time_sec - b_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - (1*60) <= 0		-- when max_excess_journey_time <= 0
		THEN 	0																												-- then passengers_with_excess_journey_time = 0
		WHEN 	(e_time_sec - b_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - (1*60)  > 0
		AND 	(e_time_sec - d_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - (1*60)  <= 0		-- when max_excess_journey_time > 0 and min_excess_journey_time <= 0
		THEN																											
				(
					(e_time_sec - b_time_sec) 
					- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)  - (1*60) 
				)
				* par.passenger_arrival_rate																					-- then passengers_with_excess_journey_time =  max_excess_journey_time * passenger_arrival_rate
		ELSE																													-- else max_excess_journey_time > 0 and min_excess_journey_time <= 0
				(
					(
						(e_time_sec - b_time_sec) 
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
					) 
					- 
					(
						(e_time_sec - d_time_sec)
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)  - (1*60) 
					)
				) * par.passenger_arrival_rate																					-- and passengers_with_excess_journey_time = (max_excess_journey_time - min_excess_journey_time) * passenger_arrival_rate
		END																														AS passengers_with_ejt_greater_than_one_min
	,CASE	
		WHEN 	(e_time_sec - b_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - (2*60) <= 0		-- when max_excess_journey_time <= 0
		THEN 	0																												-- then passengers_with_excess_journey_time = 0
		WHEN 	(e_time_sec - b_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - (2*60)  > 0
		AND 	(e_time_sec - d_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - (2*60)  <= 0		-- when max_excess_journey_time > 0 and min_excess_journey_time <= 0
		THEN																											
				(
					(e_time_sec - b_time_sec) 
					- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)  - (2*60) 
				)
				* par.passenger_arrival_rate																					-- then passengers_with_excess_journey_time =  max_excess_journey_time * passenger_arrival_rate
		ELSE																													-- else max_excess_journey_time > 0 and min_excess_journey_time <= 0
				(
					(
						(e_time_sec - b_time_sec) 
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
					) 
					- 
					(
						(e_time_sec - d_time_sec)
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)  - (2*60) 
					)
				) * par.passenger_arrival_rate																					-- and passengers_with_excess_journey_time = (max_excess_journey_time - min_excess_journey_time) * passenger_arrival_rate
		END																														AS passengers_with_ejt_greater_than_two_min
	,CASE	
		WHEN 	(e_time_sec - b_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - (3*60) <= 0		-- when max_excess_journey_time <= 0
		THEN 	0																												-- then passengers_with_excess_journey_time = 0
		WHEN 	(e_time_sec - b_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - (3*60)  > 0
		AND 	(e_time_sec - d_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - (3*60)  <= 0		-- when max_excess_journey_time > 0 and min_excess_journey_time <= 0
		THEN																											
				(
					(e_time_sec - b_time_sec) 
					- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)  - (3*60) 
				)
				* par.passenger_arrival_rate																					-- then passengers_with_excess_journey_time =  max_excess_journey_time * passenger_arrival_rate
		ELSE																													-- else max_excess_journey_time > 0 and min_excess_journey_time <= 0
				(
					(
						(e_time_sec - b_time_sec) 
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
					) 
					- 
					(
						(e_time_sec - d_time_sec)
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)  - (3*60) 
					)
				) * par.passenger_arrival_rate																					-- and passengers_with_excess_journey_time = (max_excess_journey_time - min_excess_journey_time) * passenger_arrival_rate
		END																														AS passengers_with_ejt_greater_than_three_min
	,CASE	
		WHEN 	(e_time_sec - b_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - (4*60) <= 0		-- when max_excess_journey_time <= 0
		THEN 	0																												-- then passengers_with_excess_journey_time = 0
		WHEN 	(e_time_sec - b_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - (4*60)  > 0
		AND 	(e_time_sec - d_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - (4*60)  <= 0		-- when max_excess_journey_time > 0 and min_excess_journey_time <= 0
		THEN																											
				(
					(e_time_sec - b_time_sec) 
					- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)  - (4*60) 
				)
				* par.passenger_arrival_rate																					-- then passengers_with_excess_journey_time =  max_excess_journey_time * passenger_arrival_rate
		ELSE																													-- else max_excess_journey_time > 0 and min_excess_journey_time <= 0
				(
					(
						(e_time_sec - b_time_sec) 
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
					) 
					- 
					(
						(e_time_sec - d_time_sec)
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)  - (4*60) 
					)
				) * par.passenger_arrival_rate																					-- and passengers_with_excess_journey_time = (max_excess_journey_time - min_excess_journey_time) * passenger_arrival_rate
		END																														AS passengers_with_ejt_greater_than_four_min
	,CASE	
		WHEN 	(e_time_sec - b_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - (5*60) <= 0		-- when max_excess_journey_time <= 0
		THEN 	0																												-- then passengers_with_excess_journey_time = 0
		WHEN 	(e_time_sec - b_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - (5*60)  > 0
		AND 	(e_time_sec - d_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - (5*60)  <= 0		-- when max_excess_journey_time > 0 and min_excess_journey_time <= 0
		THEN																											
				(
					(e_time_sec - b_time_sec) 
					- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)  - (5*60) 
				)
				* par.passenger_arrival_rate																					-- then passengers_with_excess_journey_time =  max_excess_journey_time * passenger_arrival_rate
		ELSE																													-- else max_excess_journey_time > 0 and min_excess_journey_time <= 0
				(
					(
						(e_time_sec - b_time_sec) 
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
					) 
					- 
					(
						(e_time_sec - d_time_sec)
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)  - (5*60) 
					)
				) * par.passenger_arrival_rate																					-- and passengers_with_excess_journey_time = (max_excess_journey_time - min_excess_journey_time) * passenger_arrival_rate
		END																														AS passengers_with_ejt_greater_than_five_min
	,CASE	
		WHEN 	(e_time_sec - b_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - (6*60) <= 0		-- when max_excess_journey_time <= 0
		THEN 	0																												-- then passengers_with_excess_journey_time = 0
		WHEN 	(e_time_sec - b_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - (6*60)  > 0
		AND 	(e_time_sec - d_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - (6*60)  <= 0		-- when max_excess_journey_time > 0 and min_excess_journey_time <= 0
		THEN																											
				(
					(e_time_sec - b_time_sec) 
					- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)  - (6*60) 
				)
				* par.passenger_arrival_rate																					-- then passengers_with_excess_journey_time =  max_excess_journey_time * passenger_arrival_rate
		ELSE																													-- else max_excess_journey_time > 0 and min_excess_journey_time <= 0
				(
					(
						(e_time_sec - b_time_sec) 
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
					) 
					- 
					(
						(e_time_sec - d_time_sec)
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)  - (6*60) 
					)
				) * par.passenger_arrival_rate																					-- and passengers_with_excess_journey_time = (max_excess_journey_time - min_excess_journey_time) * passenger_arrival_rate
		END																														AS passengers_with_ejt_greater_than_six_min
	,CASE	
		WHEN 	(e_time_sec - b_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - (7*60) <= 0		-- when max_excess_journey_time <= 0
		THEN 	0																												-- then passengers_with_excess_journey_time = 0
		WHEN 	(e_time_sec - b_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - (7*60)  > 0
		AND 	(e_time_sec - d_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - (7*60)  <= 0		-- when max_excess_journey_time > 0 and min_excess_journey_time <= 0
		THEN																											
				(
					(e_time_sec - b_time_sec) 
					- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)  - (7*60) 
				)
				* par.passenger_arrival_rate																					-- then passengers_with_excess_journey_time =  max_excess_journey_time * passenger_arrival_rate
		ELSE																													-- else max_excess_journey_time > 0 and min_excess_journey_time <= 0
				(
					(
						(e_time_sec - b_time_sec) 
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
					) 
					- 
					(
						(e_time_sec - d_time_sec)
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)  - (7*60) 
					)
				) * par.passenger_arrival_rate																					-- and passengers_with_excess_journey_time = (max_excess_journey_time - min_excess_journey_time) * passenger_arrival_rate
		END																														AS passengers_with_ejt_greater_than_seven_min
	,CASE	
		WHEN 	(e_time_sec - b_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - (8*60) <= 0		-- when max_excess_journey_time <= 0
		THEN 	0																												-- then passengers_with_excess_journey_time = 0
		WHEN 	(e_time_sec - b_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - (8*60)  > 0
		AND 	(e_time_sec - d_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - (8*60)  <= 0		-- when max_excess_journey_time > 0 and min_excess_journey_time <= 0
		THEN																											
				(
					(e_time_sec - b_time_sec) 
					- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)  - (8*60) 
				)
				* par.passenger_arrival_rate																					-- then passengers_with_excess_journey_time =  max_excess_journey_time * passenger_arrival_rate
		ELSE																													-- else max_excess_journey_time > 0 and min_excess_journey_time <= 0
				(
					(
						(e_time_sec - b_time_sec) 
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
					) 
					- 
					(
						(e_time_sec - d_time_sec)
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)  - (8*60) 
					)
				) * par.passenger_arrival_rate																					-- and passengers_with_excess_journey_time = (max_excess_journey_time - min_excess_journey_time) * passenger_arrival_rate
		END																														AS passengers_with_ejt_greater_than_eight_min
	,CASE	
		WHEN 	(e_time_sec - b_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - (9*60) <= 0		-- when max_excess_journey_time <= 0
		THEN 	0																												-- then passengers_with_excess_journey_time = 0
		WHEN 	(e_time_sec - b_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - (9*60)  > 0
		AND 	(e_time_sec - d_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - (9*60)  <= 0		-- when max_excess_journey_time > 0 and min_excess_journey_time <= 0
		THEN																											
				(
					(e_time_sec - b_time_sec) 
					- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)  - (9*60) 
				)
				* par.passenger_arrival_rate																					-- then passengers_with_excess_journey_time =  max_excess_journey_time * passenger_arrival_rate
		ELSE																													-- else max_excess_journey_time > 0 and min_excess_journey_time <= 0
				(
					(
						(e_time_sec - b_time_sec) 
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
					) 
					- 
					(
						(e_time_sec - d_time_sec)
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)  - (9*60) 
					)
				) * par.passenger_arrival_rate																					-- and passengers_with_excess_journey_time = (max_excess_journey_time - min_excess_journey_time) * passenger_arrival_rate
		END																														AS passengers_with_ejt_greater_than_nine_min
	,CASE	
		WHEN 	(e_time_sec - b_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - (10*60) <= 0		-- when max_excess_journey_time <= 0
		THEN 	0																												-- then passengers_with_excess_journey_time = 0
		WHEN 	(e_time_sec - b_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - (10*60)  > 0
		AND 	(e_time_sec - d_time_sec) - (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec) - (10*60)  <= 0		-- when max_excess_journey_time > 0 and min_excess_journey_time <= 0
		THEN																											
				(
					(e_time_sec - b_time_sec) 
					- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)  - (10*60) 
				)
				* par.passenger_arrival_rate																					-- then passengers_with_excess_journey_time =  max_excess_journey_time * passenger_arrival_rate
		ELSE																													-- else max_excess_journey_time > 0 and min_excess_journey_time <= 0
				(
					(
						(e_time_sec - b_time_sec) 
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)
					) 
					- 
					(
						(e_time_sec - d_time_sec)
						- (wt.expected_wait_time_sec + ivt.expected_in_vehicle_time_sec)  - (10*60) 
					)
				) * par.passenger_arrival_rate																					-- and passengers_with_excess_journey_time = (max_excess_journey_time - min_excess_journey_time) * passenger_arrival_rate
		END																														AS passengers_with_ejt_greater_than_ten_min
FROM	
	##daily_abcde_time abcde
        
LEFT JOIN dbo.config_time_slice ts
ON
	abcde.d_time_sec >= ts.time_slice_start_sec
	AND abcde.d_time_sec < ts.time_slice_end_sec

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
	AND cde_route_id = ivt.route_id
	AND abcde_direction_id = ivt.direction_id
	AND ts.time_slice_id = ivt.time_slice_id
				
WHERE
	abcde.service_date = @service_date_process
	AND r.route_id IN ('Orange', 'Red', 'Green-B', 'Green-C', 'Green-D', 'Green-E')


IF
	(
		SELECT 
			COUNT(*)
		FROM 
			dbo.historical_journey_time_using_cd_time
		WHERE 
			service_date = @service_date_process
	)
		> 0

DELETE FROM dbo.historical_journey_time_using_cd_time
WHERE 
	service_date = @service_date_process	
	

INSERT INTO dbo.historical_journey_time_using_cd_time
	(	
		service_date
		,route_id
		,total_excess_journey_time_hr
		,total_passengers
		,excess_journey_time_per_passenger_sec
		,passengers_with_zero_ejt
		,passengers_with_ejt_greater_than_zero_min
		,passengers_with_ejt_greater_than_one_min
		,passengers_with_ejt_greater_than_two_min
		,passengers_with_ejt_greater_than_three_min
		,passengers_with_ejt_greater_than_four_min
		,passengers_with_ejt_greater_than_five_min
		,passengers_with_ejt_greater_than_six_min
		,passengers_with_ejt_greater_than_seven_min
		,passengers_with_ejt_greater_than_eight_min
		,passengers_with_ejt_greater_than_nine_min
		,passengers_with_ejt_greater_than_ten_min
	)	
	
SELECT
	service_date
	,route_id
	,SUM(total_excess_journey_time_sec) / 60.0 / 60.0
	,SUM(total_passengers)
	,SUM(total_excess_journey_time_sec) / SUM(total_passengers)
	,SUM(passengers_with_zero_ejt)
	,SUM(passengers_with_ejt_greater_than_zero_min)
	,SUM(passengers_with_ejt_greater_than_one_min)
	,SUM(passengers_with_ejt_greater_than_two_min)
	,SUM(passengers_with_ejt_greater_than_three_min)
	,SUM(passengers_with_ejt_greater_than_four_min)
	,SUM(passengers_with_ejt_greater_than_five_min)
	,SUM(passengers_with_ejt_greater_than_six_min)
	,SUM(passengers_with_ejt_greater_than_seven_min)
	,SUM(passengers_with_ejt_greater_than_eight_min)
	,SUM(passengers_with_ejt_greater_than_nine_min)
	,SUM(passengers_with_ejt_greater_than_ten_min)
FROM
	dbo.daily_journey_time_disaggregate_using_cd_time
GROUP BY
	service_date
	,route_id	
	
	
END