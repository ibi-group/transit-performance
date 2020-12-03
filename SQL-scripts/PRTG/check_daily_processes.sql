IF OBJECT_ID('tempdb..#today_processing') IS NOT NULL 
DROP TABLE #today_processing

CREATE TABLE #today_processing
(
	process_name		varchar(300)
	,success			int 
) 



INSERT INTO #today_processing
SELECT 
	'PreProcessDaily'
	,CASE 
		WHEN 'PreProcessDaily' not in (select process_name from mbta_performance.dbo.daily_processing where convert(date,completed_timestamp) = convert(date,GETDATE())) THEN 0 
		WHEN 'PreProcessDaily' in (select process_name from mbta_performance.dbo.daily_processing where convert(date,completed_timestamp) = convert(date,GETDATE()) and success = 0) THEN 0 
		WHEN 'PreProcessDaily' in (select process_name from mbta_performance.dbo.daily_processing where convert(date,completed_timestamp) = convert(date,GETDATE()) and success = 1) THEN 1 
	END 

INSERT INTO #today_processing
SELECT 
	'PostProcessDaily'
	,CASE 
		WHEN 'PostProcessDaily' not in (select process_name from mbta_performance.dbo.daily_processing where convert(date,completed_timestamp) = convert(date,GETDATE())) THEN 0 
		WHEN 'PostProcessDaily' in (select process_name from mbta_performance.dbo.daily_processing where convert(date,completed_timestamp) = convert(date,GETDATE()) and success = 0) THEN 0 
		WHEN 'PostProcessDaily' in (select process_name from mbta_performance.dbo.daily_processing where convert(date,completed_timestamp) = convert(date,GETDATE()) and success = 1) THEN 1 
	END 


INSERT INTO #today_processing
SELECT 
	'ProcessPredictionAccuracyDaily'
	,CASE 
		WHEN 'ProcessPredictionAccuracyDaily' not in (select process_name from mbta_performance.dbo.daily_processing where convert(date,completed_timestamp) = convert(date,GETDATE())) THEN 0 
		WHEN 'ProcessPredictionAccuracyDaily' in (select process_name from mbta_performance.dbo.daily_processing where convert(date,completed_timestamp) = convert(date,GETDATE()) and success = 0) THEN 0 
		WHEN 'ProcessPredictionAccuracyDaily' in (select process_name from mbta_performance.dbo.daily_processing where convert(date,completed_timestamp) = convert(date,GETDATE()) and success = 1) THEN 1 
	END 

INSERT INTO #today_processing
SELECT 
	'PreProcessToday'
	,CASE 
		WHEN 'PreProcessToday' not in (select process_name from mbta_performance.dbo.daily_processing where convert(date,completed_timestamp) = convert(date,GETDATE())) THEN 0 
		WHEN 'PreProcessToday' in (select process_name from mbta_performance.dbo.daily_processing where convert(date,completed_timestamp) = convert(date,GETDATE()) and success = 0) THEN 0 
		WHEN 'PreProcessToday' in (select process_name from mbta_performance.dbo.daily_processing where convert(date,completed_timestamp) = convert(date,GETDATE()) and success = 1) THEN 1 
	END 

INSERT INTO #today_processing
SELECT 
	'CreateTodayRTProcess'
	,CASE 
		WHEN 'CreateTodayRTProcess' not in (select process_name from mbta_performance.dbo.daily_processing where convert(date,completed_timestamp) = convert(date,GETDATE())) THEN 0 
		WHEN 'CreateTodayRTProcess' in (select process_name from mbta_performance.dbo.daily_processing where convert(date,completed_timestamp) = convert(date,GETDATE()) and success = 0) THEN 0 
		WHEN 'CreateTodayRTProcess' in (select process_name from mbta_performance.dbo.daily_processing where convert(date,completed_timestamp) = convert(date,GETDATE()) and success = 1) THEN 1 
	END 


INSERT INTO #today_processing

SELECT 
	'clearData'
	,CASE 
		WHEN 'clearData' not in (select process_name from mbta_performance.dbo.daily_processing where convert(date,completed_timestamp) = convert(date,GETDATE())) THEN 0 
		WHEN 'clearData' in (select process_name from mbta_performance.dbo.daily_processing where convert(date,completed_timestamp) = convert(date,GETDATE()) and success = 0) THEN 0 
		WHEN 'clearData' in (select process_name from mbta_performance.dbo.daily_processing where convert(date,completed_timestamp) = convert(date,GETDATE()) and success = 1) THEN 1 
	END 
	
	
	
SELECT 
	*
FROM 
	#today_processing

