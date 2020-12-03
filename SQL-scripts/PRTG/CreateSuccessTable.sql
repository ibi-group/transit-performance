/*This script creates the table that will provide information on whether a process was successfully run
The processes that will be checked include: 
PreProcessDaily 
PostProcessDaily 
ProcessPredictionAccuracyDaily 
PreProcessToday 
CreateTodayRTProcess
ClearData 
*/ 

IF OBJECT_ID('dbo.daily_processing') IS NOT NULL 
DROP TABLE dbo.daily_processing
CREATE TABLE dbo.daily_processing

(
	service_date			DATE 
	,process_name			varchar(300)
	,success				int 
	,started_timestamp		datetime 
	,completed_timestamp	datetime  
)