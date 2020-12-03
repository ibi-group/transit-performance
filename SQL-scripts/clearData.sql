
---run this script in the transit-performance database
--USE transit_performance
--GO

IF OBJECT_ID('dbo.ClearData','P') IS NOT NULL
	DROP PROCEDURE dbo.ClearData

GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE dbo.ClearData

--Script Version: Master - 1.1.0.0 

--This procedure processes all of the events for the service_date being processed. It runs after the PreProcessDaily.

	@number_of_days		INT			--number of days to keep data

AS


BEGIN
	SET NOCOUNT ON;

	DECLARE @number_of_days_process INT = @number_of_days

	DECLARE	@service_date DATE = GETDATE()

	DECLARE @service_date_epoch INT = (SELECT dbo.fnConvertDateTimeToEpoch(@service_date))

 	--updates process_status table to notify that the process began


IF 
(
		@service_date IN (select service_date from dbo.daily_processing) 
		AND 
		'clearData' IN (select process from dbo.daily_processing where service_date = @service_date)
		--AND 
		--0 IN (select completed from dbo.daily_processing where service_date = @service_date and process = 'PreProcessDaily')
)
BEGIN 
	DELETE FROM 
	dbo.daily_processing
	WHERE 
	service_date = @service_date
	and 
	process = 'clearData' 
END 



BEGIN
	DELETE FROM 
	dbo.daily_processing
	WHERE 
	service_date = @service_date
	and 
	process = 'clearData' 

	
	INSERT INTO  dbo.daily_processing (service_date,process,completed, started_timestamp)  
	VALUES (@service_date,'clearData', 0, SYSDATETIME())	
	IF OBJECT_ID ('dbo.gtfsrt_tripupdate_denormalized','U') IS NOT NULL

		DELETE FROM dbo.gtfsrt_tripupdate_denormalized
		WHERE header_timestamp < (@service_date_epoch - @number_of_days_process*86400)

	IF OBJECT_ID ('dbo.gtfsrt_vehicleposition_denormalized','U') IS NOT NULL

		DELETE FROM dbo.gtfsrt_vehicleposition_denormalized
		WHERE header_timestamp < (@service_date_epoch - @number_of_days_process*86400)

	--updates process_status table to notify that the process has successfully ended		
	UPDATE dbo.daily_processing
	SET completed = 1, completed_timestamp = SYSDATETIME()
	WHERE 
		service_date = @service_date
		and 
		process ='clearData'																				   
END

GO