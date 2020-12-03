
--Check last event timestamp is within 10 minutes of current service_datetime
--Return difference in min. between now and the latest event timestamp

SELECT	
	(DATEDIFF(S, '1970-01-01', GETUTCDATE()) - MAX(event_time))/60
FROM 
	mbta_performance.dbo.rt_event




