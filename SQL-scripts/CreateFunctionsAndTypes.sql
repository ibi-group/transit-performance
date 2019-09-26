--Script Version: Master - 1.1.0.0 - VP quality - 1

---run this script in the transit-performance database
--USE transit_performance
--GO

--function to convert datetime into epoch
IF OBJECT_ID('fnConvertDateTimeToEpoch') IS NOT NULL
	DROP FUNCTION dbo.fnConvertDateTimeToEpoch
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE FUNCTION dbo.fnConvertDateTimeToEpoch 
( 
      @fromDateTime datetime2 --in current timezone
)
RETURNS INT
AS
BEGIN
	DECLARE @timezone_diff_epoch INT
	SET @timezone_diff_epoch = DATEDIFF(s,GETUTCDATE(),GETDATE())
		
	DECLARE @toEpoch INT; ---in GMT
	SET @toEpoch = DATEDIFF(second, '1970-01-01 00:00:00', @fromDateTime)- DATEDIFF(s,GETUTCDATE(),GETDATE());
	
	RETURN @toEpoch;

END



GO

IF OBJECT_ID ('fnConvertDateTimeToServiceDate') IS NOT NULL
DROP FUNCTION dbo.fnConvertDateTimeToServiceDate
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE FUNCTION dbo.fnConvertDateTimeToServiceDate 
( 
      @service_datetime DATETIME 
)
RETURNS DATE
AS
BEGIN
DECLARE	@service_date DATE 

	IF CONVERT(TIME, @service_datetime) > '03:30:00.000'
		SET @service_date = CONVERT(DATE, @service_datetime)
		ELSE SET @service_date = DATEADD(d,-1,CONVERT(DATE, @service_datetime))

	
	RETURN @service_date;

END




GO

IF OBJECT_ID ('fnConvertEpochToDateTime') IS NOT NULL
DROP FUNCTION dbo.fnConvertEpochToDateTime
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE FUNCTION dbo.fnConvertEpochToDateTime 
( 
      @fromEpoch INT --in GMT
)
 RETURNS datetime2
AS
BEGIN
	DECLARE @timezone_diff_epoch INT
	SET @timezone_diff_epoch = DATEDIFF(s,GETUTCDATE(),GETDATE())
		
	DECLARE @toDateTime datetime2; ---in current time zone
	SET @toDateTime = DATEADD(s, @fromEpoch + @timezone_diff_epoch, '1970-01-01');
	
	RETURN @toDateTime;

END
GO

IF OBJECT_ID ('fnGetDistanceFeet') IS NOT NULL
DROP FUNCTION dbo.fnGetDistanceFeet
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION dbo.fnGetDistanceFeet 
( 
      @Lat1 Float(18),  
      @Long1 Float(18), 
      @Lat2 Float(18), 
      @Long2 Float(18)
)
RETURNS INT
AS
BEGIN
      Declare @R Float(8); 
      Declare @dLat Float(18); 
      Declare @dLon Float(18); 
      Declare @a Float(18); 
      Declare @c Float(18); 
      Declare @d INT;
      Set @R = 3956.55 
      Set @dLat = Radians(@lat2 - @lat1);
      Set @dLon = Radians(@long2 - @long1);
      Set @a = Sin(@dLat / 2)  
                 * Sin(@dLat / 2)  
                 + Cos(Radians(@lat1)) 
                 * Cos(Radians(@lat2))  
                 * Sin(@dLon / 2)  
                 * Sin(@dLon / 2); 
      Set @c = 2 * Asin(Min(Sqrt(@a))); 

      Set @d = ROUND(@R * @c * 5280,0); 
      Return @d; 

END



GO

IF TYPE_ID ('dbo.int_val_type') IS NOT NULL
	DROP TYPE dbo.int_val_type
GO


CREATE TYPE dbo.int_val_type AS TABLE(
	int_val int NULL
)
GO

IF TYPE_ID ('dbo.str_val_type') IS NOT NULL
	DROP TYPE dbo.str_val_type
GO

CREATE TYPE dbo.str_val_type AS TABLE(
	str_val varchar(255) NULL
)
GO







