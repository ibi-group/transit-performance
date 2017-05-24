USE [master]
GO

--this script creates an SQL login. 
--replace the username, password and database name

CREATE LOGIN <login_name>			--insert login name here
	WITH PASSWORD= <password>,		--insert password 
	DEFAULT_DATABASE=  <DBname>,	--insert transit-performance database name here
	DEFAULT_LANGUAGE=[us_english], 
	CHECK_EXPIRATION=OFF, 
	CHECK_POLICY=OFF

GO


CREATE USER <username> ---insert username here 
FOR LOGIN <login_name>  --insert login name here
WITH DEFAULT_SCHEMA=[dbo]
GO