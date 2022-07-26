SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
GO
CREATE OR ALTER PROCEDURE [dbo].[StopXE] (@SessionName SYSNAME)
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @SQLForStop NVARCHAR(4000);

	IF EXISTS (SELECT 1 FROM sys.server_event_sessions WHERE [name] = @SessionName) AND
		EXISTS (SELECT 1 FROM sys.dm_xe_sessions WHERE [name] = @SessionName)
	BEGIN
		SET @SQLForStop = N'ALTER EVENT SESSION ' + QUOTENAME(@SessionName) + N' ON SERVER STATE = STOP';
		EXEC sp_executesql @SQLForStop;			
	END;

	RETURN;
END;