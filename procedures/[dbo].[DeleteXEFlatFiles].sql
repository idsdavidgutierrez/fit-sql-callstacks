SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
GO
CREATE OR ALTER PROCEDURE [dbo].[DeleteXEFlatFiles] (@SessionName SYSNAME)
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE 
		@WasInitiallyStopped BIT,
		@XEStopCount TINYINT,
		@FileCount TINYINT;

	IF EXISTS (SELECT 1 FROM sys.server_event_sessions WHERE [name] = @SessionName) AND
		NOT EXISTS (SELECT 1 FROM sys.dm_xe_sessions WHERE [name] = @SessionName)
	BEGIN
		SET @WasInitiallyStopped = 1	
	END

	SELECT @FileCount = TRY_CONVERT(TINYINT, sesf.[value])
	FROM sys.server_event_sessions ses 
	INNER JOIN sys.server_event_session_fields sesf ON ses.event_session_id = sesf.event_session_id
	where ses.name = @SessionName
	and sesf.name = N'max_rollover_files';
	
	SET @XEStopCount = 0;
	WHILE @XEStopCount < @FileCount
	BEGIN
		EXEC dbo.StopXE @SessionName;
		EXEC dbo.StartXE @SessionName;
		
		SET @XEStopCount += 1;
	END;

	IF @WasInitiallyStopped = 1
	BEGIN
		EXEC dbo.StopXE @SessionName;
	END;

	RETURN;
END;