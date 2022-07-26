SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
GO
CREATE OR ALTER PROCEDURE [AuditCallStack].[CreateAndStartXE] (
	@IsTemp BIT
)
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE
		@XESessionName SYSNAME,
		@XESessionFileName NVARCHAR(260),
		@SQLForCreate NVARCHAR(4000),
		@SQLForEnable NVARCHAR(4000);

	SET @XESessionName= AuditCallStack.GetXESessionName(@IsTemp);
	SET @XESessionFileName = AuditCallStack.GetXESessionFileNameForCreation(@IsTemp);

	IF @XESessionFileName IS NULL
	BEGIN;
		THROW 97000, N'Extended event session could not be created due to missing file name. Run the UtilityFacilIT.[dbo].[SetXEFileDirectoryConfig] stored procedure to configure the file directory.', 1;
	END;

	SET @SQLForCreate = N'CREATE EVENT SESSION ' + QUOTENAME(@XESessionName) + N' ON SERVER 
ADD EVENT sqlserver.user_event(
ACTION(sqlserver.tsql_stack)
WHERE ([event_id]=(' + CAST(AuditCallStack.GetTraceEventClass() AS NVARCHAR(2)) + N')))
ADD TARGET package0.event_file(SET filename=N''' + @XESessionFileName + ''',
max_rollover_files=(3),max_file_size=(1000))
WITH (MAX_MEMORY=4096 KB,EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS,MAX_DISPATCH_LATENCY=' + CAST(AuditCallStack.GetXEDispatchLatency() AS NVARCHAR(3))
+ N' SECONDS,MAX_EVENT_SIZE=0 KB,MEMORY_PARTITION_MODE=NONE,TRACK_CAUSALITY=OFF,STARTUP_STATE=ON)';

	IF NOT EXISTS (SELECT 1 FROM sys.server_event_sessions WHERE [name] = @XESessionName)
	BEGIN
		EXEC sp_executesql @SQLForCreate;
	END;

	EXEC dbo.StartXE @XESessionName;

	RETURN;
END;