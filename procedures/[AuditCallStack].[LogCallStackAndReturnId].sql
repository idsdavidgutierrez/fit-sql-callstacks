SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
GO
CREATE OR ALTER PROCEDURE [AuditCallStack].[LogCallStackAndReturnId] (
	@DatabaseName SYSNAME,
	@CallStackId BIGINT OUTPUT
)
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE		
		@LogId BINARY(16),
		@TraceEventClass INT;

	-- check that server configuration is complete
	IF dbo.GetXEFileDirectoryConfig() IS NOT NULL
	BEGIN
		SELECT @CallStackId = NEXT VALUE FOR AuditCallStack.[SeqAuditCallStack];

		IF NOT EXISTS (
			SELECT 1 FROM fn_my_permissions (NULL, N'SERVER')
			WHERE permission_name = N'ALTER TRACE'
		)
		BEGIN
			INSERT INTO [AuditCallStack].[CallStackLogErrors] ([CallStackId], ErrorNumber, ErrorMessage, [UTCDate])
			VALUES (@CallStackId, 8189, N'Missing ALTER TRACE permission', CAST(GETUTCDATE() AS DATE));
		END;
		ELSE
		BEGIN		
			SET @TraceEventClass = AuditCallStack.GetTraceEventClass();		

			SET @LogId = CAST(@CallStackId AS BINARY(16));

			-- this is picked up by an extended event session CollectCallStack_*
			BEGIN TRY
				EXEC master..sp_trace_generateevent @event_class = @TraceEventClass, @userinfo = @DatabaseName, @userdata = @LogId;
			END TRY
			BEGIN CATCH
				INSERT INTO [AuditCallStack].[CallStackLogErrors] ([CallStackId], ErrorNumber, ErrorMessage, [UTCDate])
				VALUES (@CallStackId, ERROR_NUMBER(), ERROR_MESSAGE(), CAST(GETUTCDATE() AS DATE));

				SET @CallStackId = NULL;
			END CATCH;
		END;
	END;

	RETURN;
END;