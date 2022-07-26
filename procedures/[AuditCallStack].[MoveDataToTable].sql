SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
GO
CREATE OR ALTER PROCEDURE [AuditCallStack].[MoveDataToTable]
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE
		@XESessionNameMain SYSNAME = AuditCallStack.GetXESessionName(0),
		@XESessionNameTemp SYSNAME = AuditCallStack.GetXESessionName(1),
		@SleepMS INT = 1000 * AuditCallStack.GetXEDispatchLatency(),
		@WriteFileErrorMessage NVARCHAR(2048);
	
	-- cannot use transactions when modifying extended event sessions

	EXEC AuditCallStack.[CreateAndStartXE] 0;

	EXEC AuditCallStack.[CreateAndStartXE] 1;

	-- process main
	EXEC dbo.[SleepByMS] @SleepMS; -- make sure the buffers have flushed

	EXEC AuditCallStack.WriteFileData 0, @WriteFileErrorMessage OUTPUT;

	IF @WriteFileErrorMessage IS NOT NULL
	BEGIN
		THROW 97001, @WriteFileErrorMessage, 1;
	END;

	EXEC dbo.[DeleteXEFlatFiles] @XESessionNameMain;

	-- process temp
	EXEC dbo.[SleepByMS] @SleepMS; -- make sure the buffers have flushed

	EXEC AuditCallStack.WriteFileData 1, @WriteFileErrorMessage OUTPUT;

	IF @WriteFileErrorMessage IS NOT NULL
	BEGIN
		THROW 97001, @WriteFileErrorMessage, 2;
	END;

	EXEC dbo.[DeleteXEFlatFiles] @XESessionNameTemp;

	EXEC [dbo].[StopXE] @XESessionNameTemp;

	RETURN;
END;