SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
GO
IF NOT EXISTS (
	SELECT 1
	FROM sys.tables t
	INNER JOIN sys.schemas sch ON t.schema_id = sch.schema_id
	WHERE t.name = N'CallStackLog' AND sch.name = N'AuditCallStack'
)
BEGIN
	CREATE TABLE [AuditCallStack].[CallStackLog](
		[CallStackId] [bigint] NOT NULL,
		[StackLevel] [tinyint] NOT NULL,
		[StatementTextId] [bigint] NOT NULL,
		[UTCDate] [date] NOT NULL,
		INDEX CI_CallStackLog CLUSTERED (CallStackId)
	)
	WITH (DATA_COMPRESSION = ROW)
	);
END;