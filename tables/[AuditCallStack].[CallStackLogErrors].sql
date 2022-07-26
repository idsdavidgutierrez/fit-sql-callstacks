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
	WHERE t.name = N'CallStackLogErrors' AND sch.name = N'AuditCallStack'
)
BEGIN
	CREATE TABLE [AuditCallStack].[CallStackLogErrors](
		[CallStackId] [bigint] NOT NULL,
		[ErrorNumber] [int] NULL,
		[ErrorMessage] [nvarchar](4000) NULL,
		[UTCDate] [date] NOT NULL,
	 CONSTRAINT [CI_CallStackLogErrors] PRIMARY KEY CLUSTERED 
	(
		[CallStackId] ASC
	)WITH (DATA_COMPRESSION = PAGE)
	);
END;

