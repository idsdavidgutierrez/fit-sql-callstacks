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
	WHERE t.name = N'SqlHandleToTextLink' AND sch.name = N'AuditCallStack'
)
BEGIN
	CREATE TABLE [AuditCallStack].[SqlHandleToTextLink](
		[StatementTextId] [bigint] IDENTITY(1,1) NOT NULL,
		[sql_handle] [varbinary](64) NOT NULL,
		[OffsetStart] [int] NOT NULL,
		[OffsetEnd] [int] NOT NULL,
		[RawStatementTextId] [bigint] NULL,
	 CONSTRAINT [PK_SqlHandleToTextLink] PRIMARY KEY CLUSTERED 
	(
		[StatementTextId] ASC
	)WITH (DATA_COMPRESSION = ROW)
	);
END;

IF NOT EXISTS (
	SELECT 1
	FROM sys.indexes i
	INNER JOIN sys.tables t ON i.object_id = t.object_id
	INNER JOIN sys.schemas sch ON t.schema_id = sch.schema_id
	WHERE i.name = N'IX_UNIQUE_SqlHandleToTextLink' AND t.name = N'SqlHandleToTextLink' AND sch.name = N'AuditCallStack'
)
BEGIN
	CREATE UNIQUE NONCLUSTERED INDEX [IX_UNIQUE_SqlHandleToTextLink] ON [AuditCallStack].[SqlHandleToTextLink]
	(
		[sql_handle] ASC,
		[OffsetStart] ASC,
		[OffsetEnd] ASC
	)
	INCLUDE([RawStatementTextId]) WITH (DATA_COMPRESSION = ROW);
END;