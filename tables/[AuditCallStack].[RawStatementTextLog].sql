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
	WHERE t.name = N'RawStatementTextLog' AND sch.name = N'AuditCallStack'
)
BEGIN
	CREATE TABLE [AuditCallStack].[RawStatementTextLog](
		[RawStatementTextId] [bigint] IDENTITY(1,1) NOT NULL,
		[StackDatabaseName] [sysname] NOT NULL,
		[StackObjectName] [sysname] NOT NULL,
		[StatementTextCompressed] [varbinary](max) NOT NULL,
		[StatementText]  AS (CONVERT([nvarchar](max),Decompress([StatementTextCompressed]))),
		[StatementTextChecksum]  AS (checksum(CONVERT([nvarchar](max),Decompress([StatementTextCompressed])))),
	CONSTRAINT [PK_RawStatementTextLog] PRIMARY KEY CLUSTERED 
	(
		[RawStatementTextId] ASC
	)WITH (DATA_COMPRESSION = PAGE)
	);
END;

IF NOT EXISTS (
	SELECT 1
	FROM sys.indexes i
	INNER JOIN sys.tables t ON i.object_id = t.object_id
	INNER JOIN sys.schemas sch ON t.schema_id = sch.schema_id
	WHERE i.name = N'IX_RawStatementTextLog_CheckExistence' AND t.name = N'RawStatementTextLog' AND sch.name = N'AuditCallStack'
)
BEGIN
	CREATE NONCLUSTERED INDEX [IX_RawStatementTextLog_CheckExistence] ON [AuditCallStack].[RawStatementTextLog]
	(
		[StackDatabaseName] ASC,
		[StackObjectName] ASC,
		[StatementTextChecksum] ASC
	)
	INCLUDE([StatementTextCompressed]) WITH (DATA_COMPRESSION = PAGE);
END;