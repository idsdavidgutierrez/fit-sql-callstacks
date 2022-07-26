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
	WHERE t.name = N'XEFileDirectoryConfig' AND sch.name = N'AuditCallStack'
)
BEGIN
	CREATE TABLE [dbo].[XEFileDirectoryConfig](
		[XEFileDirectory] [nvarchar](260) NOT NULL
	);
END;