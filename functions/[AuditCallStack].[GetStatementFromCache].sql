SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
GO
CREATE OR ALTER FUNCTION [AuditCallStack].[GetStatementFromCache] (
	@sql_handle VARBINARY(64),
	@OffsetStart INT,
	@OffsetEnd INT
) RETURNS TABLE
AS
-- original code is here: https://gist.github.com/zikato/e7dd190ed193f7de954f85ce6fcaa7c3

RETURN (
	SELECT
		dest.dbid,
		dest.objectid,
		ca.statementText
	FROM sys.dm_exec_sql_text(@sql_handle) dest
	CROSS APPLY
		(
			SELECT 
				SUBSTRING
				(	
					dest.text,
					(@OffsetStart / 2) + 1,
					((
						CASE
							WHEN @OffsetEnd = -1
								THEN DATALENGTH(dest.text)
							ELSE @OffsetEnd
						END 
						- @OffsetStart
					) / 2) + 1
				)
		) AS ca(statementText)
);