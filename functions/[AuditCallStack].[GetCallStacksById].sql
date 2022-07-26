SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
GO
CREATE OR ALTER FUNCTION [AuditCallStack].[GetCallStacksById] (@CallStackId BIGINT) RETURNS TABLE
AS
RETURN (
	SELECT TOP (987654321) -- fake TOP for ordering
	csl.StackLevel,
	CASE WHEN rtl.StackDatabaseName = N'' THEN NULL ELSE rtl.StackDatabaseName END StackDatabaseName,
	CASE WHEN rtl.StackObjectName = N'' THEN NULL ELSE rtl.StackObjectName END StackObjectName,
	rtl.StatementText
	FROM AuditCallStack.CallStackLog csl
	INNER JOIN AuditCallStack.[SqlHandleToTextLink] l ON csl.StatementTextId = l.StatementTextId
	INNER JOIN AuditCallStack.[RawStatementTextLog] rtl ON l.[RawStatementTextId] = rtl.[RawStatementTextId]	
	WHERE csl.CallStackId = @CallStackId
	ORDER BY csl.StackLevel DESC
);
