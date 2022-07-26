SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
GO
CREATE OR ALTER FUNCTION [AuditCallStack].[GetXESessionName](@IsTemp BIT) RETURNS SYSNAME
AS
BEGIN
	RETURN N'CollectCallStack_' + CASE WHEN @IsTemp= 1 THEN N'TEMP' ELSE N'MAIN' END;
END;