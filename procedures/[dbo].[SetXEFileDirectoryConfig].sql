SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
GO
CREATE OR ALTER PROCEDURE [dbo].[SetXEFileDirectoryConfig] (@XEFileDirectory nvarchar(260))
AS
BEGIN
	SET NOCOUNT ON;
	-- use this procedure to document the correct file path for extended event files
	-- example: 'Q:\XE\'

	-- no need to handle concurrency
	IF EXISTS (SELECT 1 FROM dbo.XEFileDirectoryConfig)
	BEGIN
		UPDATE dbo.XEFileDirectoryConfig
		SET XEFileDirectory = @XEFileDirectory;
	END
	ELSE
	BEGIN
		INSERT INTO dbo.XEFileDirectoryConfig (XEFileDirectory)
		VALUES (@XEFileDirectory);
	END;

	RETURN;
END;