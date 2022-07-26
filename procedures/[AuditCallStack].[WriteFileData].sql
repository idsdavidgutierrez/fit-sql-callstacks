SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
GO
CREATE OR ALTER PROCEDURE [AuditCallStack].[WriteFileData] (
	@IsTemp BIT,
	@ErrorMessage NVARCHAR(2048) OUTPUT
)
AS
BEGIN
	SET NOCOUNT ON;
	SET @ErrorMessage = NULL;

	BEGIN TRY
		DECLARE
			@FilePath NVARCHAR(260) = [AuditCallStack].GetXESessionFileNameForReading(@IsTemp),
			@CurrentDatabaseId INT,
			@PreviousDatabaseId INT,
			@CurrentDatabaseName SYSNAME,
			@PreviousDatabaseName SYSNAME,
			@SQLForCacheJoin NVARCHAR(4000),
			@SQLForQueryStoreIndex NVARCHAR(4000),
			@SQLForQueryStoreJoin NVARCHAR(4000);

		-- insert XML into temp table to avoid repeated XML validations in the query
		CREATE TABLE #RawXML (
			TimeStampUTC DATETIME2 NOT NULL,
			RawXML XML NOT NULL
		);

		INSERT INTO #RawXML (TimeStampUTC, RawXML)
		SELECT
		timestamp_utc,
		n.query('.')
		FROM
		(
			SELECT CAST(event_data AS XML), timestamp_utc
			FROM sys.fn_xe_file_target_read_file (@FilePath, NULL , NULL, NULL) 
		) AS A1(X, timestamp_utc)   
		CROSS APPLY X.nodes('event') AS q(n);

		CREATE TABLE #ShreddedXML (
			TimeStampUTC DATETIME2 NOT NULL,
			CallStackId BIGINT NOT NULL,
			CallingDatabaseName SYSNAME NOT NULL,		
			StackLevel TINYINT NOT NULL,
			[sql_handle] VARBINARY(64) NOT NULL,
			OffsetStart INT NOT NULL,
			OffsetEnd INT NOT NULL,
			--Line INT NULL -- doesn't seem to be needed
		);

		INSERT INTO #ShreddedXML (TimeStampUTC, CallStackId, CallingDatabaseName, StackLevel, [sql_handle], OffsetStart, OffsetEnd)
		SELECT 
			TimeStampUTC,
			TRY_CAST(TRY_CONVERT(VARBINARY(16), (n.value('(data[@name="user_data"]/value)[1]', 'NVARCHAR(MAX)')), 2) AS BIGINT) AS CallStackId,
			n.value('(data[@name="user_info"]/value)[1]', 'SYSNAME') AS [CallingDatabaseName],
			m.value('./@level','tinyint') AS StackLevel, 
			CONVERT(VARBINARY(64), m.value('./@handle','varchar(100)'), 1) AS [sql_handle],
			m.value('.[1]/@offsetStart', 'int') AS offsetStart,
			m.value('.[1]/@offsetEnd', 'int') AS offsetEnd
			--m.value('.[1]/@line', 'int') AS line
		FROM #RawXML
		CROSS APPLY RawXML.nodes('event') AS q(n)
		CROSS APPLY RawXML.nodes('event/action[@name="tsql_stack"]/value/frames/frame') AS r(m)
		WHERE CONVERT(VARBINARY(64), m.value('./@handle','varchar(100)'), 1) <> 0x00; -- this is typically (always?) present and will never resolve

		TRUNCATE TABLE #RawXML;

		CREATE TABLE #SqlHandleCachedObjectid (
			CallingDatabaseName SYSNAME NOT NULL,
			[sql_handle] VARBINARY(64) NOT NULL,
			OffsetStart INT NOT NULL,
			OffsetEnd INT NOT NULL,
			DbidFromCache INT NULL,
			ObjectidFromCache INT NULL,		
			StatementTextFromCache NVARCHAR(MAX) NULL,		
			INDEX CI_CallStacks CLUSTERED (DbidFromCache)
		);

		INSERT INTO #SqlHandleCachedObjectid
		(CallingDatabaseName, [sql_handle], OffsetStart, OffsetEnd, DbidFromCache, ObjectidFromCache, StatementTextFromCache)
		SELECT 
		q.CallingDatabaseName,
		q.[sql_handle],
		q.OffsetStart,
		q.OffsetEnd,
		gs.dbid AS DbidFromCache,
		gs.objectid AS ObjectidFromCache,
		gs.statementText AS StatementTextFromCache
		FROM
		(
			SELECT DISTINCT
				sx.CallingDatabaseName,
				sx.[sql_handle],
				sx.OffsetStart,
				sx.OffsetEnd 
			FROM #ShreddedXML sx
			WHERE NOT EXISTS (
				SELECT 1
				FROM AuditCallStack.[SqlHandleToTextLink] l
				WHERE sx.[sql_handle] = l.[sql_handle]
				AND sx.OffsetStart = l.OffsetStart
				AND sx.OffsetEnd = l.OffsetEnd		
				AND l.[RawStatementTextId] IS NOT NULL
			)
		) q
		OUTER APPLY AuditCallStack.GetStatementFromCache(q.[sql_handle], q.OffsetStart, q.OffsetEnd) gs;

		CREATE TABLE #SqlHandleCachedObjectName (
			CallingDatabaseName SYSNAME NULL,
			[sql_handle] VARBINARY(64) NOT NULL,
			OffsetStart INT NOT NULL,
			OffsetEnd INT NOT NULL,
			CacheDatabaseName SYSNAME NULL,
			CacheObjectName SYSNAME NULL,
			CacheStatementText NVARCHAR(MAX) NULL,		
			INDEX CI_CallStacks CLUSTERED (CallingDatabaseName)
		);

		-- perform skip scan over clustered index
		SET @CurrentDatabaseId = NULL;
		SELECT TOP (1) @CurrentDatabaseId = DbidFromCache
		FROM #SqlHandleCachedObjectid
		WHERE DbidFromCache IS NOT NULL
		ORDER BY DbidFromCache;

		WHILE @CurrentDatabaseId IS NOT NULL
		BEGIN
			-- possible permission issues on sys.all_objects are unlikely because agent jobs are usually created as sysadmin
			SET @CurrentDatabaseName = DB_NAME(@CurrentDatabaseId);
			-- RECOMPILE to avoid plan cache pollution due to temp tables in dynamic SQL
			SET @SQLForCacheJoin = N'INSERT INTO #SqlHandleCachedObjectName
			(CallingDatabaseName, [sql_handle], OffsetStart, OffsetEnd, CacheDatabaseName, CacheObjectName, CacheStatementText)
			SELECT
				cs.[CallingDatabaseName],		
				cs.[sql_handle],
				cs.OffsetStart,
				cs.OffsetEnd,			
				CASE WHEN so.name IS NOT NULL THEN @CurrentDatabaseName ELSE NULL END,
				so.name,
				cs.StatementTextFromCache
			FROM #SqlHandleCachedObjectid cs
			LEFT OUTER JOIN ' + QUOTENAME(@CurrentDatabaseName) + N'.sys.all_objects so ON cs.ObjectidFromCache = so.[object_id]
			WHERE cs.DbidFromCache = @CurrentDatabaseId
			OPTION (RECOMPILE)';

			EXEC sp_executesql
				@SQLForCacheJoin,
				N'@CurrentDatabaseId INT, @CurrentDatabaseName SYSNAME',
				@CurrentDatabaseId,
				@CurrentDatabaseName;

			SET @PreviousDatabaseId = @CurrentDatabaseId;
			SET @CurrentDatabaseId = NULL;
			SELECT TOP (1) @CurrentDatabaseId = DbidFromCache
			FROM #SqlHandleCachedObjectid
			WHERE DbidFromCache > @PreviousDatabaseId
			ORDER BY DbidFromCache;
		END;

		INSERT INTO #SqlHandleCachedObjectName
		(CallingDatabaseName, [sql_handle], OffsetStart, OffsetEnd, CacheStatementText)
		SELECT
		cs.CallingDatabaseName,
		cs.[sql_handle],
		cs.OffsetStart,
		cs.OffsetEnd,
		cs.StatementTextFromCache
		FROM #SqlHandleCachedObjectid cs
		WHERE DB_NAME(cs.DbidFromCache) IS NULL;

		-- always check query store too
		-- some procedure names may not have been resolved due to plan cache eviction or for other unknown reasons
		CREATE TABLE #SqlHandleWithQueryStore (
			[sql_handle] VARBINARY(64) NOT NULL,
			OffsetStart INT NOT NULL,
			OffsetEnd INT NOT NULL,
			DatabaseName SYSNAME NULL,
			ObjectName SYSNAME NULL,
			StatementText NVARCHAR(MAX) NULL,
		);

		-- OUTER APPLY doesn't work well against some query store dmvs
		CREATE TABLE #IndexedQueryStore (
			[last_compile_batch_sql_handle] varbinary(64) NOT NULL,
			MinObjectId INT NULL,
			IsObjectIdUnique BIT NOT NULL,
			MinQueryTextId BIGINT NULL,
			IsTextIdUnique BIT NOT NULL,
			PRIMARY KEY ([last_compile_batch_sql_handle])
		);	

		-- perform skip scan over clustered index
		SET @CurrentDatabaseName = NULL;
		SELECT TOP (1) @CurrentDatabaseName = [CallingDatabaseName]
		FROM #SqlHandleCachedObjectName
		WHERE [CallingDatabaseName] IS NOT NULL
		ORDER BY [CallingDatabaseName];

		WHILE @CurrentDatabaseName IS NOT NULL
		BEGIN	
			-- the query below is a best effort to find a match from query store
			-- sometimes there will be multiple distinct queries for a sql_handle in which case we try to take the object_name only (if it's unique)

			-- RECOMPILE to avoid plan cache pollution due to temp tables in dynamic SQL

			SET @SQLForQueryStoreIndex = N'INSERT INTO #IndexedQueryStore
			([last_compile_batch_sql_handle], MinObjectId, IsObjectIdUnique, MinQueryTextId, IsTextIdUnique)
			SELECT [last_compile_batch_sql_handle], MinObjectId, IsObjectIdUnique, MinQueryTextId, IsTextIdUnique
			FROM (
				SELECT qsq.[last_compile_batch_sql_handle],
				MIN(qsq.Object_id) MinObjectId,
				CASE WHEN MIN(qsq.Object_id) = MAX(qsq.Object_id) THEN 1 ELSE 0 END IsObjectIdUnique,
				MIN(qsq.query_text_id) MinQueryTextId,
				CASE WHEN MIN(qsq.query_text_id) = MAX(qsq.query_text_id) THEN 1 ELSE 0 END IsTextIdUnique
				FROM ' + QUOTENAME(@CurrentDatabaseName) + N'.sys.query_store_query qsq
				GROUP BY qsq.[last_compile_batch_sql_handle]
			) q
			WHERE q.IsObjectIdUnique = 1 OR q.IsTextIdUnique = 1
			OPTION (RECOMPILE)';

			TRUNCATE TABLE #IndexedQueryStore;
			EXEC sp_executesql @SQLForQueryStoreIndex;
			
			SET @SQLForQueryStoreJoin = N'INSERT INTO #SqlHandleWithQueryStore
			([sql_handle], OffsetStart, OffsetEnd, DatabaseName, ObjectName, StatementText)
			SELECT
				cs.[sql_handle],
				cs.OffsetStart,
				cs.OffsetEnd,
				ISNULL(cs.CacheDatabaseName, @CurrentDatabaseName),
				ISNULL(cs.CacheObjectName, CASE WHEN iqs.IsObjectIdUnique = 1 THEN so.name ELSE NULL END),
				ISNULL(cs.CacheStatementText, CASE WHEN iqs.IsTextIdUnique = 1 THEN qt.query_sql_text ELSE NULL END)
			FROM #SqlHandleCachedObjectName cs
			LEFT OUTER JOIN #IndexedQueryStore iqs ON cs.[sql_handle] = iqs.[last_compile_batch_sql_handle]
			LEFT OUTER JOIN ' + QUOTENAME(@CurrentDatabaseName) + N'.sys.query_store_query_text qt ON iqs.[MinQueryTextId] = qt.[query_text_id]
			LEFT OUTER JOIN ' + QUOTENAME(@CurrentDatabaseName) + N'.sys.all_objects so ON iqs.[MinObjectId] = so.[object_id]
			WHERE cs.[CallingDatabaseName] = @CurrentDatabaseName
			OPTION (RECOMPILE)';

			EXEC sp_executesql
				@SQLForQueryStoreJoin,
				N'@CurrentDatabaseName SYSNAME',
				@CurrentDatabaseName;

			SET @PreviousDatabaseName = @CurrentDatabaseName;
			SET @CurrentDatabaseName = NULL;
			SELECT TOP (1) @CurrentDatabaseName = [CallingDatabaseName]
			FROM #SqlHandleCachedObjectName
			WHERE [CallingDatabaseName] > @PreviousDatabaseName
			ORDER BY [CallingDatabaseName];
		END;
		
		INSERT INTO #SqlHandleWithQueryStore
		([sql_handle], OffsetStart, OffsetEnd, DatabaseName, ObjectName, StatementText)
		SELECT
			cs.[sql_handle],
			cs.OffsetStart,
			cs.OffsetEnd,
			cs.CacheDatabaseName,
			cs.CacheObjectName,
			cs.CacheStatementText
		FROM #SqlHandleCachedObjectName cs
		WHERE cs.[CallingDatabaseName] IS NULL;

		CREATE TABLE #SqlHandleFinal (
			[sql_handle] VARBINARY(64) NOT NULL,
			OffsetStart INT NOT NULL,
			OffsetEnd INT NOT NULL,
			DatabaseName SYSNAME NULL,
			ObjectName SYSNAME NULL,
			StatementText NVARCHAR(MAX) NULL,
		);

		-- insert instead of delete to get minimal logging
		-- we're also likely to delete a large percentage of rows
		INSERT INTO #SqlHandleFinal
		([sql_handle], OffsetStart, OffsetEnd, DatabaseName, ObjectName, StatementText)
		SELECT
			q.[sql_handle],
			q.OffsetStart,
			q.OffsetEnd,
			q.DatabaseName,
			q.ObjectName,
			q.StatementText
		FROM #SqlHandleWithQueryStore q
		WHERE (ObjectName IS NULL OR ObjectName <> N'LogCallStackAndReturnId')
		AND (StatementText IS NULL OR (
			StatementText NOT LIKE 'EXEC master..sp[_]trace[_]generateevent%'
			AND CHARINDEX('.LogCallStackAndReturnId', StatementText) = 0
			AND StatementText NOT IN (N'sp_trace_generateevent', N'sp_executesql')
		));

		INSERT INTO [AuditCallStack].[RawStatementTextLog]
		([StackDatabaseName], [StackObjectName], [StatementTextCompressed])
		SELECT
			src.StackDatabaseName,
			src.[StackObjectName],
			src.[StatementTextCompressed]
		FROM
		(
			SELECT DISTINCT
				ISNULL(DatabaseName, N'') AS StackDatabaseName,
				ISNULL(ObjectName, N'') AS StackObjectName,
				COMPRESS(StatementText) StatementTextCompressed,
				CHECKSUM(StatementText) StatementTextChecksum
			FROM #SqlHandleFinal
			WHERE StatementText IS NOT NULL
		) src
		WHERE NOT EXISTS (
			SELECT 1
			FROM [AuditCallStack].[RawStatementTextLog] rtl
			WHERE src.StackDatabaseName = rtl.StackDatabaseName
			AND src.StackObjectName = rtl.StackObjectName
			AND src.StatementTextChecksum = rtl.StatementTextChecksum
			AND src.StatementTextCompressed = rtl.StatementTextCompressed -- avoid collisions
		);

		UPDATE tgt
		SET
			[RawStatementTextId] = src.[RawStatementTextId]
		FROM AuditCallStack.[SqlHandleToTextLink] tgt
		INNER JOIN (
			SELECT
				q.[sql_handle],
				q.OffsetStart,
				q.OffsetEnd,
				rtl.[RawStatementTextId]
			FROM (
				SELECT DISTINCT
					[sql_handle],
					OffsetStart,
					OffsetEnd,
					ISNULL(DatabaseName, N'') AS StackDatabaseName,
					ISNULL(ObjectName, N'') AS StackObjectName,
					COMPRESS(StatementText) StatementTextCompressed,
					CHECKSUM(StatementText) StatementTextChecksum,
					ROW_NUMBER() OVER (PARTITION BY [sql_handle], OffsetStart, OffsetEnd ORDER BY OffsetStart) RN -- arbitrary order
				FROM #SqlHandleFinal
				WHERE StatementText IS NOT NULL
			) q
			INNER JOIN [AuditCallStack].[RawStatementTextLog] rtl ON 
				q.StackDatabaseName = rtl.StackDatabaseName
				AND q.StackObjectName = rtl.StackObjectName
				AND q.StatementTextChecksum = rtl.StatementTextChecksum
				AND q.StatementTextCompressed = rtl.StatementTextCompressed -- avoid collisions			
			WHERE q.RN = 1
		) src ON (tgt.[sql_handle] = src.[sql_handle] AND tgt.OffsetStart = src.OffsetStart AND tgt.OffsetEnd = src.OffsetEnd)
		WHERE tgt.[RawStatementTextId] IS NULL;
	
		INSERT INTO AuditCallStack.SqlHandleToTextLink
		([sql_handle], OffsetStart, OffsetEnd, [RawStatementTextId])
		SELECT
			src.[sql_handle],
			src.OffsetStart,
			src.OffsetEnd,
			rtl.[RawStatementTextId]
		FROM (
			SELECT 
				[sql_handle],
				OffsetStart,
				OffsetEnd,
				ISNULL(DatabaseName, N'') AS StackDatabaseName,
				ISNULL(ObjectName, N'') AS StackObjectName,
				COMPRESS(StatementText) StatementTextCompressed,
				CHECKSUM(StatementText) StatementTextChecksum,
				StatementText,
				ROW_NUMBER() OVER (PARTITION BY [sql_handle], OffsetStart, OffsetEnd ORDER BY CASE WHEN StatementText IS NOT NULL THEN 0 ELSE 1 END) RN
			FROM #SqlHandleFinal
		) src
		LEFT OUTER JOIN [AuditCallStack].[RawStatementTextLog] rtl ON 
			src.StackDatabaseName = rtl.StackDatabaseName
			AND src.StackObjectName = rtl.StackObjectName
			AND src.StatementTextChecksum = rtl.StatementTextChecksum
			AND src.StatementTextCompressed = rtl.StatementTextCompressed -- avoid collisions	
		WHERE src.RN = 1
		AND NOT EXISTS ( -- can be duplicates because two extended event sessions can exist concurrently
			SELECT 1
			FROM AuditCallStack.SqlHandleToTextLink tgt
			WHERE tgt.[sql_handle] = src.[sql_handle] AND tgt.OffsetStart = src.OffsetStart AND tgt.OffsetEnd = src.OffsetEnd
		);

		INSERT INTO AuditCallStack.CallStackLog
		(CallStackId, StackLevel, StatementTextId, UTCDate)
		SELECT
			sx.CallStackId,
			sx.StackLevel,
			stl.StatementTextId,
			CAST(sx.TimeStampUTC AS DATE)
		FROM #ShreddedXML sx
		INNER JOIN AuditCallStack.SqlHandleToTextLink stl ON
			sx.[sql_handle] = stl.[sql_handle] AND sx.OffsetStart = stl.OffsetStart AND sx.OffsetEnd = stl.OffsetEnd
		WHERE NOT EXISTS ( -- can be duplicates because two extended event sessions can exist concurrently
			SELECT 1
			FROM AuditCallStack.CallStackLog csl
			WHERE sx.CallStackId = csl.CallStackId
		);
	END TRY
	BEGIN CATCH
		SET @ErrorMessage = LEFT(ERROR_MESSAGE(), 2048);
	END CATCH;

	RETURN;
END;