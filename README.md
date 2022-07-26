# Introduction

SQL Server exposes T-SQL call stacks through Extended Events. For example, consider a stored procedure that calls another stored procedure. The child stored procedure performs an insert into a table which has an `AFTER INSERT` DML trigger. The T-SQL call stack could look something like this within the trigger:

![image](https://user-images.githubusercontent.com/33984311/181058394-4dac5da0-7dea-4608-be3d-ea6b27faf9f5.png)

One easy way to get T-SQL call stacks is to use the [SQL Server Audit functionality](https://docs.microsoft.com/en-us/sql/relational-databases/security/auditing/sql-server-audit-database-engine?view=sql-server-ver16). However, this functionality has the following downsides:

1. The audit needs to be setup and running before the problem or event of interest occurs.
2. It can be difficult to tie application context to the data.
3. Depending on application code, a large amount of data may be logged by the audit.

To solve these problems, this repository provides a simple API that allows for developers to request that the T-SQL call stack be logged for the currently executing code. A `BIGINT` identifier is returned by the API to allow application context to be added to the request. The call stack information can be retrived for the Id by calling the `[AuditCallStack].[GetCallStacksById]` function. Note that the data won't be available instantly. It is processed asynchronously both by SQL Server and by the code in this repository.

Possible use cases for the API include:

1. Adding T-SQL call stack logging as part of a generic error handler.
2. Adding T-SQL call stack logging to DML triggers.
3. Adding T-SQL call stack logging to determine exactly how a stored procedure is being called by an application.

# Setup and Configuration

1. Install the code in your preferred database using the latest release. Note that objects will be created both under the "dbo" and the "AuditCallStack" schemas.
2. Configure a file path for the Extended Event files using the `[dbo].[SetXEFileDirectoryConfig]` stored procedure. SQL Server must be able to write to this file path.
3. Execute the `[AuditCallStack].[MoveDataToTable]` stored procedure on a recurring basis to move data from the Extended Event .xel files into SQL Server tables. A SQL Server Agent job is one way to do this.
4. Add calls to `[AuditCallStack].[LogCallStackAndReturnId]` in your code wherever you want to log a T-SQL call stack. Example code is below:

```
DECLARE @DatabaseName SYSNAME = DB_NAME();
DECLARE @CallStackId BIGINT;
EXEC [AuditCallStack].[LogCallStackAndReturnId] @DatabaseName, @CallStackId OUTPUT;
-- do something with the value in @CallStackId such as inserting it into a table with additional application context if desired
```
# Technical Notes

This code has been tested on a recent CU of SQL Server 2017 and SQL Server 2019, but it will probably work on SQL Server 2016 as well.

Required permissions may be challenging for some applications. The `[AuditCallStack].[LogCallStackAndReturnId]` stored procedure currently needs the `ALTER TRACE` permission because it calls the `sp_trace_generateevent` stored procedure. If this is a blocker for you please create a Github issue. The `[AuditCallStack].[MoveDataToTable]` stored procedure needs the ability to create, start, and stop Extended Event sessions.

Internally, SQL Server stores the T-SQL call stacks as a sql_handle, line number, and offset values. The code to resolve the sql_handle and offset values into a SQL statement first checks the plan cache for a match. It will also try to use Query Store to resolve the query. However, there are some scenarios where it isn't possible to resolve the full call stack. The best results will be achieved if the `[AuditCallStack].[MoveDataToTable]` stored procedure is run frequently enough to avoid plan cache retention issues. In other words, if a query plan is expected to remain in the plan cache for 30 minutes then the procedure should be run more frequently than once every 30 minutes.

In theory, the Extended Event session is configured to allow a total of 6 GB of data to be stored in the .xel files. In practice this number will be much lower. Already-processed data is automatically removed from the .xel files as part of the `[AuditCallStack].[MoveDataToTable]` stored procedure.

It is difficult to estimate the amount of SQL Server storage space that is required to store T-SQL call stacks. The amount of space depends on many factors including the depth of the call stack, the length of the SQL queries, and the frequency of reused queries. With that said, the code does attempt to minimize the amount of stored space used. For one data point, our production environment uses about 1 MB of space per 10000 call stack events.
