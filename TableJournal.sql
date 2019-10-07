if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[spCreateJournal]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
 drop procedure [dbo].[spCreateJournal]
GO

SET QUOTED_IDENTIFIER ON 
GO
SET ANSI_NULLS ON 
GO



CREATE PROCEDURE spCreateJournal (@TblName sysname)
AS
BEGIN

DECLARE @ColumnName sysname
DECLARE @full_table_name nvarchar(255)
DECLARE @SQLCommand nvarchar(4000)
DECLARE @SelectIntoList nvarchar(2000)
DECLARE @InsertIntoList nvarchar(2000)
DECLARE @SelectTriggerList nvarchar(2000)
DECLARE @JoinCondition nvarchar(2000)
DECLARE @Ret int

SET @SelectIntoList = ''
SET @InsertIntoList = ''
SET @SelectTriggerList = ''
SET @JoinCondition = ''

SET NOCOUNT ON

DECLARE @table_id int
SELECT @full_table_name = quotename(@TblName)
SELECT @table_id = object_id(@full_table_name)
IF @table_id is null
BEGIN    /* Original table doesn't exist */
 RAISERROR ('Error: The table %s does not exist in the current database.', -1, -1, @TblName)
 RETURN
END

IF not exists(select * from sysindexes where id = @table_id and (status & 0x800) = 0x800)
BEGIN    /* Original table doesn't have primary key */
 RAISERROR ('Error: The table %s must have a primary key to be journalized.', -1, -1, @TblName)
 RETURN
END

DECLARE c CURSOR
FOR
SELECT [name]
 FROM syscolumns
WHERE [id] = @table_id
ORDER BY colorder

OPEN c

FETCH NEXT FROM c INTO @ColumnName
WHILE (@@fetch_status <> -1)
BEGIN
 /* NullIF function is used in order to create column of the same type as original one and in the same time to allow null */
 SET @SelectIntoList = @SelectIntoList + ' NullIF(' + @ColumnName + ',' + @ColumnName + ') Old_' + @ColumnName + ',' + ' NullIF(' + @ColumnName + ',' + @ColumnName + ') New_' + @ColumnName + ','
 SET @InsertIntoList = @InsertIntoList + ' Old_' + @ColumnName + ', New_' + @ColumnName + ','
 SET @SelectTriggerList = @SelectTriggerList + ' d.' + @ColumnName + ',' + ' i.' + @ColumnName + ','
 FETCH NEXT FROM c INTO @ColumnName
END

CLOSE c
DEALLOCATE c

SET @SQLCommand = 'if exists (select * from dbo.sysobjects where id = object_id(N''[dbo].[JRN_' + @TblName + ']'') and OBJECTPROPERTY(id, N''IsUserTable'') = 1)
drop table [dbo].[JRN_' + @TblName + ']'

SET @SQLCommand = @SQLCommand + ' SELECT IDENTITY(bigint, 1, 1) JRNID, ' + @SelectIntoList + ' suser_sname() UserName, ''ACT'' Action, GetDate() DateTime INTO [JRN_' + @TblName + '] FROM ' + @full_table_name + ' WHERE 1 != 1'

/* Create journal table */
exec @Ret=sp_executesql @SQLCommand 
IF @Ret = 0
 Print 'Journal Table [JRN_' + @TblName + ']' + ' has been created.'

SET @SQLCommand = 'ALTER TABLE [JRN_' + @TblName + '] ADD CONSTRAINT [PK_JRN_' + @TblName + '] PRIMARY KEY CLUSTERED (JRNID)'

/* Create primary key */
exec @Ret=sp_executesql @SQLCommand 
IF @Ret = 0
 Print 'Primary Key [PK_JRN_' + @TblName + ']' + ' has been created.'


/* Drop trigger if it exists */
SET @SQLCommand = '
IF EXISTS (SELECT name 
     FROM sysobjects 
     WHERE name = N''Trg_' + @TblName + ''' 
     AND      type = ''TR'')
 DROP TRIGGER [Trg_' + @TblName + ']'
exec @Ret=sp_executesql @SQLCommand 

/* Create string with join condition */
DECLARE c CURSOR
FOR
select COLUMN_NAME = convert(sysname,c1.name)
 from sysindexes i, syscolumns c1
where i.id = @table_id
 and i.id = c1.id
 and (i.status & 0x800) = 0x800 
 and c1.name = index_col (@full_table_name, i.indid, c1.colid)
OPEN c

FETCH NEXT FROM c INTO @ColumnName
WHILE (@@fetch_status <> -1)
BEGIN
 SET @JoinCondition = @JoinCondition + ' d.' + quotename(@ColumnName) + ' = i.' + quotename(@ColumnName)
 FETCH NEXT FROM c INTO @ColumnName
 IF (@@fetch_status <> -1) SET @JoinCondition = @JoinCondition + ' and '
END

CLOSE c
DEALLOCATE c


SET @SQLCommand = 'CREATE TRIGGER [Trg_' + @TblName + ']
ON [' + @TblName + ']
FOR INSERT, UPDATE, DELETE
AS 
BEGIN
DECLARE @Action char(3)
IF not exists(Select * from deleted)
 SET @Action = ''INS''
ELSE IF not exists(Select * from inserted)
 SET @Action = ''DEL''
ELSE
 SET @Action = ''UPD''
INSERT INTO [JRN_' + @TblName + '] (' + @InsertIntoList + ' UserName, Action, DateTime)
SELECT ' + @SelectTriggerList + ' suser_sname(), @Action, getdate()
FROM deleted d full outer join inserted i
 ON ' + @JoinCondition + '
END'

/* Create trigger */
exec @Ret=sp_executesql @SQLCommand 
IF @Ret = 0
 Print 'Trigger [Trg_' + @TblName + '] on ' + @full_table_name + ' table has been created.'

SET NOCOUNT OFF

END
GO
SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS ON 
GO



-- List of changes
DECLARE @TblName sysname
DECLARE @table_id int
DECLARE @ColumnName sysname
DECLARE @Length smallint
DECLARE @Case nvarchar(4000)
DECLARE @SQLCommand nvarchar(4000)

CREATE TABLE #retUpdateColumns (jrnid bigint,
 updated nvarchar(4000))

SET NOCOUNT ON

SET @TblName = N'put journal table name here'

SET @Case = N''
SELECT @table_id = object_id(quotename(@TblName))
IF @table_id is null
BEGIN    /* Original table doesn't exist */
 RAISERROR ('Table name must be the name of the current database.', -1, -1)
END

IF @TblName not like 'JRN_%'
BEGIN    /* Original table doesn't have primary key */
 RAISERROR ('This procedure is created to work only with Journal tables created by spCreateJournal.', -1, -1)
END

IF not exists(select * from syscolumns WHERE [id] = @table_id and [name] = 'jrnid')
BEGIN    /* Original table doesn't have primary key */
 RAISERROR ('This procedure is not going to work with older versions of Journal tables', -1, -1)
END

DECLARE c CURSOR
FOR
SELECT distinct substring([name], 5, len([name])-4), length
 FROM syscolumns
WHERE [id] = @table_id
 AND ([name] like 'old_%' or [name] like 'new_%')

OPEN c

FETCH NEXT FROM c INTO @ColumnName, @Length
WHILE (@@fetch_status <> -1)
BEGIN
 SET @Case = @Case + case when len(@Case) > 0 then ' + ' else '' end +
 'case when Old_' + @ColumnName + ' != New_' + @ColumnName + ' then '' [' + 
 upper(@ColumnName) + '] Old: '' + cast(Old_' + @ColumnName + ' as char(' + RTRIM(cast(@Length as char(4))) + '))' +
 ' + ''; New: '' + cast(New_' + @ColumnName + ' as char(' + RTRIM(cast(@Length as char(4))) + '))' +
 ' else '''' end'
 FETCH NEXT FROM c INTO @ColumnName, @Length
END

CLOSE c
DEALLOCATE c

set @Case = @Case + ' [Updated]'
set @SQLCommand = 'SELECT JRNID, ' + @Case + ' FROM ' + quotename(@TblName) + ' WHERE [Action] = ''UPD'''

insert into #retUpdateColumns
exec sp_executesql @SQLCommand 
SET NOCOUNT OFF

select * from #retUpdateColumns
where updated is not null

DROP TABLE #retUpdateColumns