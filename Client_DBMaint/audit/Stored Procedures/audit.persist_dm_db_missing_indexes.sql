CREATE PROCEDURE 
    audit.persist_dm_db_missing_indexes 
AS

DECLARE 
    @last_service_start_date    datetime 
  , @last_data_persist_date     datetime ;


INSERT INTO 
    audit.dm_db_missing_index_groups( 
        index_group_handle, index_handle, date_stamp )
SELECT 
    index_group_handle	=	sysStats.index_group_handle
  , index_handle		=	sysStats.index_handle
  , date_stamp			=	GETDATE()
FROM 
    sys.dm_db_missing_index_groups AS sysStats
LEFT OUTER JOIN 
    audit.dm_db_missing_index_groups AS persist
        ON sysStats.index_group_handle = persist.index_group_handle
            AND sysStats.index_handle = persist.index_handle
WHERE 
    persist.index_group_handle IS NULL ; 


INSERT INTO 
    audit.dm_db_missing_index_details( 
        index_handle, database_id, object_id, equality_columns, inequality_columns
            , included_columns, statement, date_stamp )
SELECT 
	index_handle		=	sysStats.index_handle
  , database_id			=	sysStats.database_id
  , object_id			=	sysStats.object_id
  , equality_columns	=	sysStats.equality_columns
  , inequality_columns	=	sysStats.inequality_columns
  , included_columns	=	sysStats.included_columns
  , statement			=	sysStats.statement
  , date_stamp 			=	GETDATE()
FROM 
    sys.dm_db_missing_index_details AS sysStats
LEFT OUTER JOIN 
    audit.dm_db_missing_index_details AS persist 
        ON sysStats.index_handle = persist.index_handle
WHERE 
    persist.index_handle IS NULL ;

/*
The sys.dm_db_missing_index_columns DMF only provides normalized 
column metadata from sys.dm_db_missing_index_details.  I only insert new rows into
the table upon each cycle of persisting data from the DMOs.
*/

INSERT INTO 
    audit.dm_db_missing_index_columns(
        index_handle, column_id, column_name, column_usage, date_stamp )
SELECT
	index_handle	=	sysDetail.index_handle
  , column_id		=	sysColumn.column_id
  , column_name		=	sysColumn.column_name
  , column_usage	=	sysColumn.column_usage
  , date_stamp 		=	GETDATE()
FROM 
    sys.dm_db_missing_index_details AS sysDetail
CROSS APPLY 
    sys.dm_db_missing_index_columns( sysDetail.index_handle ) AS sysColumn
LEFT JOIN 
    audit.dm_db_missing_index_columns AS persistColumn
        ON persistColumn.index_handle = sysDetail.index_handle
           AND persistColumn.column_id = sysColumn.column_id
WHERE 
    persistColumn.index_handle IS NULL
ORDER BY 
    sysDetail.index_handle ;

--Determine last service restart date based upon tempdb creation date 
SELECT  
    @last_service_start_date = create_date
FROM    
    sys.databases
WHERE 
    name = 'tempdb' ; 
  
--Return the value for the last refresh date of the persisting table 
SELECT  
    @last_data_persist_date =   MAX( date_stamp )  
FROM    
    audit.dm_db_missing_index_group_stats ;

--Take care of updated records first 
IF  ( @last_service_start_date < @last_data_persist_date )
	UPDATE 
		persist
	SET
		unique_compiles             =   persist.unique_compiles + ( sysStats.unique_compiles - persist.last_poll_unique_compiles ) 
      , user_seeks                  =   persist.user_seeks + ( sysStats.user_seeks - persist.last_poll_user_seeks ) 
      , user_scans                  =   persist.user_scans + ( sysStats.user_scans - persist.last_poll_user_scans ) 
      , last_user_seek              =   sysStats.last_user_seek
      , last_user_scan              =   sysStats.last_user_scan
      , avg_total_user_cost         =   sysStats.avg_total_user_cost
      , avg_user_impact             =   sysStats.avg_user_impact
      , system_seeks                =   persist.system_seeks + ( sysStats.system_seeks - persist.last_poll_system_seeks ) 
      , system_scans                =   persist.system_scans + ( sysStats.system_scans - persist.last_poll_system_scans ) 
      , last_system_seek            =   sysStats.last_system_seek
      , last_system_scan            =   sysStats.last_system_scan
      , avg_total_system_cost       =   sysStats.avg_total_system_cost
      , avg_system_impact           =   sysStats.avg_system_impact
      , last_poll_unique_compiles   =   sysStats.unique_compiles
      , last_poll_user_seeks        =   sysStats.user_seeks
      , last_poll_user_scans        =   sysStats.user_scans 
      , last_poll_system_seeks      =   sysStats.system_seeks
      , last_poll_system_scans      =   sysStats.system_scans 
      , date_stamp					=	GETDATE() 
    FROM 
        audit.dm_db_missing_index_group_stats AS persist 
    INNER JOIN 
        sys.dm_db_missing_index_group_stats AS sysStats 
            ON sysStats.group_handle = persist.group_handle ;
ELSE
	UPDATE 
		persist
	SET
		unique_compiles             =   persist.unique_compiles + sysStats.unique_compiles		 
      , user_seeks                  =   persist.user_seeks + sysStats.user_seeks				
      , user_scans                  =   persist.user_scans + sysStats.user_scans				
      , last_user_seek              =   sysStats.last_user_seek
      , last_user_scan              =   sysStats.last_user_scan
      , avg_total_user_cost         =   sysStats.avg_total_user_cost
      , avg_user_impact             =   sysStats.avg_user_impact
      , system_seeks                =   persist.system_seeks + sysStats.system_seeks			
      , system_scans                =   persist.system_scans + sysStats.system_scans			
      , last_system_seek            =   sysStats.last_system_seek
      , last_system_scan            =   sysStats.last_system_scan
      , avg_total_system_cost       =   sysStats.avg_total_system_cost
      , avg_system_impact           =   sysStats.avg_system_impact
      , last_poll_unique_compiles   =   sysStats.unique_compiles
      , last_poll_user_seeks        =   sysStats.user_seeks
      , last_poll_user_scans        =   sysStats.user_scans 
      , last_poll_system_seeks      =   sysStats.system_seeks
      , last_poll_system_scans      =   sysStats.system_scans 
      , date_stamp					=	GETDATE() 
	FROM 
		audit.dm_db_missing_index_group_stats AS persist 
	INNER JOIN 
		sys.dm_db_missing_index_group_stats AS sysStats 
            ON sysStats.group_handle = persist.group_handle ;


--Take care of new records next 
INSERT INTO
    audit.dm_db_missing_index_group_stats( 
        group_handle, unique_compiles, user_seeks, user_scans, last_user_seek, last_user_scan
            , avg_total_user_cost, avg_user_impact, system_seeks, system_scans, last_system_seek, last_system_scan
            , avg_total_system_cost, avg_system_impact, last_poll_unique_compiles
            , last_poll_user_seeks, last_poll_user_scans, last_poll_system_seeks, last_poll_system_scans
            , date_stamp ) 
SELECT 
    group_handle				=	sysStats.group_handle
  , unique_compiles				=	sysStats.unique_compiles
  , user_seeks					=	sysStats.user_seeks 
  , user_scans					=	sysStats.user_scans
  , last_user_seek				=	sysStats.last_user_seek
  , last_user_scan				=	sysStats.last_user_scan
  , avg_total_user_cost			=	sysStats.avg_total_user_cost
  , avg_user_impact				=	sysStats.avg_user_impact
  , system_seeks				=	sysStats.system_seeks 
  , system_scans				=	sysStats.system_scans 
  , last_system_seek			=	sysStats.last_system_seek
  , last_system_scan			=	sysStats.last_system_scan
  , avg_total_system_cost		=	sysStats.avg_total_system_cost
  , avg_system_impact			=	sysStats.avg_system_impact
  , last_poll_unique_compiles	=	sysStats.unique_compiles
  , last_poll_user_seeks		=	sysStats.user_seeks
  , last_poll_user_scans		=	sysStats.user_scans 
  , last_poll_system_seeks		=	sysStats.system_seeks
  , last_poll_system_scans		=	sysStats.system_scans 
  , date_stamp 					=	GETDATE()     
FROM 
    sys.dm_db_missing_index_group_stats AS sysStats
LEFT JOIN 
    audit.dm_db_missing_index_group_stats AS persist 
        ON persist.group_handle = sysStats.group_handle
WHERE 
    persist.group_handle IS NULL ;