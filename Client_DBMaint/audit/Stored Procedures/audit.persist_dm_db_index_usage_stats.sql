CREATE PROCEDURE 
    audit.persist_dm_db_index_usage_stats 
AS  

SET XACT_ABORT, NOCOUNT ON ; 

DECLARE 
    @last_service_start_date    datetime 
  , @last_data_persist_date     datetime ;

  
--  SELECT last service restart date based upon tempdb creation date 
SELECT 
    @last_service_start_date    =   create_date
FROM 
    sys.databases
WHERE 
    name = 'tempdb' ; 

    
--  SELECT last refresh date from audit table 
SELECT 
    @last_data_persist_date =  MAX( date_stamp )  
FROM 
    audit.dm_db_index_usage_stats ;

    
--  UPDATE records with new index usage data 
IF( @last_service_start_date < @last_data_persist_date )
    BEGIN 
        UPDATE 
            auditUsageStats
        SET  
            auditUsageStats.user_seeks                  =   auditUsageStats.user_seeks      +   ( sysUsageStats.user_seeks - auditUsageStats.last_poll_user_seeks ) 
          , auditUsageStats.user_scans                  =   auditUsageStats.user_scans      +   ( sysUsageStats.user_scans - auditUsageStats.last_poll_user_scans ) 
          , auditUsageStats.user_lookups                =   auditUsageStats.user_lookups    +   ( sysUsageStats.user_lookups - auditUsageStats.last_poll_user_lookups ) 
          , auditUsageStats.user_updates                =   auditUsageStats.user_updates    +   ( sysUsageStats.user_updates - auditUsageStats.last_poll_user_updates ) 
          , auditUsageStats.last_user_seek              =   sysUsageStats.last_user_seek 
          , auditUsageStats.last_user_scan              =   sysUsageStats.last_user_scan 
          , auditUsageStats.last_user_lookup            =   sysUsageStats.last_user_lookup 
          , auditUsageStats.last_user_update            =   sysUsageStats.last_user_update 
          , auditUsageStats.system_seeks                =   auditUsageStats.system_seeks    +   ( sysUsageStats.system_seeks - auditUsageStats.last_poll_system_seeks ) 
          , auditUsageStats.system_scans                =   auditUsageStats.system_scans    +   ( sysUsageStats.system_scans - auditUsageStats.last_poll_system_scans ) 
          , auditUsageStats.system_lookups              =   auditUsageStats.system_lookups  +   ( sysUsageStats.system_lookups - auditUsageStats.last_poll_system_lookups ) 
          , auditUsageStats.system_updates              =   auditUsageStats.system_updates  +   ( sysUsageStats.system_updates - auditUsageStats.last_poll_system_updates ) 
          , auditUsageStats.last_system_seek            =   sysUsageStats.last_system_seek 
          , auditUsageStats.last_system_scan            =   sysUsageStats.last_system_scan 
          , auditUsageStats.last_system_lookup          =   sysUsageStats.last_system_lookup 
          , auditUsageStats.last_system_update          =   sysUsageStats.last_system_update 
          , auditUsageStats.last_poll_user_seeks        =   sysUsageStats.user_seeks 
          , auditUsageStats.last_poll_user_scans        =   sysUsageStats.user_scans 
          , auditUsageStats.last_poll_user_lookups      =   sysUsageStats.user_lookups 
          , auditUsageStats.last_poll_user_updates      =   sysUsageStats.user_updates 
          , auditUsageStats.last_poll_system_seeks      =   sysUsageStats.system_seeks 
          , auditUsageStats.last_poll_system_scans      =   sysUsageStats.system_scans 
          , auditUsageStats.last_poll_system_lookups    =   sysUsageStats.system_lookups 
          , auditUsageStats.last_poll_system_updates    =   sysUsageStats.system_updates 
          , auditUsageStats.date_stamp                  =   GETDATE() 
        FROM 
            sys.dm_db_index_usage_stats AS sysUsageStats 
        INNER JOIN  
            audit.dm_db_index_usage_stats AS auditUsageStats
                ON auditUsageStats.database_id = sysUsageStats.database_id
                    AND auditUsageStats.object_id = sysUsageStats.object_id
                    AND auditUsageStats.index_id  = sysUsageStats.index_id ;
    END 
ELSE 
    BEGIN 
        UPDATE 
            auditUsageStats
        SET  
            auditUsageStats.user_seeks                  =   auditUsageStats.user_seeks      +   sysUsageStats.user_seeks       
          , auditUsageStats.user_scans                  =   auditUsageStats.user_scans      +   sysUsageStats.user_scans       
          , auditUsageStats.user_lookups                =   auditUsageStats.user_lookups    +   sysUsageStats.user_lookups     
          , auditUsageStats.user_updates                =   auditUsageStats.user_updates    +   sysUsageStats.user_updates     
          , auditUsageStats.last_user_seek              =   sysUsageStats.last_user_seek                                       
          , auditUsageStats.last_user_scan              =   sysUsageStats.last_user_scan                                       
          , auditUsageStats.last_user_lookup            =   sysUsageStats.last_user_lookup                                     
          , auditUsageStats.last_user_update            =   sysUsageStats.last_user_update                                     
          , auditUsageStats.system_seeks                =   auditUsageStats.system_seeks    +   sysUsageStats.system_seeks     
          , auditUsageStats.system_scans                =   auditUsageStats.system_scans    +   sysUsageStats.system_scans     
          , auditUsageStats.system_lookups              =   auditUsageStats.system_lookups  +   sysUsageStats.system_lookups    
          , auditUsageStats.system_updates              =   auditUsageStats.system_updates  +   sysUsageStats.system_updates    
          , auditUsageStats.last_system_seek            =   sysUsageStats.last_system_seek 
          , auditUsageStats.last_system_scan            =   sysUsageStats.last_system_scan 
          , auditUsageStats.last_system_lookup          =   sysUsageStats.last_system_lookup 
          , auditUsageStats.last_system_update          =   sysUsageStats.last_system_update 
          , auditUsageStats.last_poll_user_seeks        =   sysUsageStats.user_seeks 
          , auditUsageStats.last_poll_user_scans        =   sysUsageStats.user_scans 
          , auditUsageStats.last_poll_user_lookups      =   sysUsageStats.user_lookups 
          , auditUsageStats.last_poll_user_updates      =   sysUsageStats.user_updates 
          , auditUsageStats.last_poll_system_seeks      =   sysUsageStats.system_seeks 
          , auditUsageStats.last_poll_system_scans      =   sysUsageStats.system_scans 
          , auditUsageStats.last_poll_system_lookups    =   sysUsageStats.system_lookups 
          , auditUsageStats.last_poll_system_updates    =   sysUsageStats.system_updates 
          , auditUsageStats.date_stamp                  =   GETDATE() 
        FROM 
            sys.dm_db_index_usage_stats AS sysUsageStats 
        INNER JOIN  
            audit.dm_db_index_usage_stats AS auditUsageStats
                ON auditUsageStats.database_id = sysUsageStats.database_id
                    AND auditUsageStats.object_id = sysUsageStats.object_id
                    AND auditUsageStats.index_id  = sysUsageStats.index_id ;
    END 

--  INSERT usage stats where none had previous existed for a given index 
INSERT INTO
    audit.dm_db_index_usage_stats( 
        database_id, object_id, index_id
            , user_seeks, user_scans, user_lookups, user_updates
            , last_user_seek, last_user_scan, last_user_lookup, last_user_update
            , system_seeks, system_scans, system_lookups, system_updates
            , last_system_seek, last_system_scan, last_system_lookup, last_system_update
            , last_poll_user_seeks, last_poll_user_scans, last_poll_user_lookups, last_poll_user_updates
            , last_poll_system_seeks, last_poll_system_scans, last_poll_system_lookups, last_poll_system_updates
            , date_stamp )
SELECT 
    database_id                 =   sysUsageStats.database_id                          
  , object_id                   =   sysUsageStats.object_id                   
  , index_id                    =   sysUsageStats.index_id                    
  , user_seeks                  =   sysUsageStats.user_seeks                  
  , user_scans                  =   sysUsageStats.user_scans                  
  , user_lookups                =   sysUsageStats.user_lookups                
  , user_updates                =   sysUsageStats.user_updates                
  , last_user_seek              =   sysUsageStats.last_user_seek              
  , last_user_scan              =   sysUsageStats.last_user_scan              
  , last_user_lookup            =   sysUsageStats.last_user_lookup            
  , last_user_update            =   sysUsageStats.last_user_update            
  , system_seeks                =   sysUsageStats.system_seeks                
  , system_scans                =   sysUsageStats.system_scans                
  , system_lookups              =   sysUsageStats.system_lookups              
  , system_updates              =   sysUsageStats.system_updates              
  , last_system_seek            =   sysUsageStats.last_system_seek            
  , last_system_scan            =   sysUsageStats.last_system_scan            
  , last_system_lookup          =   sysUsageStats.last_system_lookup          
  , last_system_update          =   sysUsageStats.last_system_update          
  , last_poll_user_seeks        =   sysUsageStats.user_seeks       
  , last_poll_user_scans        =   sysUsageStats.user_scans       
  , last_poll_user_lookups      =   sysUsageStats.user_lookups     
  , last_poll_user_updates      =   sysUsageStats.user_updates     
  , last_poll_system_seeks      =   sysUsageStats.system_seeks       
  , last_poll_system_scans      =   sysUsageStats.system_scans       
  , last_poll_system_lookups    =   sysUsageStats.system_lookups     
  , last_poll_system_updates    =   sysUsageStats.system_updates     
  , date_stamp                  =   GETDATE()    
FROM
    sys.dm_db_index_usage_stats AS sysUsageStats
WHERE 
    NOT EXISTS( 
        SELECT 1 FROM audit.dm_db_index_usage_stats AS auditUsageStats
        WHERE
            auditUsageStats.database_id = sysUsageStats.database_id
                AND auditUsageStats.object_id = sysUsageStats.object_id
                AND auditUsageStats.index_id = sysUsageStats.index_id ) ;                
                
RETURN