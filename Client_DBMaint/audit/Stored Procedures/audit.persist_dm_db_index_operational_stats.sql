CREATE PROCEDURE 
    audit.persist_dm_db_index_operational_stats 
AS  
/*

    TODO:   Document header for proc

    CREDIT: Retaining historical index usage statistics for SQL Server 
            Tim Ford
            https://www.mssqltips.com/sqlservertip/1749/retaining-historical-index-usage-statistics-for-sql-server-part-1-of-3/

*/
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
    audit.dm_db_index_operational_stats ;

    
--Take care of updated records first 
IF( @last_service_start_date < @last_data_persist_date )
    BEGIN 
        UPDATE 
            persist
        SET 
            leaf_insert_count                   = persist.leaf_insert_count + ( sysStats.leaf_insert_count - persist.last_poll_leaf_insert_count ) 
          , leaf_delete_count                   = persist.leaf_delete_count + (sysStats.leaf_delete_count - persist.last_poll_leaf_delete_count) 
          , leaf_update_count                   = persist.leaf_update_count + (sysStats.leaf_update_count - persist.last_poll_leaf_update_count) 
          , leaf_ghost_count                    = persist.leaf_ghost_count + (sysStats.leaf_ghost_count - persist.last_poll_leaf_ghost_count) 
          , nonleaf_insert_count                = persist.nonleaf_insert_count + (sysStats.nonleaf_insert_count - persist.last_poll_nonleaf_insert_count) 
          , nonleaf_delete_count                = persist.nonleaf_delete_count + (sysStats.nonleaf_delete_count - persist.last_poll_nonleaf_delete_count) 
          , nonleaf_update_count                = persist.nonleaf_update_count + (sysStats.nonleaf_update_count - persist.last_poll_nonleaf_update_count) 
          , leaf_allocation_count               = persist.leaf_allocation_count + (sysStats.leaf_allocation_count - persist.last_poll_leaf_allocation_count) 
          , nonleaf_allocation_count            = persist.nonleaf_allocation_count + (sysStats.nonleaf_allocation_count - persist.last_poll_nonleaf_allocation_count) 
          , leaf_page_merge_count               = persist.leaf_page_merge_count + (sysStats.leaf_page_merge_count - persist.last_poll_leaf_page_merge_count) 
          , nonleaf_page_merge_count            = persist.nonleaf_page_merge_count + (sysStats.nonleaf_page_merge_count - persist.last_poll_nonleaf_page_merge_count) 
          , range_scan_count                    = persist.range_scan_count + (sysStats.range_scan_count - persist.last_poll_range_scan_count) 
          , singleton_lookup_count              = persist.singleton_lookup_count + (sysStats.singleton_lookup_count - persist.last_poll_singleton_lookup_count) 
          , forwarded_fetch_count               = persist.forwarded_fetch_count + (sysStats.forwarded_fetch_count - persist.last_poll_forwarded_fetch_count) 
          , lob_fetch_in_pages                  = persist.lob_fetch_in_pages + (sysStats.lob_fetch_in_pages - persist.last_poll_lob_fetch_in_pages) 
          , lob_fetch_in_bytes                  = persist.lob_fetch_in_bytes + (sysStats.lob_fetch_in_bytes - persist.last_poll_lob_fetch_in_bytes) 
          , lob_orphan_create_count             = persist.lob_orphan_create_count + (sysStats.lob_orphan_create_count - persist.last_poll_lob_orphan_create_count) 
          , lob_orphan_insert_count             = persist.lob_orphan_insert_count + (sysStats.lob_orphan_insert_count - persist.last_poll_lob_orphan_insert_count) 
          , row_overflow_fetch_in_pages         = persist.row_overflow_fetch_in_pages + (sysStats.row_overflow_fetch_in_pages - persist.last_poll_row_overflow_fetch_in_pages) 
          , row_overflow_fetch_in_bytes         = persist.row_overflow_fetch_in_bytes + (sysStats.row_overflow_fetch_in_bytes - persist.last_poll_row_overflow_fetch_in_bytes) 
          , column_value_push_off_row_count     = persist.column_value_push_off_row_count + (sysStats.column_value_push_off_row_count - persist.last_poll_column_value_push_off_row_count) 
          , column_value_pull_in_row_count      = persist.column_value_pull_in_row_count + (sysStats.column_value_pull_in_row_count - persist.last_poll_column_value_pull_in_row_count) 
          , row_lock_count                      = persist.row_lock_count + (sysStats.row_lock_count - persist.last_poll_row_lock_count) 
          , row_lock_wait_count                 = persist.row_lock_wait_count + (sysStats.row_lock_wait_count - persist.last_poll_row_lock_wait_count) 
          , row_lock_wait_in_ms                 = persist.row_lock_wait_in_ms + (sysStats.row_lock_wait_in_ms - persist.last_poll_row_lock_wait_in_ms) 
          , page_lock_count                     = persist.page_lock_count + (sysStats.page_lock_count - persist.last_poll_page_lock_count) 
          , page_lock_wait_count                = persist.page_lock_wait_count + (sysStats.page_lock_wait_count - persist.last_poll_page_lock_wait_count) 
          , page_lock_wait_in_ms                = persist.page_lock_wait_in_ms + (sysStats.page_lock_wait_in_ms - persist.last_poll_page_lock_wait_in_ms) 
          , index_lock_promotion_attempt_count  = persist.index_lock_promotion_attempt_count + (sysStats.index_lock_promotion_attempt_count - persist.last_poll_index_lock_promotion_attempt_count) 
          , index_lock_promotion_count          = persist.index_lock_promotion_count + (sysStats.index_lock_promotion_count - persist.last_poll_index_lock_promotion_count) 
          , page_latch_wait_count               = persist.page_latch_wait_count + (sysStats.page_latch_wait_count - persist.last_poll_page_latch_wait_count) 
          , page_latch_wait_in_ms               = persist.page_latch_wait_in_ms + (sysStats.page_latch_wait_in_ms - persist.last_poll_page_latch_wait_in_ms) 
          , page_io_latch_wait_count            = persist.page_io_latch_wait_count + (sysStats.page_io_latch_wait_count - persist.last_poll_page_io_latch_wait_count) 
          , page_io_latch_wait_in_ms            = persist.page_io_latch_wait_in_ms + (sysStats.page_io_latch_wait_in_ms - persist.last_poll_page_io_latch_wait_in_ms) 
          , last_poll_leaf_insert_count         = sysStats.leaf_insert_count 
          , last_poll_leaf_delete_count         = sysStats.leaf_delete_count 
          , last_poll_leaf_update_count         = sysStats.leaf_update_count 
          , last_poll_leaf_ghost_count          = sysStats.leaf_ghost_count 
          , last_poll_nonleaf_insert_count      = sysStats.nonleaf_insert_count 
          , last_poll_nonleaf_delete_count      = sysStats.nonleaf_delete_count 
          , last_poll_nonleaf_update_count      = sysStats.nonleaf_update_count 
          , last_poll_leaf_allocation_count     = sysStats.leaf_allocation_count 
          , last_poll_nonleaf_allocation_count  = sysStats.nonleaf_allocation_count 
          , last_poll_leaf_page_merge_count     = sysStats.leaf_page_merge_count 
          , last_poll_nonleaf_page_merge_count  = sysStats.nonleaf_page_merge_count 
          , last_poll_range_scan_count          = sysStats.range_scan_count 
          , last_poll_singleton_lookup_count    = sysStats.singleton_lookup_count 
          , last_poll_forwarded_fetch_count     = sysStats.forwarded_fetch_count 
          , last_poll_lob_fetch_in_pages        = sysStats.lob_fetch_in_pages 
          , last_poll_lob_fetch_in_bytes        = sysStats.lob_fetch_in_bytes 
          , last_poll_lob_orphan_create_count   = sysStats.lob_orphan_create_count 
          , last_poll_lob_orphan_insert_count           = sysStats.lob_orphan_insert_count 
          , last_poll_row_overflow_fetch_in_pages       = sysStats.row_overflow_fetch_in_pages 
          , last_poll_row_overflow_fetch_in_bytes       = sysStats.row_overflow_fetch_in_bytes 
          , last_poll_column_value_push_off_row_count   = sysStats.column_value_push_off_row_count 
          , last_poll_column_value_pull_in_row_count    = sysStats.column_value_pull_in_row_count 
          , last_poll_row_lock_count                    = sysStats.row_lock_count 
          , last_poll_row_lock_wait_count       = sysStats.row_lock_wait_count 
          , last_poll_row_lock_wait_in_ms       = sysStats.row_lock_wait_in_ms 
          , last_poll_page_lock_count           = sysStats.page_lock_count 
          , last_poll_page_lock_wait_count      = sysStats.page_lock_wait_count 
          , last_poll_page_lock_wait_in_ms                  = sysStats.page_lock_wait_in_ms 
          , last_poll_index_lock_promotion_attempt_count    = sysStats.index_lock_promotion_attempt_count 
          , last_poll_index_lock_promotion_count            = sysStats.index_lock_promotion_count 
          , last_poll_page_latch_wait_count     = sysStats.page_latch_wait_count 
          , last_poll_page_latch_wait_in_ms     = sysStats.page_latch_wait_in_ms 
          , last_poll_page_io_latch_wait_count  = sysStats.page_io_latch_wait_count 
          , last_poll_page_io_latch_wait_in_ms  = sysStats.page_io_latch_wait_in_ms       
          , date_stamp                          = GETDATE() 
        FROM 
            sys.dm_db_index_operational_stats( NULL, NULL, NULL, NULL ) AS sysStats
        INNER JOIN  
            audit.dm_db_index_operational_stats AS persist 
                ON sysStats.database_id             = persist.database_id
                    AND sysStats.object_id          = persist.object_id
                    AND sysStats.index_id           = persist.index_id 
                    AND sysStats.partition_number   = persist.partition_number
    END
ELSE
    BEGIN 
        UPDATE 
            persist
        SET  
            leaf_insert_count                   = persist.leaf_insert_count             + sysStats.leaf_insert_count                
          , leaf_delete_count                   = persist.leaf_delete_count             + sysStats.leaf_delete_count               
          , leaf_update_count                   = persist.leaf_update_count             + sysStats.leaf_update_count               
          , leaf_ghost_count                    = persist.leaf_ghost_count              + sysStats.leaf_ghost_count                 
          , nonleaf_insert_count                = persist.nonleaf_insert_count          + sysStats.nonleaf_insert_count         
          , nonleaf_delete_count                = persist.nonleaf_delete_count          + sysStats.nonleaf_delete_count         
          , nonleaf_update_count                = persist.nonleaf_update_count          + sysStats.nonleaf_update_count         
          , leaf_allocation_count               = persist.leaf_allocation_count         + sysStats.leaf_allocation_count        
          , nonleaf_allocation_count            = persist.nonleaf_allocation_count      + sysStats.nonleaf_allocation_count  
          , leaf_page_merge_count               = persist.leaf_page_merge_count         + sysStats.leaf_page_merge_count                
          , nonleaf_page_merge_count            = persist.nonleaf_page_merge_count      + sysStats.nonleaf_page_merge_count             
          , range_scan_count                    = persist.range_scan_count              + sysStats.range_scan_count                     
          , singleton_lookup_count              = persist.singleton_lookup_count        + sysStats.singleton_lookup_count               
          , forwarded_fetch_count               = persist.forwarded_fetch_count         + sysStats.forwarded_fetch_count                
          , lob_fetch_in_pages                  = persist.lob_fetch_in_pages            + sysStats.lob_fetch_in_pages                   
          , lob_fetch_in_bytes                  = persist.lob_fetch_in_bytes            + sysStats.lob_fetch_in_bytes                   
          , lob_orphan_create_count             = persist.lob_orphan_create_count       + sysStats.lob_orphan_create_count              
          , lob_orphan_insert_count             = persist.lob_orphan_insert_count       + sysStats.lob_orphan_insert_count              
          , row_overflow_fetch_in_pages         = persist.row_overflow_fetch_in_pages   + sysStats.row_overflow_fetch_in_pages          
          , row_overflow_fetch_in_bytes         = persist.row_overflow_fetch_in_bytes       + sysStats.row_overflow_fetch_in_bytes      
          , column_value_push_off_row_count     = persist.column_value_push_off_row_count   + sysStats.column_value_push_off_row_count  
          , column_value_pull_in_row_count      = persist.column_value_pull_in_row_count    + sysStats.column_value_pull_in_row_count   
          , row_lock_count                      = persist.row_lock_count                + sysStats.row_lock_count                       
          , row_lock_wait_count                 = persist.row_lock_wait_count           + sysStats.row_lock_wait_count                  
          , row_lock_wait_in_ms                 = persist.row_lock_wait_in_ms           + sysStats.row_lock_wait_in_ms                  
          , page_lock_count                     = persist.page_lock_count               + sysStats.page_lock_count                      
          , page_lock_wait_count                = persist.page_lock_wait_count          + sysStats.page_lock_wait_count                 
          , page_lock_wait_in_ms                = persist.page_lock_wait_in_ms                  + sysStats.page_lock_wait_in_ms         
          , index_lock_promotion_attempt_count  = persist.index_lock_promotion_attempt_count    + sysStats.index_lock_promotion_attempt_count 
          , index_lock_promotion_count          = persist.index_lock_promotion_count            + sysStats.index_lock_promotion_count   
          , page_latch_wait_count               = persist.page_latch_wait_count         + sysStats.page_latch_wait_count                
          , page_latch_wait_in_ms               = persist.page_latch_wait_in_ms         + sysStats.page_latch_wait_in_ms                
          , page_io_latch_wait_count            = persist.page_io_latch_wait_count      + sysStats.page_io_latch_wait_count             
          , page_io_latch_wait_in_ms            = persist.page_io_latch_wait_in_ms      + sysStats.page_io_latch_wait_in_ms             
          , last_poll_leaf_insert_count         = sysStats.leaf_insert_count 
          , last_poll_leaf_delete_count         = sysStats.leaf_delete_count 
          , last_poll_leaf_update_count         = sysStats.leaf_update_count 
          , last_poll_leaf_ghost_count          = sysStats.leaf_ghost_count 
          , last_poll_nonleaf_insert_count      = sysStats.nonleaf_insert_count 
          , last_poll_nonleaf_delete_count      = sysStats.nonleaf_delete_count 
          , last_poll_nonleaf_update_count      = sysStats.nonleaf_update_count 
          , last_poll_leaf_allocation_count     = sysStats.leaf_allocation_count 
          , last_poll_nonleaf_allocation_count  = sysStats.nonleaf_allocation_count 
          , last_poll_leaf_page_merge_count     = sysStats.leaf_page_merge_count 
          , last_poll_nonleaf_page_merge_count  = sysStats.nonleaf_page_merge_count 
          , last_poll_range_scan_count          = sysStats.range_scan_count 
          , last_poll_singleton_lookup_count    = sysStats.singleton_lookup_count 
          , last_poll_forwarded_fetch_count     = sysStats.forwarded_fetch_count 
          , last_poll_lob_fetch_in_pages        = sysStats.lob_fetch_in_pages 
          , last_poll_lob_fetch_in_bytes        = sysStats.lob_fetch_in_bytes 
          , last_poll_lob_orphan_create_count   = sysStats.lob_orphan_create_count 
          , last_poll_lob_orphan_insert_count           = sysStats.lob_orphan_insert_count 
          , last_poll_row_overflow_fetch_in_pages       = sysStats.row_overflow_fetch_in_pages 
          , last_poll_row_overflow_fetch_in_bytes       = sysStats.row_overflow_fetch_in_bytes 
          , last_poll_column_value_push_off_row_count   = sysStats.column_value_push_off_row_count 
          , last_poll_column_value_pull_in_row_count    = sysStats.column_value_pull_in_row_count 
          , last_poll_row_lock_count                    = sysStats.row_lock_count 
          , last_poll_row_lock_wait_count       = sysStats.row_lock_wait_count 
          , last_poll_row_lock_wait_in_ms       = sysStats.row_lock_wait_in_ms 
          , last_poll_page_lock_count           = sysStats.page_lock_count 
          , last_poll_page_lock_wait_count      = sysStats.page_lock_wait_count 
          , last_poll_page_lock_wait_in_ms                  = sysStats.page_lock_wait_in_ms 
          , last_poll_index_lock_promotion_attempt_count    = sysStats.index_lock_promotion_attempt_count 
          , last_poll_index_lock_promotion_count            = sysStats.index_lock_promotion_count 
          , last_poll_page_latch_wait_count     = sysStats.page_latch_wait_count 
          , last_poll_page_latch_wait_in_ms     = sysStats.page_latch_wait_in_ms 
          , last_poll_page_io_latch_wait_count  = sysStats.page_io_latch_wait_count 
          , last_poll_page_io_latch_wait_in_ms  = sysStats.page_io_latch_wait_in_ms       
          , date_stamp                          = GETDATE() 

        FROM 
            sys.dm_db_index_operational_stats( NULL, NULL, NULL, NULL ) AS sysStats
        INNER JOIN  
            audit.dm_db_index_operational_stats AS persist 
                ON sysStats.database_id             = persist.database_id
                    AND sysStats.object_id          = persist.object_id
                    AND sysStats.index_id           = persist.index_id 
                    AND sysStats.partition_number   = persist.partition_number
    END

--Take care of new records next 
INSERT INTO
    audit.dm_db_index_operational_stats( 
        database_id, object_id, index_id, partition_number, leaf_insert_count, leaf_delete_count, leaf_update_count, leaf_ghost_count
            , nonleaf_insert_count, nonleaf_delete_count, nonleaf_update_count, leaf_allocation_count, nonleaf_allocation_count
            , leaf_page_merge_count, nonleaf_page_merge_count, range_scan_count, singleton_lookup_count, forwarded_fetch_count
            , lob_fetch_in_pages, lob_fetch_in_bytes, lob_orphan_create_count, lob_orphan_insert_count, row_overflow_fetch_in_pages
            , row_overflow_fetch_in_bytes, column_value_push_off_row_count, column_value_pull_in_row_count, row_lock_count
            , row_lock_wait_count, row_lock_wait_in_ms, page_lock_count, page_lock_wait_count, page_lock_wait_in_ms
            , index_lock_promotion_attempt_count, index_lock_promotion_count, page_latch_wait_count, page_latch_wait_in_ms
            , page_io_latch_wait_count, page_io_latch_wait_in_ms
            , last_poll_leaf_insert_count, last_poll_leaf_delete_count, last_poll_leaf_update_count, last_poll_leaf_ghost_count
            , last_poll_nonleaf_insert_count, last_poll_nonleaf_delete_count, last_poll_nonleaf_update_count
            , last_poll_leaf_allocation_count, last_poll_nonleaf_allocation_count, last_poll_leaf_page_merge_count, last_poll_nonleaf_page_merge_count
            , last_poll_range_scan_count, last_poll_singleton_lookup_count, last_poll_forwarded_fetch_count, last_poll_lob_fetch_in_pages
            , last_poll_lob_fetch_in_bytes, last_poll_lob_orphan_create_count, last_poll_lob_orphan_insert_count
            , last_poll_row_overflow_fetch_in_pages, last_poll_row_overflow_fetch_in_bytes, last_poll_column_value_push_off_row_count
            , last_poll_column_value_pull_in_row_count, last_poll_row_lock_count, last_poll_row_lock_wait_count, last_poll_row_lock_wait_in_ms
            , last_poll_page_lock_count, last_poll_page_lock_wait_count, last_poll_page_lock_wait_in_ms, last_poll_index_lock_promotion_attempt_count
            , last_poll_index_lock_promotion_count, last_poll_page_latch_wait_count, last_poll_page_latch_wait_in_ms, last_poll_page_io_latch_wait_count
            , last_poll_page_io_latch_wait_in_ms, date_stamp )
SELECT 
    database_id                                     =   sysStats.database_id
  , object_id                                       =   sysStats.object_id
  , index_id                                        =   sysStats.index_id
  , partition_number                                =   sysStats.partition_number
  , leaf_insert_count                               =   sysStats.leaf_insert_count
  , leaf_delete_count                               =   sysStats.leaf_delete_count
  , leaf_update_count                               =   sysStats.leaf_update_count
  , leaf_ghost_count                                =   sysStats.leaf_ghost_count
  , nonleaf_insert_count                            =   sysStats.nonleaf_insert_count
  , nonleaf_delete_count                            =   sysStats.nonleaf_delete_count
  , nonleaf_update_count                            =   sysStats.nonleaf_update_count
  , leaf_allocation_count                           =   sysStats.leaf_allocation_count
  , nonleaf_allocation_count                        =   sysStats.nonleaf_allocation_count
  , leaf_page_merge_count                           =   sysStats.leaf_page_merge_count
  , nonleaf_page_merge_count                        =   sysStats.nonleaf_page_merge_count
  , range_scan_count                                =   sysStats.range_scan_count
  , singleton_lookup_count                          =   sysStats.singleton_lookup_count
  , forwarded_fetch_count                           =   sysStats.forwarded_fetch_count
  , lob_fetch_in_pages                              =   sysStats.lob_fetch_in_pages
  , lob_fetch_in_bytes                              =   sysStats.lob_fetch_in_bytes
  , lob_orphan_create_count                         =   sysStats.lob_orphan_create_count
  , lob_orphan_insert_count                         =   sysStats.lob_orphan_insert_count
  , row_overflow_fetch_in_pages                     =   sysStats.row_overflow_fetch_in_pages
  , row_overflow_fetch_in_bytes                     =   sysStats.row_overflow_fetch_in_bytes
  , column_value_push_off_row_count                 =   sysStats.column_value_push_off_row_count
  , column_value_pull_in_row_count                  =   sysStats.column_value_pull_in_row_count
  , row_lock_count                                  =   sysStats.row_lock_count
  , row_lock_wait_count                             =   sysStats.row_lock_wait_count
  , row_lock_wait_in_ms                             =   sysStats.row_lock_wait_in_ms
  , page_lock_count                                 =   sysStats.page_lock_count
  , page_lock_wait_count                            =   sysStats.page_lock_wait_count
  , page_lock_wait_in_ms                            =   sysStats.page_lock_wait_in_ms
  , index_lock_promotion_attempt_count              =   sysStats.index_lock_promotion_attempt_count
  , index_lock_promotion_count                      =   sysStats.index_lock_promotion_count
  , page_latch_wait_count                           =   sysStats.page_latch_wait_count
  , page_latch_wait_in_ms                           =   sysStats.page_latch_wait_in_ms
  , page_io_latch_wait_count                        =   sysStats.page_io_latch_wait_count
  , page_io_latch_wait_in_ms                        =   sysStats.page_io_latch_wait_in_ms
  , last_poll_leaf_insert_count                     =   sysStats.leaf_insert_count
  , last_poll_leaf_delete_count                     =   sysStats.leaf_delete_count
  , last_poll_leaf_update_count                     =   sysStats.leaf_update_count
  , last_poll_leaf_ghost_count                      =   sysStats.leaf_ghost_count
  , last_poll_nonleaf_insert_count                  =   sysStats.nonleaf_insert_count
  , last_poll_nonleaf_delete_count                  =   sysStats.nonleaf_delete_count
  , last_poll_nonleaf_update_count                  =   sysStats.nonleaf_update_count
  , last_poll_leaf_allocation_count                 =   sysStats.leaf_allocation_count
  , last_poll_nonleaf_allocation_count              =   sysStats.nonleaf_allocation_count
  , last_poll_leaf_page_merge_count                 =   sysStats.leaf_page_merge_count
  , last_poll_nonleaf_page_merge_count              =   sysStats.nonleaf_page_merge_count
  , last_poll_range_scan_count                      =   sysStats.range_scan_count
  , last_poll_singleton_lookup_count                =   sysStats.singleton_lookup_count
  , last_poll_forwarded_fetch_count                 =   sysStats.forwarded_fetch_count
  , last_poll_lob_fetch_in_pages                    =   sysStats.lob_fetch_in_pages
  , last_poll_lob_fetch_in_bytes                    =   sysStats.lob_fetch_in_bytes
  , last_poll_lob_orphan_create_count               =   sysStats.lob_orphan_create_count
  , last_poll_lob_orphan_insert_count               =   sysStats.lob_orphan_insert_count
  , last_poll_row_overflow_fetch_in_pages           =   sysStats.row_overflow_fetch_in_pages
  , last_poll_row_overflow_fetch_in_bytes           =   sysStats.row_overflow_fetch_in_bytes
  , last_poll_column_value_push_off_row_count       =   sysStats.column_value_push_off_row_count
  , last_poll_column_value_pull_in_row_count        =   sysStats.column_value_pull_in_row_count
  , last_poll_row_lock_count                        =   sysStats.row_lock_count
  , last_poll_row_lock_wait_count                   =   sysStats.row_lock_wait_count
  , last_poll_row_lock_wait_in_ms                   =   sysStats.row_lock_wait_in_ms
  , last_poll_page_lock_count                       =   sysStats.page_lock_count
  , last_poll_page_lock_wait_count                  =   sysStats.page_lock_wait_count
  , last_poll_page_lock_wait_in_ms                  =   sysStats.page_lock_wait_in_ms
  , last_poll_index_lock_promotion_attempt_count    =   sysStats.index_lock_promotion_attempt_count
  , last_poll_index_lock_promotion_count            =   sysStats.index_lock_promotion_count
  , last_poll_page_latch_wait_count                 =   sysStats.page_latch_wait_count
  , last_poll_page_latch_wait_in_ms                 =   sysStats.page_latch_wait_in_ms
  , last_poll_page_io_latch_wait_count              =   sysStats.page_io_latch_wait_count
  , last_poll_page_io_latch_wait_in_ms              =   sysStats.page_io_latch_wait_in_ms
  , date_stamp                                      =   GETDATE() 
FROM 
    sys.dm_db_index_operational_stats( NULL, NULL, NULL, NULL) AS sysStats
LEFT JOIN 
    audit.dm_db_index_operational_stats AS persist 
        ON sysStats.database_id             = persist.database_id
            AND sysStats.object_id          = persist.object_id
            AND sysStats.index_id           = persist.index_id 
            AND sysStats.partition_number   = persist.partition_number
WHERE 
    persist.database_id IS NULL  ; 
    
RETURN ;