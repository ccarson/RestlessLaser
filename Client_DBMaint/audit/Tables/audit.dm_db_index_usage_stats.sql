CREATE TABLE [audit].[dm_db_index_usage_stats] (
    [database_id]              SMALLINT NOT NULL,
    [object_id]                INT      NOT NULL,
    [index_id]                 INT      NOT NULL,
    [user_seeks]               BIGINT   NOT NULL,
    [user_scans]               BIGINT   NOT NULL,
    [user_lookups]             BIGINT   NOT NULL,
    [user_updates]             BIGINT   NOT NULL,
    [last_user_seek]           DATETIME NULL,
    [last_user_scan]           DATETIME NULL,
    [last_user_lookup]         DATETIME NULL,
    [last_user_update]         DATETIME NULL,
    [system_seeks]             BIGINT   NOT NULL,
    [system_scans]             BIGINT   NOT NULL,
    [system_lookups]           BIGINT   NOT NULL,
    [system_updates]           BIGINT   NOT NULL,
    [last_system_seek]         DATETIME NULL,
    [last_system_scan]         DATETIME NULL,
    [last_system_lookup]       DATETIME NULL,
    [last_system_update]       DATETIME NULL,
    [last_poll_user_seeks]     BIGINT   NOT NULL,
    [last_poll_user_scans]     BIGINT   NOT NULL,
    [last_poll_user_lookups]   BIGINT   NOT NULL,
    [last_poll_user_updates]   BIGINT   NOT NULL,
    [last_poll_system_seeks]   BIGINT   NOT NULL,
    [last_poll_system_scans]   BIGINT   NOT NULL,
    [last_poll_system_lookups] BIGINT   NOT NULL,
    [last_poll_system_updates] BIGINT   NOT NULL,
    [date_stamp]               DATETIME NOT NULL,
    CONSTRAINT [PK_dm_db_index_usage_stats] PRIMARY KEY CLUSTERED ([database_id] ASC, [object_id] ASC, [index_id] ASC) WITH (FILLFACTOR = 90)
);


GO
CREATE NONCLUSTERED INDEX [IX_user_writes]
    ON [audit].[dm_db_index_usage_stats]([user_updates] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [IX_user_reads]
    ON [audit].[dm_db_index_usage_stats]([user_scans] ASC, [user_seeks] ASC, [user_lookups] ASC) WITH (FILLFACTOR = 80);

