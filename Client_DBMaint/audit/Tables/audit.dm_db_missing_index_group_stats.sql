CREATE TABLE [audit].[dm_db_missing_index_group_stats] (
    [group_handle]              INT        NOT NULL,
    [unique_compiles]           BIGINT     NOT NULL,
    [user_seeks]                BIGINT     NOT NULL,
    [user_scans]                BIGINT     NOT NULL,
    [last_user_seek]            DATETIME   NULL,
    [last_user_scan]            DATETIME   NULL,
    [avg_total_user_cost]       FLOAT (53) NULL,
    [avg_user_impact]           FLOAT (53) NULL,
    [system_seeks]              BIGINT     NOT NULL,
    [system_scans]              BIGINT     NOT NULL,
    [last_system_seek]          DATETIME   NULL,
    [last_system_scan]          DATETIME   NULL,
    [avg_total_system_cost]     FLOAT (53) NULL,
    [avg_system_impact]         FLOAT (53) NULL,
    [last_poll_unique_compiles] BIGINT     NULL,
    [last_poll_user_seeks]      BIGINT     NULL,
    [last_poll_user_scans]      BIGINT     NULL,
    [last_poll_system_seeks]    BIGINT     NULL,
    [last_poll_system_scans]    BIGINT     NULL,
    [date_stamp]                DATETIME   NOT NULL,
    CONSTRAINT [PK_dm_db_missing_index_group_stats] PRIMARY KEY CLUSTERED ([group_handle] ASC) WITH (FILLFACTOR = 90)
);

