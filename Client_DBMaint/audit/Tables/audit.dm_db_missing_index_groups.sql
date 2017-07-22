CREATE TABLE [audit].[dm_db_missing_index_groups] (
    [index_group_handle] INT      NOT NULL,
    [index_handle]       INT      NOT NULL,
    [date_stamp]         DATETIME NOT NULL,
    CONSTRAINT [PK_dm_db_missing_index_groups] PRIMARY KEY CLUSTERED ([index_group_handle] ASC, [index_handle] ASC) WITH (FILLFACTOR = 90)
);

