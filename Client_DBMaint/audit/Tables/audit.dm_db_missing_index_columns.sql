CREATE TABLE [audit].[dm_db_missing_index_columns] (
    [index_handle] INT          NOT NULL,
    [column_id]    INT          NOT NULL,
    [column_name]  [sysname]    NOT NULL,
    [column_usage] VARCHAR (20) NOT NULL,
    [date_stamp]   DATETIME     NOT NULL,
    CONSTRAINT [PK_dm_db_missing_index_columns] PRIMARY KEY CLUSTERED ([index_handle] ASC, [column_id] ASC) WITH (FILLFACTOR = 100)
);

