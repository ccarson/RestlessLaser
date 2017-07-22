CREATE TABLE [audit].[dm_db_missing_index_details] (
    [index_handle]       INT             NOT NULL,
    [database_id]        SMALLINT        NOT NULL,
    [object_id]          INT             NOT NULL,
    [equality_columns]   NVARCHAR (4000) NULL,
    [inequality_columns] NVARCHAR (4000) NULL,
    [included_columns]   NVARCHAR (4000) NULL,
    [statement]          NVARCHAR (4000) NULL,
    [date_stamp]         DATETIME        NOT NULL,
    CONSTRAINT [PK_dm_db_missing_index_details] PRIMARY KEY CLUSTERED ([index_handle] ASC) WITH (FILLFACTOR = 100)
);

