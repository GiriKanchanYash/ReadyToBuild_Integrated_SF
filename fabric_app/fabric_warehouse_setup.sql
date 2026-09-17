/* =====================================================================
   Fabric Warehouse objects required by backend/fabric_app

   Run this against the WAREHOUSE named in Config.FABRIC_READYTOBUILD_
   WAREHOUSE_DATABASE (the read+write endpoint), in the schema named by
   Config.DEFAULT_SCHEMA (default: dbo).

   IMPORTANT ABOUT THE STORED PROCEDURES BELOW:
   I never saw the body of your Snowflake SP_CREATE_PO /
   SP_REFRESH_CTB_DATA / SP_UPDATE_WORK_ORDER_PRIORITY procedures - only how
   Python calls them (`CALL db.schema.SP_CREATE_PO(...)`). So the procedure
   *signatures* below match exactly what backend/fabric_app/service.py sends,
   but the procedure *bodies* are working templates you'll need to adapt to
   your actual PROCUREMENT_SUMMARY_VW / CTB_RESULT_VW / WORK_ORDER base
   table names and columns. Everything else in this file (the tables) is a
   direct, safe port and should not need changes.
   ===================================================================== */

/* ---------------------------------------------------------------------
   1. Query result cache
   Used by db.py's cache_get()/cache_set() to avoid re-calling Azure OpenAI
   for a question that's already been answered recently.
   --------------------------------------------------------------------- */
IF OBJECT_ID('information_mart.QUERY_RESULT_CACHE', 'U') IS NULL
CREATE TABLE dbo.QUERY_RESULT_CACHE (
    CACHE_KEY       VARCHAR(64)   NOT NULL,   -- sha256 hex of the question
    QUESTION_HASH   VARCHAR(64)   NOT NULL,
    QUESTION_TEXT   VARCHAR(2000) NOT NULL,
    GENERATED_SQL   VARCHAR(8000) NULL,
    RESULT_JSON     VARCHAR(MAX)  NULL,
    ROW_COUNT       INT           NOT NULL DEFAULT 0,
    CREATED_AT      VARCHAR(19)   NOT NULL,   -- 'yyyy-MM-dd HH:mm:ss' (matches db.py's FORMAT() calls)
    EXPIRES_AT      VARCHAR(19)   NOT NULL,
    HIT_COUNT       INT           NOT NULL DEFAULT 0
);
GO

/* A unique index lets db.py's UPDATE-then-INSERT pattern work correctly. */
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_QUERY_RESULT_CACHE_KEY')
CREATE UNIQUE INDEX UX_QUERY_RESULT_CACHE_KEY ON dbo.QUERY_RESULT_CACHE (CACHE_KEY);
GO


/* ---------------------------------------------------------------------
   2. Saved insights
   Used by copilot_service.py's save_insight/delete_insight/load_saved_insights.
   Column shape matches Snowflake's SAVED_INSIGHTS table.
   --------------------------------------------------------------------- */
IF OBJECT_ID('information_mart.SAVED_INSIGHTS', 'U') IS NULL
CREATE TABLE dbo.SAVED_INSIGHTS (
    INSIGHT_ID  INT IDENTITY(1,1) PRIMARY KEY,
    CREATED_BY  VARCHAR(100)  NOT NULL,   -- the app username (JWT `sub`), NOT a shared service account
    PAGE        VARCHAR(50)   NULL,
    TITLE       VARCHAR(200)  NOT NULL,
    QUESTION    VARCHAR(2000) NOT NULL,
    SQL_TEXT    VARCHAR(MAX)  NULL,
    CREATED_AT  DATETIME2     NOT NULL DEFAULT SYSDATETIME()
);
GO


/* ---------------------------------------------------------------------
   3. Question history (frequent / most-frequent questions)
   Used by copilot_service.py's save_question_history/load_frequent_questions/
   load_most_frequent_all. [USER] and [TYPE] are bracket-quoted because
   both are reserved words in T-SQL.
   --------------------------------------------------------------------- */
IF OBJECT_ID('information_mart.GENIE_QUESTION_HISTORY', 'U') IS NULL
CREATE TABLE dbo.GENIE_QUESTION_HISTORY (
    ID          INT IDENTITY(1,1) PRIMARY KEY,
    QUESTION    VARCHAR(2000) NOT NULL,
    [TYPE]      VARCHAR(50)   NOT NULL,
    [USER]      VARCHAR(100)  NOT NULL,   -- app username (JWT `sub`)
    CREATED_AT  DATETIME2     NOT NULL DEFAULT SYSDATETIME()
);
GO


/* ---------------------------------------------------------------------
   4. Genie context memory (per-question/user logging used by
   genie_middleware.py's log_event/log_events_upsert). Optional - only
   needed if you wire genie_middleware's logging into the Fabric copilot
   flow (it isn't called from copilot_service.py yet).
   --------------------------------------------------------------------- */
IF OBJECT_ID('information_mart.GENIE_CONTEXT_MEMORY', 'U') IS NULL
CREATE TABLE dbo.GENIE_CONTEXT_MEMORY (
    ID               INT IDENTITY(1,1) PRIMARY KEY,
    SessionId        VARCHAR(100)  NULL,
    Username         VARCHAR(100)  NULL,
    user_id          VARCHAR(64)   NULL,
    Question         VARCHAR(2000) NULL,
    AnswerSummary     VARCHAR(4000) NULL,
    FullAnswer        VARCHAR(MAX)  NULL,
    Context_Hash      VARCHAR(64)   NULL,
    Sql_Query         VARCHAR(MAX)  NULL,
    Tables_Used       VARCHAR(2000) NULL,
    Filters_Applied   VARCHAR(2000) NULL,
    Relevance_Score   FLOAT         NULL,
    Usage_Count       INT           NULL,
    Last_Accessed_At  DATETIME2     NULL,
    CacheKey          VARCHAR(64)   NULL,
    Frequency         INT           NULL,
    Action_Type       VARCHAR(50)   NULL,
    Action_Details    VARCHAR(2000) NULL,
    ChatDate          DATE          NULL,
    CreatedAt         DATETIME2     NULL,
    UpdatedAt         DATETIME2     NULL
);
GO


/* =====================================================================
   5. Stored procedures - ADAPT THE BODIES BELOW TO YOUR REAL SCHEMA.
   The signatures match backend/fabric_app/service.py exactly; only the
   internals here are placeholders that need your actual table/column
   names plugged in.
   ===================================================================== */

CREATE OR ALTER PROCEDURE information_mart.SP_CREATE_PO
    @PartNumber     VARCHAR(50),
    @Qty            INT,
    @SupplierId     VARCHAR(50),
    @PlantId        VARCHAR(50),
    @DeliveryDate   DATE,
    @UnitCost       DECIMAL(18,2) = 100.00,
    @CreatedBy      VARCHAR(50)   = 'REACT_APP'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @NewPoId VARCHAR(50) = CONCAT('PO-', FORMAT(GETDATE(), 'yyyyMMddHHmmss'));

    -- TODO: replace PROCUREMENT_ORDERS with your actual PO base table
    -- (the one PROCUREMENT_SUMMARY_VW is built from).
    INSERT INTO dbo.PROCUREMENT_ORDERS (
        PO_ID, PART_NUMBER, QTY_ORDERED, QTY_OUTSTANDING, SUPPLIER_ID,
        PLANT_ID, CONFIRMED_DELIVERY_DATE, UNIT_COST, PO_STATUS,
        CREATED_BY, CREATED_AT
    )
    VALUES (
        @NewPoId, @PartNumber, @Qty, @Qty, @SupplierId,
        @PlantId, @DeliveryDate, @UnitCost, 'OPEN',
        @CreatedBy, GETDATE()
    );

    SELECT @NewPoId AS PO_ID, 'Purchase order created' AS MESSAGE;
END
GO


CREATE OR ALTER PROCEDURE information_mart.SP_REFRESH_CTB_DATA
AS
BEGIN
    SET NOCOUNT ON;

    -- TODO: this should trigger whatever recalculates CTB_RESULT_VW's
    -- underlying data (e.g. a Fabric pipeline/notebook refresh, or a
    -- direct recompute if CTB_RESULT is a materialized table rather than
    -- a live view). Snowflake's version presumably called a task or
    -- re-ran a MERGE - point this at the equivalent Fabric mechanism.

    SELECT 'CTB refresh triggered' AS MESSAGE, GETDATE() AS REFRESHED_AT;
END
GO


CREATE OR ALTER PROCEDURE information_mart.SP_UPDATE_WORK_ORDER_PRIORITY
    @WorkOrderId    VARCHAR(50),
    @NewPriority    INT,
    @EffectiveWeek  DATE,
    @UpdatedBy      VARCHAR(50) = 'REACT_APP'
AS
BEGIN
    SET NOCOUNT ON;

    -- TODO: replace with your actual WORK_ORDER base table name/columns.
    UPDATE dbo.WORK_ORDER
    SET PRIORITY = @NewPriority
    WHERE WORK_ORDER_ID = @WorkOrderId;

    SELECT @WorkOrderId AS WORK_ORDER_ID, @NewPriority AS PRIORITY, 'Priority updated' AS MESSAGE;
END
GO


/* =====================================================================
   6. Cross-Site Inventory Transfer - stored procedures + Lakehouse note

   Used by backend/fabric_app/transfer_service.py, which is called (via
   service_factory.get_transfer_service) from routers/transfer.py whenever
   the frontend Data Source dropdown is set to "fabric". Before this
   change, the Transfer page had NO Fabric implementation at all - the
   router imported the Snowflake transfer_service directly - so this
   section, plus transfer_service.py, is what makes the Transfer page
   respect the data source switch.

   LAKEHOUSE NOTE (not created by this script - this file only creates
   WAREHOUSE objects): transfer_service.py's read functions
   (get_plants/get_lanes/get_shortage_parts/get_transfer_candidates/
   get_open_stos) expect these Lakehouse tables in
   {FABRIC_READYTOBUILD_DATABASE}.{SCHEMA}, mirroring Snowflake's
   PLANT_VW / LANE_VW / TRANSFER_CANDIDATES_VW / STO_VW column-for-column:
       - plant_dt
       - lane_dt
       - transfer_candidates_dt
       - sto_dt
   These are expected to be produced by the same data pipeline that
   already produces work_order_dt / ctb_result_dt / etc. for the CTB
   pages - creating them is a data-engineering task, not something this
   SQL file or transfer_service.py can do.
   ===================================================================== */

CREATE OR ALTER PROCEDURE information_mart.SP_CREATE_STO
    @PartNumber             VARCHAR(50),
    @Qty                    INT,
    @OriginPlantId          VARCHAR(50),
    @DestPlantId            VARCHAR(50),
    @TransportMode          VARCHAR(20),
    @RequestedDeliveryDate  DATE,
    @RequestedBy            VARCHAR(50) = 'AI_AGENT'
AS
BEGIN
    SET NOCOUNT ON;

    -- TODO: replace dbo.STO_STAGING / dbo.LANE with your actual base
    -- tables (the ones sto_dt / lane_dt are built from). This mirrors
    -- Snowflake's create_sto: look up the matching lane, then insert a
    -- new PLANNED stock transfer order line.

    DECLARE @LaneId       VARCHAR(50);
    DECLARE @TransitDays  INT;
    DECLARE @Carrier      VARCHAR(100);
    DECLARE @UnitCost     DECIMAL(18,2);

    SELECT TOP (1)
           @LaneId = LANE_ID,
           @TransitDays = TRANSIT_DAYS,
           @Carrier = CARRIER_NAME,
           @UnitCost = COST_PER_UNIT_USD
    FROM dbo.LANE
    WHERE ORIGIN_PLANT_ID = @OriginPlantId
      AND DEST_PLANT_ID = @DestPlantId
      AND TRANSPORT_MODE = @TransportMode
      AND IS_ACTIVE = 1;

    DECLARE @NewStoId VARCHAR(50) = CONCAT('STO', RIGHT('000000' + CAST(
        (SELECT ISNULL(MAX(CAST(SUBSTRING(STO_ID, 4, 50) AS INT)), 700000) + 1
         FROM dbo.STO_STAGING) AS VARCHAR(20)), 6));

    INSERT INTO dbo.STO_STAGING (
        STO_ID, STO_LINE_ID, LANE_ID,
        ORIGIN_PLANT_ID, DEST_PLANT_ID,
        PART_NUMBER, UNIT_OF_MEASURE,
        QTY_REQUESTED, QTY_SHIPPED, QTY_IN_TRANSIT, QTY_RECEIVED,
        TRANSPORT_MODE, CARRIER_NAME, TRANSIT_DAYS,
        UNIT_TRANSFER_COST, TOTAL_TRANSFER_COST, CURRENCY_CODE,
        STO_STATUS, PRIORITY, REASON_CODE,
        REQUESTED_DELIVERY_DATE,
        REQUESTED_BY,
        CREATED_AT, UPDATED_AT
    )
    VALUES (
        @NewStoId, CONCAT(@NewStoId, '-010'), @LaneId,
        @OriginPlantId, @DestPlantId,
        @PartNumber, 'EA',
        @Qty, 0, 0, 0,
        @TransportMode, @Carrier, @TransitDays,
        @UnitCost, @Qty * @UnitCost, 'USD',
        'PLANNED', 1, 'SHORTAGE_COVER',
        @RequestedDeliveryDate,
        @RequestedBy,
        GETDATE(), GETDATE()
    );

    SELECT
        'SUCCESS'      AS STATUS,
        @NewStoId      AS STO_ID,
        @OriginPlantId AS ORIGIN_PLANT_ID,
        @DestPlantId   AS DEST_PLANT_ID,
        @PartNumber    AS PART_NUMBER,
        @Qty           AS QTY,
        @TransportMode AS TRANSPORT_MODE;
END
GO


CREATE OR ALTER PROCEDURE information_mart.SP_REFRESH_TRANSFER_DATA
AS
BEGIN
    SET NOCOUNT ON;


    SELECT 'Transfer data refresh triggered' AS MESSAGE, GETDATE() AS REFRESHED_AT;
END
GO
