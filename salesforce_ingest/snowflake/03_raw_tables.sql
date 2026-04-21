-- snowflake/03_raw_tables.sql
--
-- Raw tables in RAW.SALESFORCE.
--
-- Design pattern: VARIANT + typed extraction columns.
--
--   RAW_DATA     VARIANT  — full JSON record, exactly as Lambda wrote it.
--                           This is the source of truth for replays.
--   Typed columns          — pre-extracted scalars for query performance and
--                           Snowpipe COPY INTO column mapping.
--   _LOAD_*      columns  — pipeline metadata: when, from where, which pipe.
--
-- Why keep RAW_DATA as VARIANT?
--   • Zero-schema-change risk when Salesforce adds custom fields — they land
--     in RAW_DATA automatically without any Snowflake DDL changes.
--   • dbt staging models can extract new fields immediately by querying
--     RAW_DATA:new_field::varchar.
--   • Full replay: if a staging transform has a bug, drop the staging table
--     and rebuild from RAW_DATA — no need to re-extract from Salesforce.
--
-- Snowpipe COPY INTO uses column mapping (copy_options MATCH_BY_COLUMN_NAME)
-- to populate typed columns; unknown fields flow into RAW_DATA.

USE ROLE SYSADMIN;
USE DATABASE RAW;
USE SCHEMA   SALESFORCE;


-- ────────────────────────────────────────────────────────────────────────────
-- Helper macro: every raw table has the same metadata footer
-- ────────────────────────────────────────────────────────────────────────────
-- (Snowflake doesn't support macros in SQL; these comments document the pattern
--  applied consistently across all CREATE TABLE statements below.)
--
-- Metadata columns (appended to every table):
--   RAW_DATA              VARIANT      Full JSON record
--   _EXTRACT_TIMESTAMP    TIMESTAMP_NTZ  When Lambda ran
--   _SOURCE_OBJECT        VARCHAR      Salesforce object name
--   _LOAD_TIMESTAMP       TIMESTAMP_NTZ  When Snowpipe ingested the file
--   _FILE_NAME            VARCHAR      S3 key of the source file
--   _FILE_ROW_NUMBER      NUMBER       Row position within the file

-- ── ACCOUNT ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ACCOUNT (
    -- Natural key
    ID                          VARCHAR(18)     NOT NULL,

    -- Core fields (pre-extracted for query perf)
    NAME                        VARCHAR(255),
    TYPE                        VARCHAR(100),
    INDUSTRY                    VARCHAR(100),
    SUB_INDUSTRY__C             VARCHAR(100),
    RATING                      VARCHAR(50),
    ACCOUNTSOURCE               VARCHAR(100),
    DESCRIPTION                 VARCHAR(32000),
    WEBSITE                     VARCHAR(255),
    PHONE                       VARCHAR(40),
    FAX                         VARCHAR(40),
    ANNUALREVENUE               NUMBER(38,2),
    NUMBEROFEMPLOYEES           NUMBER(10),
    BILLINGSTREET               VARCHAR(255),
    BILLINGCITY                 VARCHAR(100),
    BILLINGSTATE                VARCHAR(100),
    BILLINGPOSTALCODE           VARCHAR(20),
    BILLINGCOUNTRY              VARCHAR(100),
    BILLINGLATITUDE             FLOAT,
    BILLINGLONGITUDE            FLOAT,
    PARENTID                    VARCHAR(18),
    MASTERRECORDID              VARCHAR(18),
    OWNERID                     VARCHAR(18),
    ISDELETED                   BOOLEAN,
    CREATEDDATE                 TIMESTAMP_NTZ,
    LASTMODIFIEDDATE            TIMESTAMP_NTZ,
    SYSTEMMODSTAMP              TIMESTAMP_NTZ,

    -- Full record (schema-agnostic replay source)
    RAW_DATA                    VARIANT,

    -- Pipeline metadata
    _EXTRACT_TIMESTAMP          TIMESTAMP_NTZ,
    _SOURCE_OBJECT              VARCHAR(100),
    _LOAD_TIMESTAMP             TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _FILE_NAME                  VARCHAR(1000),
    _FILE_ROW_NUMBER            NUMBER(18)
)
CLUSTER BY (DATE_TRUNC('DAY', SYSTEMMODSTAMP))
COMMENT = 'Raw Salesforce Account records — append+merge via Snowpipe';


-- ── CONTACT ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS CONTACT (
    ID                          VARCHAR(18)     NOT NULL,
    FIRSTNAME                   VARCHAR(100),
    LASTNAME                    VARCHAR(100),
    NAME                        VARCHAR(200),
    SALUTATION                  VARCHAR(40),
    TITLE                       VARCHAR(128),
    DEPARTMENT                  VARCHAR(100),
    ACCOUNTID                   VARCHAR(18),
    REPORTSTOID                 VARCHAR(18),
    OWNERID                     VARCHAR(18),
    EMAIL                       VARCHAR(320),
    PHONE                       VARCHAR(40),
    MOBILEPHONE                 VARCHAR(40),
    FAX                         VARCHAR(40),
    LINKEDIN_URL__C             VARCHAR(500),
    LEADSOURCE                  VARCHAR(100),
    MAILINGSTREET               VARCHAR(255),
    MAILINGCITY                 VARCHAR(100),
    MAILINGSTATE                VARCHAR(100),
    MAILINGPOSTALCODE           VARCHAR(20),
    MAILINGCOUNTRY              VARCHAR(100),
    HASOPTEDOUTOFEMAIL          BOOLEAN,
    DONOTCALL                   BOOLEAN,
    BIRTHDATE                   DATE,
    ISDELETED                   BOOLEAN,
    CREATEDDATE                 TIMESTAMP_NTZ,
    LASTMODIFIEDDATE            TIMESTAMP_NTZ,
    SYSTEMMODSTAMP              TIMESTAMP_NTZ,
    LASTACTIVITYDATE            DATE,
    RAW_DATA                    VARIANT,
    _EXTRACT_TIMESTAMP          TIMESTAMP_NTZ,
    _SOURCE_OBJECT              VARCHAR(100),
    _LOAD_TIMESTAMP             TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _FILE_NAME                  VARCHAR(1000),
    _FILE_ROW_NUMBER            NUMBER(18)
)
CLUSTER BY (DATE_TRUNC('DAY', SYSTEMMODSTAMP))
COMMENT = 'Raw Salesforce Contact records';


-- ── LEAD ────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS LEAD (
    ID                          VARCHAR(18)     NOT NULL,
    FIRSTNAME                   VARCHAR(100),
    LASTNAME                    VARCHAR(100),
    NAME                        VARCHAR(200),
    SALUTATION                  VARCHAR(40),
    TITLE                       VARCHAR(128),
    EMAIL                       VARCHAR(320),
    PHONE                       VARCHAR(40),
    MOBILEPHONE                 VARCHAR(40),
    COMPANY                     VARCHAR(255),
    INDUSTRY                    VARCHAR(100),
    ANNUALREVENUE               NUMBER(38,2),
    NUMBEROFEMPLOYEES           NUMBER(10),
    WEBSITE                     VARCHAR(255),
    STATUS                      VARCHAR(100),
    LEADSOURCE                  VARCHAR(100),
    RATING                      VARCHAR(50),
    STREET                      VARCHAR(255),
    CITY                        VARCHAR(100),
    STATE                       VARCHAR(100),
    POSTALCODE                  VARCHAR(20),
    COUNTRY                     VARCHAR(100),
    ISCONVERTED                 BOOLEAN,
    CONVERTEDDATE               DATE,
    CONVERTEDACCOUNTID          VARCHAR(18),
    CONVERTEDCONTACTID          VARCHAR(18),
    CONVERTEDOPPORTUNITYID      VARCHAR(18),
    OWNERID                     VARCHAR(18),
    CAMPAIGNID                  VARCHAR(18),
    HASOPTEDOUTOFEMAIL          BOOLEAN,
    DONOTCALL                   BOOLEAN,
    LEAD_SCORE__C               NUMBER(5),
    LEAD_GRADE__C               VARCHAR(10),
    ISDELETED                   BOOLEAN,
    CREATEDDATE                 TIMESTAMP_NTZ,
    LASTMODIFIEDDATE            TIMESTAMP_NTZ,
    SYSTEMMODSTAMP              TIMESTAMP_NTZ,
    LASTACTIVITYDATE            DATE,
    RAW_DATA                    VARIANT,
    _EXTRACT_TIMESTAMP          TIMESTAMP_NTZ,
    _SOURCE_OBJECT              VARCHAR(100),
    _LOAD_TIMESTAMP             TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _FILE_NAME                  VARCHAR(1000),
    _FILE_ROW_NUMBER            NUMBER(18)
)
CLUSTER BY (DATE_TRUNC('DAY', SYSTEMMODSTAMP));


-- ── OPPORTUNITY ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS OPPORTUNITY (
    ID                          VARCHAR(18)     NOT NULL,
    NAME                        VARCHAR(255),
    TYPE                        VARCHAR(100),
    DESCRIPTION                 VARCHAR(32000),
    NEXTSTEP                    VARCHAR(255),
    ACCOUNTID                   VARCHAR(18),
    OWNERID                     VARCHAR(18),
    CAMPAIGNID                  VARCHAR(18),
    STAGENAME                   VARCHAR(100),
    FORECASTCATEGORY            VARCHAR(50),
    AMOUNT                      NUMBER(38,2),
    EXPECTEDREVENUE             NUMBER(38,2),
    PROBABILITY                 NUMBER(5,2),
    TOTALOPPORTUNITYQUANTITY    NUMBER(18,4),
    CLOSEDATE                   DATE,
    FISCALQUARTER               NUMBER(1),
    FISCALYEAR                  NUMBER(4),
    ISCLOSED                    BOOLEAN,
    ISWON                       BOOLEAN,
    HASOPPORTUNITYLINEITEM      BOOLEAN,
    LEADSOURCE                  VARCHAR(100),
    COMPETITOR__C               VARCHAR(255),
    REASON_WON_LOST__C          VARCHAR(255),
    ARR__C                      NUMBER(38,2),
    MRR__C                      NUMBER(38,2),
    TCV__C                      NUMBER(38,2),
    CONTRACT_START_DATE__C      DATE,
    CONTRACT_END_DATE__C        DATE,
    CONTRACT_TERM_MONTHS__C     NUMBER(4),
    ISDELETED                   BOOLEAN,
    CREATEDDATE                 TIMESTAMP_NTZ,
    LASTMODIFIEDDATE            TIMESTAMP_NTZ,
    SYSTEMMODSTAMP              TIMESTAMP_NTZ,
    LASTACTIVITYDATE            DATE,
    RAW_DATA                    VARIANT,
    _EXTRACT_TIMESTAMP          TIMESTAMP_NTZ,
    _SOURCE_OBJECT              VARCHAR(100),
    _LOAD_TIMESTAMP             TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _FILE_NAME                  VARCHAR(1000),
    _FILE_ROW_NUMBER            NUMBER(18)
)
CLUSTER BY (DATE_TRUNC('DAY', SYSTEMMODSTAMP));


-- ── OPPORTUNITY_LINE_ITEM ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS OPPORTUNITY_LINE_ITEM (
    ID                          VARCHAR(18)     NOT NULL,
    OPPORTUNITYID               VARCHAR(18),
    PRODUCT2ID                  VARCHAR(18),
    PRICEBOOKENTRYID            VARCHAR(18),
    NAME                        VARCHAR(255),
    PRODUCTCODE                 VARCHAR(255),
    DESCRIPTION                 VARCHAR(4000),
    QUANTITY                    NUMBER(18,4),
    UNITPRICE                   NUMBER(38,2),
    LISTPRICE                   NUMBER(38,2),
    TOTALPRICE                  NUMBER(38,2),
    DISCOUNT                    NUMBER(5,2),
    REVENUE_TYPE__C             VARCHAR(100),
    SERVICEDATE                 DATE,
    SORTORDER                   NUMBER(6),
    ISDELETED                   BOOLEAN,
    CREATEDDATE                 TIMESTAMP_NTZ,
    SYSTEMMODSTAMP              TIMESTAMP_NTZ,
    RAW_DATA                    VARIANT,
    _EXTRACT_TIMESTAMP          TIMESTAMP_NTZ,
    _SOURCE_OBJECT              VARCHAR(100),
    _LOAD_TIMESTAMP             TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _FILE_NAME                  VARCHAR(1000),
    _FILE_ROW_NUMBER            NUMBER(18)
)
CLUSTER BY (DATE_TRUNC('DAY', SYSTEMMODSTAMP));


-- ── CAMPAIGN ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS CAMPAIGN (
    ID                          VARCHAR(18)     NOT NULL,
    NAME                        VARCHAR(255),
    TYPE                        VARCHAR(100),
    STATUS                      VARCHAR(100),
    PARENTID                    VARCHAR(18),
    OWNERID                     VARCHAR(18),
    STARTDATE                   DATE,
    ENDDATE                     DATE,
    BUDGETEDCOST                NUMBER(38,2),
    ACTUALCOST                  NUMBER(38,2),
    EXPECTEDREVENUE             NUMBER(38,2),
    EXPECTEDRESPONSE            NUMBER(5,2),
    NUMBERSENT                  NUMBER(10),
    NUMBEROFLEADS               NUMBER(10),
    NUMBEROFCONVERTEDLEADS      NUMBER(10),
    NUMBEROFCONTACTS            NUMBER(10),
    NUMBEROFRESPONSES           NUMBER(10),
    NUMBEROFOPPORTUNITIES       NUMBER(10),
    NUMBEROFWONOPPORTUNITIES    NUMBER(10),
    AMOUNTALLOPPORTUNITIES      NUMBER(38,2),
    AMOUNTWONOPPORTUNITIES      NUMBER(38,2),
    UTM_SOURCE__C               VARCHAR(255),
    UTM_MEDIUM__C               VARCHAR(255),
    UTM_CONTENT__C              VARCHAR(255),
    CHANNEL__C                  VARCHAR(100),
    ISACTIVE                    BOOLEAN,
    ISDELETED                   BOOLEAN,
    CREATEDDATE                 TIMESTAMP_NTZ,
    LASTMODIFIEDDATE            TIMESTAMP_NTZ,
    SYSTEMMODSTAMP              TIMESTAMP_NTZ,
    RAW_DATA                    VARIANT,
    _EXTRACT_TIMESTAMP          TIMESTAMP_NTZ,
    _SOURCE_OBJECT              VARCHAR(100),
    _LOAD_TIMESTAMP             TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _FILE_NAME                  VARCHAR(1000),
    _FILE_ROW_NUMBER            NUMBER(18)
);


-- ── CAMPAIGN_MEMBER ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS CAMPAIGN_MEMBER (
    ID                          VARCHAR(18)     NOT NULL,
    CAMPAIGNID                  VARCHAR(18),
    LEADID                      VARCHAR(18),
    CONTACTID                   VARCHAR(18),
    STATUS                      VARCHAR(100),
    HASRESPONDED                BOOLEAN,
    FIRSTRESPONDEDDATE          DATE,
    ISDELETED                   BOOLEAN,
    CREATEDDATE                 TIMESTAMP_NTZ,
    SYSTEMMODSTAMP              TIMESTAMP_NTZ,
    RAW_DATA                    VARIANT,
    _EXTRACT_TIMESTAMP          TIMESTAMP_NTZ,
    _SOURCE_OBJECT              VARCHAR(100),
    _LOAD_TIMESTAMP             TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _FILE_NAME                  VARCHAR(1000),
    _FILE_ROW_NUMBER            NUMBER(18)
);


-- ── TASK ────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS TASK (
    ID                          VARCHAR(18)     NOT NULL,
    SUBJECT                     VARCHAR(255),
    TYPE                        VARCHAR(100),
    STATUS                      VARCHAR(100),
    PRIORITY                    VARCHAR(50),
    WHOID                       VARCHAR(18),
    WHATID                      VARCHAR(18),
    OWNERID                     VARCHAR(18),
    ACTIVITYDATE                DATE,
    DESCRIPTION                 VARCHAR(32000),
    CALLTYPE                    VARCHAR(100),
    CALLBACKDURATIONINSECONDS   NUMBER(10),
    CALLDISPOSITION             VARCHAR(255),
    ISHIGHPRIORITY              BOOLEAN,
    ISDELETED                   BOOLEAN,
    CREATEDDATE                 TIMESTAMP_NTZ,
    LASTMODIFIEDDATE            TIMESTAMP_NTZ,
    SYSTEMMODSTAMP              TIMESTAMP_NTZ,
    RAW_DATA                    VARIANT,
    _EXTRACT_TIMESTAMP          TIMESTAMP_NTZ,
    _SOURCE_OBJECT              VARCHAR(100),
    _LOAD_TIMESTAMP             TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _FILE_NAME                  VARCHAR(1000),
    _FILE_ROW_NUMBER            NUMBER(18)
);


-- ── EVENT ────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS EVENT (
    ID                          VARCHAR(18)     NOT NULL,
    SUBJECT                     VARCHAR(255),
    TYPE                        VARCHAR(100),
    WHOID                       VARCHAR(18),
    WHATID                      VARCHAR(18),
    OWNERID                     VARCHAR(18),
    ACTIVITYDATETIME            TIMESTAMP_NTZ,
    ENDDATETIME                 TIMESTAMP_NTZ,
    DURATIONINMINUTES           NUMBER(6),
    LOCATION                    VARCHAR(255),
    DESCRIPTION                 VARCHAR(32000),
    ISALLDAYEVENT               BOOLEAN,
    ISPRIVATE                   BOOLEAN,
    ISDELETED                   BOOLEAN,
    CREATEDDATE                 TIMESTAMP_NTZ,
    LASTMODIFIEDDATE            TIMESTAMP_NTZ,
    SYSTEMMODSTAMP              TIMESTAMP_NTZ,
    RAW_DATA                    VARIANT,
    _EXTRACT_TIMESTAMP          TIMESTAMP_NTZ,
    _SOURCE_OBJECT              VARCHAR(100),
    _LOAD_TIMESTAMP             TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _FILE_NAME                  VARCHAR(1000),
    _FILE_ROW_NUMBER            NUMBER(18)
);


-- ── USER ────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS "USER" (   -- USER is reserved in Snowflake
    ID                          VARCHAR(18)     NOT NULL,
    FIRSTNAME                   VARCHAR(100),
    LASTNAME                    VARCHAR(100),
    NAME                        VARCHAR(200),
    EMAIL                       VARCHAR(320),
    USERNAME                    VARCHAR(320),
    ALIAS                       VARCHAR(8),
    TITLE                       VARCHAR(128),
    DEPARTMENT                  VARCHAR(100),
    DIVISION                    VARCHAR(100),
    PHONE                       VARCHAR(40),
    MOBILEPHONE                 VARCHAR(40),
    MANAGERID                   VARCHAR(18),
    USERROLEID                  VARCHAR(18),
    PROFILEID                   VARCHAR(18),
    TIMEZONESIDKEY              VARCHAR(100),
    LOCALESIDKEY                VARCHAR(50),
    ISACTIVE                    BOOLEAN,
    ISDELETED                   BOOLEAN,
    CREATEDDATE                 TIMESTAMP_NTZ,
    LASTMODIFIEDDATE            TIMESTAMP_NTZ,
    SYSTEMMODSTAMP              TIMESTAMP_NTZ,
    LASTLOGINDATE               TIMESTAMP_NTZ,
    RAW_DATA                    VARIANT,
    _EXTRACT_TIMESTAMP          TIMESTAMP_NTZ,
    _SOURCE_OBJECT              VARCHAR(100),
    _LOAD_TIMESTAMP             TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _FILE_NAME                  VARCHAR(1000),
    _FILE_ROW_NUMBER            NUMBER(18)
);


-- ── Future-table grants ──────────────────────────────────────────────────────
GRANT INSERT ON ALL TABLES    IN SCHEMA RAW.SALESFORCE TO ROLE LOADER;
GRANT INSERT ON FUTURE TABLES IN SCHEMA RAW.SALESFORCE TO ROLE LOADER;
