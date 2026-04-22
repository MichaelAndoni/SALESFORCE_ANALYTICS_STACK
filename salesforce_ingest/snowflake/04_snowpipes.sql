-- snowflake/04_snowpipes.sql
--
-- Creates one Snowpipe per Salesforce object.
--
-- Architecture:
--   S3 object created → SQS message → Snowpipe polls SQS → COPY INTO table
--
-- The AUTO_INGEST = TRUE + ERROR_INTEGRATION wiring means Snowpipe will:
--   1. Poll the SQS queue for new S3 event notifications (no Lambda trigger needed)
--   2. Issue a COPY INTO for each new file
--   3. Skip files it has already loaded (idempotent by default)
--   4. Route load errors to the error integration (DLQ concept)
--
-- Snowpipe COPY INTO options:
--   NULLIF pattern    — NULLIF(field::VARCHAR, '')::TYPE avoids cast failures on
--                       empty-string JSON values for numeric/date columns
--   PURGE = FALSE      — keep files in S3 (they're our source of truth)
--   ON_ERROR = CONTINUE — skip bad rows, don't abort the whole file
--   FORCE = FALSE      — don't re-load files already in load history

USE ROLE SYSADMIN;
USE DATABASE RAW;
USE SCHEMA   SALESFORCE;


-- ── ACCOUNT ─────────────────────────────────────────────────────────────────
CREATE PIPE IF NOT EXISTS PIPE_ACCOUNT
    AUTO_INGEST        = TRUE
    -- ERROR_INTEGRATION  = SF_SQS_INTEGRATION  -- requires Snowflake Enterprise edition
    COMMENT            = 'Snowpipe for Salesforce Account NDJSON files'
AS
COPY INTO ACCOUNT (
    ID, NAME, TYPE, INDUSTRY, RATING, ACCOUNTSOURCE,
    DESCRIPTION, WEBSITE, PHONE, FAX, ANNUALREVENUE, NUMBEROFEMPLOYEES,
    BILLINGSTREET, BILLINGCITY, BILLINGSTATE, BILLINGPOSTALCODE, BILLINGCOUNTRY,
    BILLINGLATITUDE, BILLINGLONGITUDE,
    PARENTID, MASTERRECORDID, OWNERID,
    ISDELETED, CREATEDDATE, LASTMODIFIEDDATE, SYSTEMMODSTAMP,
    RAW_DATA, _EXTRACT_TIMESTAMP, _SOURCE_OBJECT, _FILE_NAME, _FILE_ROW_NUMBER
)
FROM (
    SELECT
        $1:Id::VARCHAR,
        $1:Name::VARCHAR,
        $1:Type::VARCHAR,
        $1:Industry::VARCHAR,
        $1:Rating::VARCHAR,
        $1:AccountSource::VARCHAR,
        $1:Description::VARCHAR,
        $1:Website::VARCHAR,
        $1:Phone::VARCHAR,
        $1:Fax::VARCHAR,
        NULLIF($1:AnnualRevenue::VARCHAR,    '')::NUMBER(38,2),
        NULLIF($1:NumberOfEmployees::VARCHAR,'')::NUMBER(10),
        $1:BillingStreet::VARCHAR,
        $1:BillingCity::VARCHAR,
        $1:BillingState::VARCHAR,
        $1:BillingPostalCode::VARCHAR,
        $1:BillingCountry::VARCHAR,
        NULLIF($1:BillingLatitude::VARCHAR,  '')::FLOAT,
        NULLIF($1:BillingLongitude::VARCHAR, '')::FLOAT,
        $1:ParentId::VARCHAR,
        $1:MasterRecordId::VARCHAR,
        $1:OwnerId::VARCHAR,
        $1:IsDeleted::BOOLEAN,
        $1:CreatedDate::TIMESTAMP_NTZ,
        $1:LastModifiedDate::TIMESTAMP_NTZ,
        $1:SystemModstamp::TIMESTAMP_NTZ,
        $1,
        $1:_extract_timestamp::TIMESTAMP_NTZ,
        $1:_source_object::VARCHAR,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER
    FROM @STG_SALESFORCE_LANDING/account/
)
FILE_FORMAT = FF_SALESFORCE_NDJSON
ON_ERROR    = CONTINUE;


-- ── CONTACT ─────────────────────────────────────────────────────────────────
CREATE PIPE IF NOT EXISTS PIPE_CONTACT
    AUTO_INGEST = TRUE  -- ERROR_INTEGRATION = SF_SQS_INTEGRATION requires Snowflake Enterprise
    COMMENT = 'Snowpipe for Salesforce Contact NDJSON files'
AS
COPY INTO CONTACT (
    ID, FIRSTNAME, LASTNAME, NAME, SALUTATION, TITLE, DEPARTMENT,
    ACCOUNTID, REPORTSTOID, OWNERID, EMAIL, PHONE, MOBILEPHONE, FAX,
    LEADSOURCE,
    MAILINGSTREET, MAILINGCITY, MAILINGSTATE, MAILINGPOSTALCODE, MAILINGCOUNTRY,
    ISDELETED, CREATEDDATE, LASTMODIFIEDDATE, SYSTEMMODSTAMP, LASTACTIVITYDATE,
    RAW_DATA, _EXTRACT_TIMESTAMP, _SOURCE_OBJECT, _FILE_NAME, _FILE_ROW_NUMBER
)
FROM (
    SELECT
        $1:Id::VARCHAR, $1:FirstName::VARCHAR, $1:LastName::VARCHAR,
        $1:Name::VARCHAR, $1:Salutation::VARCHAR, $1:Title::VARCHAR,
        $1:Department::VARCHAR, $1:AccountId::VARCHAR, $1:ReportsToId::VARCHAR,
        $1:OwnerId::VARCHAR, $1:Email::VARCHAR, $1:Phone::VARCHAR,
        $1:MobilePhone::VARCHAR, $1:Fax::VARCHAR, $1:LeadSource::VARCHAR,
        $1:MailingStreet::VARCHAR, $1:MailingCity::VARCHAR, $1:MailingState::VARCHAR,
        $1:MailingPostalCode::VARCHAR, $1:MailingCountry::VARCHAR,
        $1:IsDeleted::BOOLEAN, $1:CreatedDate::TIMESTAMP_NTZ,
        $1:LastModifiedDate::TIMESTAMP_NTZ, $1:SystemModstamp::TIMESTAMP_NTZ,
        NULLIF($1:LastActivityDate::VARCHAR, '')::DATE,
        $1, $1:_extract_timestamp::TIMESTAMP_NTZ, $1:_source_object::VARCHAR,
        METADATA$FILENAME, METADATA$FILE_ROW_NUMBER
    FROM @STG_SALESFORCE_LANDING/contact/
)
FILE_FORMAT = FF_SALESFORCE_NDJSON ON_ERROR = CONTINUE;


-- ── LEAD ────────────────────────────────────────────────────────────────────
CREATE PIPE IF NOT EXISTS PIPE_LEAD
    AUTO_INGEST = TRUE  -- ERROR_INTEGRATION = SF_SQS_INTEGRATION requires Snowflake Enterprise
    COMMENT = 'Snowpipe for Salesforce Lead NDJSON files'
AS
COPY INTO LEAD (
    ID, FIRSTNAME, LASTNAME, NAME, SALUTATION, TITLE, EMAIL, PHONE, MOBILEPHONE,
    COMPANY, INDUSTRY, ANNUALREVENUE, NUMBEROFEMPLOYEES, WEBSITE,
    STATUS, LEADSOURCE, RATING,
    STREET, CITY, STATE, POSTALCODE, COUNTRY,
    ISCONVERTED, CONVERTEDDATE, CONVERTEDACCOUNTID, CONVERTEDCONTACTID,
    CONVERTEDOPPORTUNITYID, OWNERID,
    ISDELETED, CREATEDDATE, LASTMODIFIEDDATE, SYSTEMMODSTAMP, LASTACTIVITYDATE,
    RAW_DATA, _EXTRACT_TIMESTAMP, _SOURCE_OBJECT, _FILE_NAME, _FILE_ROW_NUMBER
)
FROM (
    SELECT
        $1:Id::VARCHAR, $1:FirstName::VARCHAR, $1:LastName::VARCHAR,
        $1:Name::VARCHAR, $1:Salutation::VARCHAR, $1:Title::VARCHAR,
        $1:Email::VARCHAR, $1:Phone::VARCHAR, $1:MobilePhone::VARCHAR,
        $1:Company::VARCHAR, $1:Industry::VARCHAR,
        NULLIF($1:AnnualRevenue::VARCHAR,    '')::NUMBER(38,2),
        NULLIF($1:NumberOfEmployees::VARCHAR,'')::NUMBER(10),
        $1:Website::VARCHAR, $1:Status::VARCHAR, $1:LeadSource::VARCHAR,
        $1:Rating::VARCHAR, $1:Street::VARCHAR, $1:City::VARCHAR,
        $1:State::VARCHAR, $1:PostalCode::VARCHAR, $1:Country::VARCHAR,
        $1:IsConverted::BOOLEAN,
        NULLIF($1:ConvertedDate::VARCHAR,     '')::DATE,
        $1:ConvertedAccountId::VARCHAR, $1:ConvertedContactId::VARCHAR,
        $1:ConvertedOpportunityId::VARCHAR, $1:OwnerId::VARCHAR,
        $1:IsDeleted::BOOLEAN, $1:CreatedDate::TIMESTAMP_NTZ,
        $1:LastModifiedDate::TIMESTAMP_NTZ, $1:SystemModstamp::TIMESTAMP_NTZ,
        NULLIF($1:LastActivityDate::VARCHAR,  '')::DATE,
        $1, $1:_extract_timestamp::TIMESTAMP_NTZ, $1:_source_object::VARCHAR,
        METADATA$FILENAME, METADATA$FILE_ROW_NUMBER
    FROM @STG_SALESFORCE_LANDING/lead/
)
FILE_FORMAT = FF_SALESFORCE_NDJSON ON_ERROR = CONTINUE;


-- ── OPPORTUNITY ─────────────────────────────────────────────────────────────
CREATE PIPE IF NOT EXISTS PIPE_OPPORTUNITY
    AUTO_INGEST = TRUE  -- ERROR_INTEGRATION = SF_SQS_INTEGRATION requires Snowflake Enterprise
    COMMENT = 'Snowpipe for Salesforce Opportunity NDJSON files'
AS
COPY INTO OPPORTUNITY (
    ID, NAME, TYPE, DESCRIPTION, NEXTSTEP, ACCOUNTID, OWNERID, CAMPAIGNID,
    STAGENAME, FORECASTCATEGORY, AMOUNT, EXPECTEDREVENUE, PROBABILITY,
    TOTALOPPORTUNITYQUANTITY, CLOSEDATE, FISCALQUARTER, FISCALYEAR,
    ISCLOSED, ISWON, HASOPPORTUNITYLINEITEM, LEADSOURCE,
    ISDELETED, CREATEDDATE, LASTMODIFIEDDATE, SYSTEMMODSTAMP, LASTACTIVITYDATE,
    RAW_DATA, _EXTRACT_TIMESTAMP, _SOURCE_OBJECT, _FILE_NAME, _FILE_ROW_NUMBER
)
FROM (
    SELECT
        $1:Id::VARCHAR, $1:Name::VARCHAR, $1:Type::VARCHAR,
        $1:Description::VARCHAR, $1:NextStep::VARCHAR,
        $1:AccountId::VARCHAR, $1:OwnerId::VARCHAR, $1:CampaignId::VARCHAR,
        $1:StageName::VARCHAR, $1:ForecastCategory::VARCHAR,
        NULLIF($1:Amount::VARCHAR,                  '')::NUMBER(38,2),
        NULLIF($1:ExpectedRevenue::VARCHAR,          '')::NUMBER(38,2),
        NULLIF($1:Probability::VARCHAR,              '')::NUMBER(5,2),
        NULLIF($1:TotalOpportunityQuantity::VARCHAR, '')::NUMBER(18,4),
        NULLIF($1:CloseDate::VARCHAR,                '')::DATE,
        NULLIF($1:FiscalQuarter::VARCHAR,            '')::NUMBER(1),
        NULLIF($1:FiscalYear::VARCHAR,               '')::NUMBER(4),
        $1:IsClosed::BOOLEAN, $1:IsWon::BOOLEAN,
        $1:HasOpportunityLineItem::BOOLEAN, $1:LeadSource::VARCHAR,
        $1:IsDeleted::BOOLEAN, $1:CreatedDate::TIMESTAMP_NTZ,
        $1:LastModifiedDate::TIMESTAMP_NTZ, $1:SystemModstamp::TIMESTAMP_NTZ,
        NULLIF($1:LastActivityDate::VARCHAR,         '')::DATE,
        $1, $1:_extract_timestamp::TIMESTAMP_NTZ, $1:_source_object::VARCHAR,
        METADATA$FILENAME, METADATA$FILE_ROW_NUMBER
    FROM @STG_SALESFORCE_LANDING/opportunity/
)
FILE_FORMAT = FF_SALESFORCE_NDJSON ON_ERROR = CONTINUE;


-- ── OPPORTUNITY_LINE_ITEM ────────────────────────────────────────────────────
CREATE PIPE IF NOT EXISTS PIPE_OPPORTUNITY_LINE_ITEM
    AUTO_INGEST = TRUE  -- ERROR_INTEGRATION = SF_SQS_INTEGRATION requires Snowflake Enterprise
AS
COPY INTO OPPORTUNITY_LINE_ITEM (
    ID, OPPORTUNITYID, PRODUCT2ID, PRICEBOOKENTRYID, NAME, PRODUCTCODE,
    DESCRIPTION, QUANTITY, UNITPRICE, LISTPRICE, TOTALPRICE,
    REVENUE_TYPE__C, SERVICEDATE, SORTORDER,
    ISDELETED, CREATEDDATE, SYSTEMMODSTAMP,
    RAW_DATA, _EXTRACT_TIMESTAMP, _SOURCE_OBJECT, _FILE_NAME, _FILE_ROW_NUMBER
)
FROM (
    SELECT
        $1:Id::VARCHAR, $1:OpportunityId::VARCHAR, $1:Product2Id::VARCHAR,
        $1:PricebookEntryId::VARCHAR, $1:Name::VARCHAR, $1:ProductCode::VARCHAR,
        $1:Description::VARCHAR,
        NULLIF($1:Quantity::VARCHAR,   '')::NUMBER(18,4),
        NULLIF($1:UnitPrice::VARCHAR,  '')::NUMBER(38,2),
        NULLIF($1:ListPrice::VARCHAR,  '')::NUMBER(38,2),
        NULLIF($1:TotalPrice::VARCHAR, '')::NUMBER(38,2),
        $1:Revenue_Type__c::VARCHAR,
        NULLIF($1:ServiceDate::VARCHAR,'')::DATE,
        NULLIF($1:SortOrder::VARCHAR,  '')::NUMBER(6),
        $1:IsDeleted::BOOLEAN,
        $1:CreatedDate::TIMESTAMP_NTZ, $1:SystemModstamp::TIMESTAMP_NTZ,
        $1, $1:_extract_timestamp::TIMESTAMP_NTZ, $1:_source_object::VARCHAR,
        METADATA$FILENAME, METADATA$FILE_ROW_NUMBER
    FROM @STG_SALESFORCE_LANDING/opportunitylineitem/
)
FILE_FORMAT = FF_SALESFORCE_NDJSON ON_ERROR = CONTINUE;


-- ── CAMPAIGN ────────────────────────────────────────────────────────────────
CREATE PIPE IF NOT EXISTS PIPE_CAMPAIGN
    AUTO_INGEST = TRUE  -- ERROR_INTEGRATION = SF_SQS_INTEGRATION requires Snowflake Enterprise
AS
COPY INTO CAMPAIGN (
    ID, NAME, TYPE, STATUS, PARENTID, OWNERID, STARTDATE, ENDDATE,
    BUDGETEDCOST, ACTUALCOST, EXPECTEDREVENUE, EXPECTEDRESPONSE, NUMBERSENT,
    NUMBEROFLEADS, NUMBEROFCONVERTEDLEADS, NUMBEROFCONTACTS,
    NUMBEROFRESPONSES, NUMBEROFOPPORTUNITIES, NUMBEROFWONOPPORTUNITIES,
    AMOUNTALLOPPORTUNITIES, AMOUNTWONOPPORTUNITIES,
    ISACTIVE, ISDELETED, CREATEDDATE, LASTMODIFIEDDATE, SYSTEMMODSTAMP,
    RAW_DATA, _EXTRACT_TIMESTAMP, _SOURCE_OBJECT, _FILE_NAME, _FILE_ROW_NUMBER
)
FROM (
    SELECT
        $1:Id::VARCHAR, $1:Name::VARCHAR, $1:Type::VARCHAR, $1:Status::VARCHAR,
        $1:ParentId::VARCHAR, $1:OwnerId::VARCHAR,
        NULLIF($1:StartDate::VARCHAR,                '')::DATE,
        NULLIF($1:EndDate::VARCHAR,                  '')::DATE,
        NULLIF($1:BudgetedCost::VARCHAR,             '')::NUMBER(38,2),
        NULLIF($1:ActualCost::VARCHAR,               '')::NUMBER(38,2),
        NULLIF($1:ExpectedRevenue::VARCHAR,          '')::NUMBER(38,2),
        NULLIF($1:ExpectedResponse::VARCHAR,         '')::NUMBER(5,2),
        NULLIF($1:NumberSent::VARCHAR,               '')::NUMBER(10),
        NULLIF($1:NumberOfLeads::VARCHAR,            '')::NUMBER(10),
        NULLIF($1:NumberOfConvertedLeads::VARCHAR,   '')::NUMBER(10),
        NULLIF($1:NumberOfContacts::VARCHAR,         '')::NUMBER(10),
        NULLIF($1:NumberOfResponses::VARCHAR,        '')::NUMBER(10),
        NULLIF($1:NumberOfOpportunities::VARCHAR,    '')::NUMBER(10),
        NULLIF($1:NumberOfWonOpportunities::VARCHAR, '')::NUMBER(10),
        NULLIF($1:AmountAllOpportunities::VARCHAR,   '')::NUMBER(38,2),
        NULLIF($1:AmountWonOpportunities::VARCHAR,   '')::NUMBER(38,2),
        $1:IsActive::BOOLEAN, $1:IsDeleted::BOOLEAN,
        $1:CreatedDate::TIMESTAMP_NTZ, $1:LastModifiedDate::TIMESTAMP_NTZ,
        $1:SystemModstamp::TIMESTAMP_NTZ,
        $1, $1:_extract_timestamp::TIMESTAMP_NTZ, $1:_source_object::VARCHAR,
        METADATA$FILENAME, METADATA$FILE_ROW_NUMBER
    FROM @STG_SALESFORCE_LANDING/campaign/
)
FILE_FORMAT = FF_SALESFORCE_NDJSON ON_ERROR = CONTINUE;


-- ── CAMPAIGN_MEMBER ──────────────────────────────────────────────────────────
CREATE PIPE IF NOT EXISTS PIPE_CAMPAIGN_MEMBER
    AUTO_INGEST = TRUE  -- ERROR_INTEGRATION = SF_SQS_INTEGRATION requires Snowflake Enterprise
AS
COPY INTO CAMPAIGN_MEMBER (
    ID, CAMPAIGNID, LEADID, CONTACTID, STATUS, HASRESPONDED,
    FIRSTRESPONDEDDATE, ISDELETED, CREATEDDATE, SYSTEMMODSTAMP,
    RAW_DATA, _EXTRACT_TIMESTAMP, _SOURCE_OBJECT, _FILE_NAME, _FILE_ROW_NUMBER
)
FROM (
    SELECT
        $1:Id::VARCHAR, $1:CampaignId::VARCHAR, $1:LeadId::VARCHAR,
        $1:ContactId::VARCHAR, $1:Status::VARCHAR, $1:HasResponded::BOOLEAN,
        NULLIF($1:FirstRespondedDate::VARCHAR, '')::DATE,
        $1:IsDeleted::BOOLEAN,
        $1:CreatedDate::TIMESTAMP_NTZ, $1:SystemModstamp::TIMESTAMP_NTZ,
        $1, $1:_extract_timestamp::TIMESTAMP_NTZ, $1:_source_object::VARCHAR,
        METADATA$FILENAME, METADATA$FILE_ROW_NUMBER
    FROM @STG_SALESFORCE_LANDING/campaignmember/
)
FILE_FORMAT = FF_SALESFORCE_NDJSON ON_ERROR = CONTINUE;


-- ── TASK ────────────────────────────────────────────────────────────────────
CREATE PIPE IF NOT EXISTS PIPE_TASK
    AUTO_INGEST = TRUE  -- ERROR_INTEGRATION = SF_SQS_INTEGRATION requires Snowflake Enterprise
AS
COPY INTO TASK (
    ID, SUBJECT, TYPE, STATUS, PRIORITY, WHOID, WHATID, OWNERID,
    ACTIVITYDATE, DESCRIPTION, CALLTYPE, CALLBACKDURATIONINSECONDS,
    CALLDISPOSITION, ISHIGHPRIORITY,
    ISDELETED, CREATEDDATE, LASTMODIFIEDDATE, SYSTEMMODSTAMP,
    RAW_DATA, _EXTRACT_TIMESTAMP, _SOURCE_OBJECT, _FILE_NAME, _FILE_ROW_NUMBER
)
FROM (
    SELECT
        $1:Id::VARCHAR, $1:Subject::VARCHAR, $1:Type::VARCHAR, $1:Status::VARCHAR,
        $1:Priority::VARCHAR, $1:WhoId::VARCHAR, $1:WhatId::VARCHAR,
        $1:OwnerId::VARCHAR,
        NULLIF($1:ActivityDate::VARCHAR,            '')::DATE,
        $1:Description::VARCHAR,
        $1:CallType::VARCHAR,
        NULLIF($1:CallDurationInSeconds::VARCHAR,   '')::NUMBER(10),
        $1:CallDisposition::VARCHAR, $1:IsHighPriority::BOOLEAN,
        $1:IsDeleted::BOOLEAN, $1:CreatedDate::TIMESTAMP_NTZ,
        $1:LastModifiedDate::TIMESTAMP_NTZ, $1:SystemModstamp::TIMESTAMP_NTZ,
        $1, $1:_extract_timestamp::TIMESTAMP_NTZ, $1:_source_object::VARCHAR,
        METADATA$FILENAME, METADATA$FILE_ROW_NUMBER
    FROM @STG_SALESFORCE_LANDING/task/
)
FILE_FORMAT = FF_SALESFORCE_NDJSON ON_ERROR = CONTINUE;


-- ── EVENT ────────────────────────────────────────────────────────────────────
CREATE PIPE IF NOT EXISTS PIPE_EVENT
    AUTO_INGEST = TRUE  -- ERROR_INTEGRATION = SF_SQS_INTEGRATION requires Snowflake Enterprise
AS
COPY INTO EVENT (
    ID, SUBJECT, TYPE, WHOID, WHATID, OWNERID,
    ACTIVITYDATETIME, ENDDATETIME, DURATIONINMINUTES,
    LOCATION, DESCRIPTION, ISALLDAYEVENT, ISPRIVATE,
    ISDELETED, CREATEDDATE, LASTMODIFIEDDATE, SYSTEMMODSTAMP,
    RAW_DATA, _EXTRACT_TIMESTAMP, _SOURCE_OBJECT, _FILE_NAME, _FILE_ROW_NUMBER
)
FROM (
    SELECT
        $1:Id::VARCHAR, $1:Subject::VARCHAR, $1:Type::VARCHAR,
        $1:WhoId::VARCHAR, $1:WhatId::VARCHAR, $1:OwnerId::VARCHAR,
        $1:ActivityDateTime::TIMESTAMP_NTZ, $1:EndDateTime::TIMESTAMP_NTZ,
        NULLIF($1:DurationInMinutes::VARCHAR, '')::NUMBER(6),
        $1:Location::VARCHAR,
        $1:Description::VARCHAR, $1:IsAllDayEvent::BOOLEAN, $1:IsPrivate::BOOLEAN,
        $1:IsDeleted::BOOLEAN, $1:CreatedDate::TIMESTAMP_NTZ,
        $1:LastModifiedDate::TIMESTAMP_NTZ, $1:SystemModstamp::TIMESTAMP_NTZ,
        $1, $1:_extract_timestamp::TIMESTAMP_NTZ, $1:_source_object::VARCHAR,
        METADATA$FILENAME, METADATA$FILE_ROW_NUMBER
    FROM @STG_SALESFORCE_LANDING/event/
)
FILE_FORMAT = FF_SALESFORCE_NDJSON ON_ERROR = CONTINUE;


-- ── USER ────────────────────────────────────────────────────────────────────
CREATE PIPE IF NOT EXISTS PIPE_USER
    AUTO_INGEST = TRUE  -- ERROR_INTEGRATION = SF_SQS_INTEGRATION requires Snowflake Enterprise
AS
COPY INTO "USER" (
    ID, FIRSTNAME, LASTNAME, NAME, EMAIL, USERNAME, ALIAS,
    TITLE, DEPARTMENT, DIVISION, PHONE, MOBILEPHONE,
    MANAGERID, USERROLEID, PROFILEID,
    TIMEZONESIDKEY, LOCALESIDKEY, ISACTIVE, ISDELETED,
    CREATEDDATE, LASTMODIFIEDDATE, SYSTEMMODSTAMP, LASTLOGINDATE,
    RAW_DATA, _EXTRACT_TIMESTAMP, _SOURCE_OBJECT, _FILE_NAME, _FILE_ROW_NUMBER
)
FROM (
    SELECT
        $1:Id::VARCHAR, $1:FirstName::VARCHAR, $1:LastName::VARCHAR,
        $1:Name::VARCHAR, $1:Email::VARCHAR, $1:Username::VARCHAR,
        $1:Alias::VARCHAR, $1:Title::VARCHAR, $1:Department::VARCHAR,
        $1:Division::VARCHAR, $1:Phone::VARCHAR, $1:MobilePhone::VARCHAR,
        $1:ManagerId::VARCHAR, $1:UserRoleId::VARCHAR, $1:ProfileId::VARCHAR,
        $1:TimeZoneSidKey::VARCHAR, $1:LocaleSidKey::VARCHAR,
        $1:IsActive::BOOLEAN, $1:IsDeleted::BOOLEAN,
        $1:CreatedDate::TIMESTAMP_NTZ, $1:LastModifiedDate::TIMESTAMP_NTZ,
        $1:SystemModstamp::TIMESTAMP_NTZ,
        NULLIF($1:LastLoginDate::VARCHAR, '')::TIMESTAMP_NTZ,
        $1, $1:_extract_timestamp::TIMESTAMP_NTZ, $1:_source_object::VARCHAR,
        METADATA$FILENAME, METADATA$FILE_ROW_NUMBER
    FROM @STG_SALESFORCE_LANDING/user/
)
FILE_FORMAT = FF_SALESFORCE_NDJSON ON_ERROR = CONTINUE;


-- ────────────────────────────────────────────────────────────────────────────
-- Post-creation: inspect pipe status
-- ────────────────────────────────────────────────────────────────────────────
-- Run after creating pipes and confirming SQS integration:
--
-- SHOW PIPES IN SCHEMA RAW.SALESFORCE;
-- SELECT SYSTEM$PIPE_STATUS('SALESFORCE.PIPE_OPPORTUNITY');
--
-- Expected output when healthy:
-- {"executionState":"RUNNING","pendingFileCount":0,"notificationChannelStatus":"CONNECTED"}
