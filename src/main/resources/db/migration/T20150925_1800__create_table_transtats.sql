-- Create transtats table that consolidates
-- delay from united states internal flight

-- This definition has been created from csv file with
-- http://www.convertcsv.com/csv-to-sql.htm

CREATE TABLE transtats(
   FL_DATE               DATE  NOT NULL
  ,UNIQUE_CARRIER        VARCHAR(255) NOT NULL
  ,AIRLINE_ID            INTEGER  NOT NULL
  ,CARRIER               VARCHAR(255) NOT NULL
  ,TAIL_NUM              VARCHAR(255) NOT NULL
  ,FL_NUM                INTEGER  NOT NULL
  ,ORIGIN_AIRPORT_ID     INTEGER  NOT NULL
  ,ORIGIN_AIRPORT_SEQ_ID INTEGER  NOT NULL
  ,ORIGIN_CITY_MARKET_ID INTEGER  NOT NULL
  ,ORIGIN                VARCHAR(255) NOT NULL
  ,ORIGIN_CITY_NAME      VARCHAR(255) NOT NULL
  ,ORIGIN_STATE_ABR      VARCHAR(255) NOT NULL
  ,ORIGIN_STATE_NM       VARCHAR(255) NOT NULL
  ,ORIGIN_WAC            INTEGER  NOT NULL
  ,DEST_AIRPORT_ID       INTEGER  NOT NULL
  ,DEST_AIRPORT_SEQ_ID   INTEGER  NOT NULL
  ,DEST_CITY_MARKET_ID   INTEGER  NOT NULL
  ,DEST                  VARCHAR(255) NOT NULL
  ,DEST_CITY_NAME        VARCHAR(255) NOT NULL
  ,DEST_STATE_ABR        VARCHAR(255) NOT NULL
  ,DEST_STATE_NM         VARCHAR(255) NOT NULL
  ,DEST_WAC              INTEGER  NOT NULL
  ,DEP_TIME              INTEGER  NOT NULL
  ,DEP_DELAY             INTEGER  NOT NULL
  ,ARR_TIME              INTEGER  NOT NULL
  ,ARR_DELAY             INTEGER  NOT NULL
  ,DISTANCE              INTEGER  NOT NULL
  ,CARRIER_DELAY         INTEGER 
  ,WEATHER_DELAY         INTEGER 
  ,NAS_DELAY             INTEGER 
  ,SECURITY_DELAY        INTEGER 
  ,LATE_AIRCRAFT_DELAY   INTEGER
);