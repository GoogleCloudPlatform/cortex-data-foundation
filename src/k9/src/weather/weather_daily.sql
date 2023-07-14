#--  Copyright 2022 Google Inc.
#
#--  Licensed under the Apache License, Version 2.0 (the "License");
#--  you may not use this file except in compliance with the License.
#--  You may obtain a copy of the License at
#
#--      http://www.apache.org/licenses/LICENSE-2.0
#
#--  Unless required by applicable law or agreed to in writing, software
#--  distributed under the License is distributed on an "AS IS" BASIS,
#--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#--  See the License for the specific language governing permissions and
#--  limitations under the License.

/* Creates a table with daily weather details with actual (for past) values or
 * with forecat (for future) values.
 *
 * During the first run, the table will be created afresh. For all subsequent
 * runs, table will be refreshed only for necessary dates.
 *
 * @param project_id_src: Source Project Id - substitued using Jinja templating.
 * @param k9_datasets_processing: K9 processed dataset - substitued using Jinja templating.
 * @param trending_start_date: Start Date for all the data - substitued using Jinja templating.
 */

DECLARE noaa_max_creation_datetime DATETIME;
DECLARE observed_start_date, observed_end_date, forecast_start_date, forecast_end_date DATE;
DECLARE is_first_run BOOL;
DECLARE current_ts TIMESTAMP;

-- Create required tables. Applicable to the first run.
CREATE TABLE IF NOT EXISTS `{{ project_id_src }}.{{ k9_datasets_processing }}.weather_daily`
(
  country STRING,
  postcode STRING,
  date DATE,
  min_temp FLOAT64,
  max_temp FLOAT64,
  value_type STRING,
  insert_timestamp TIMESTAMP,
  update_timestamp TIMESTAMP
);

SET is_first_run = (
  SELECT COUNT(*) = 0
  FROM `{{ project_id_src }}.{{ k9_datasets_processing }}.weather_daily`);

-- For first run, we want to start from the "start date" defined by `trending_start_date`.
-- For subsequent runs, we start where we left off.
IF is_first_run THEN
  SET observed_start_date = DATE_TRUNC(DATE('2017-01-01'), WEEK(MONDAY));
ELSE
  SET observed_start_date = (
    SELECT DATE_ADD(MAX(date), INTERVAL 1 DAY)
    FROM `{{ project_id_src }}.{{ k9_datasets_processing }}.weather_daily`
    WHERE value_type = 'OBSERVED');
END IF;

-- Determine the last date for which we have a full day of observations available in NOAA table.
-- A full day will have all 4 forecast sets available (hours 00, 06, 12, 18).
SET noaa_max_creation_datetime = (
  SELECT MAX(creation_time)
## --CORTEX-CUSTOMER: Replace all occurrences of the dataset below if using a copy or a different name
  FROM `bigquery-public-data.noaa_global_forecast_system.NOAA_GFS0P25`
  WHERE creation_time > DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 7 DAY));

IF (EXTRACT(HOUR FROM noaa_max_creation_datetime) = 18) THEN
  SET observed_end_date = DATE(noaa_max_creation_datetime);
ELSE
  SET observed_end_date = DATE_SUB(DATE(noaa_max_creation_datetime), INTERVAL 1 DAY);
END IF;

-- Proceed only if we have new data in NOAA table to process.
IF observed_start_date > observed_end_date THEN
  RETURN;
END IF;

SET forecast_start_date = (DATE_ADD(observed_end_date, INTERVAL 1 DAY));

-- We want to have data up to the last day (Sunday) of the week 13 weeks ago,
-- including current week.
SET forecast_end_date = DATE_SUB(
  DATE_ADD(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)), INTERVAL 13 WEEK),
  INTERVAL 1 DAY);

-- Observed (past) values.
CREATE TEMP TABLE ObservedValues
AS (
  SELECT
    P.country,
    P.postcode,
    DATE(G.creation_time) AS date,
    MIN(F.temperature_2m_above_ground) AS min_temp,
    MAX(F.temperature_2m_above_ground) AS max_temp,
    'OBSERVED' AS value_type
  FROM
    `bigquery-public-data.noaa_global_forecast_system.NOAA_GFS0P25` AS G
    CROSS JOIN UNNEST(G.forecast) AS F
    INNER JOIN `{{ project_id_src }}.{{ k9_datasets_processing }}.postcode` AS P
      ON ST_WITHIN(ST_GEOGFROMTEXT(P.centroid), G.geography_polygon)
  WHERE
    DATE(G.creation_time) BETWEEN observed_start_date AND observed_end_date
    -- When Forecast datetime is same as creation datetime, that value is obvserved value
    -- Typically, there are 4 such records each day.
    AND G.CREATION_TIME = F.TIME
  GROUP BY country, postcode, date
);

-- Check for the uniqueness of the data.
ASSERT (
  SELECT COUNT(*) = 0
  FROM (
    SELECT country, postcode, date
    FROM ObservedValues
    GROUP BY country, postcode, date HAVING COUNT(*) > 1)
) AS 'ObservedValues temp table uniqueness requirements not met.';

-- Forecast (future) values.
CREATE TEMP TABLE ForecastValues
AS (
  WITH
    -- Data containing all forecast values
    ForecastValuesDailyAll AS (
      SELECT
        P.country,
        P.postcode,
        F.time AS forecast_time,
        F.temperature_2m_above_ground,
        -- Typically, there are 4 observed values in a given date.
        -- Let's rank them now to select only the latest one later on.
        ROW_NUMBER()
          OVER (
            PARTITION BY
              P.country, P.postcode, F.time
            ORDER BY G.creation_time DESC)
          AS rn
      FROM
        `bigquery-public-data.noaa_global_forecast_system.NOAA_GFS0P25` AS G CROSS JOIN UNNEST(G.forecast) AS F
        INNER JOIN `{{ project_id_src }}.{{ k9_datasets_processing }}.postcode` AS P
          ON ST_WITHIN(ST_GEOGFROMTEXT(P.centroid), G.geography_polygon)
      WHERE
        -- Always use latest data set from NOAA for forecast data.
        DATE(G.creation_time) = DATE(noaa_max_creation_datetime)
        AND F.time >= DATETIME(forecast_start_date)
    )
  -- Get forecast for each date+hour, and then from there, we get various aggregates at Day level.
  SELECT
    country,
    postcode,
    DATE(forecast_time) AS date,
    MIN(temperature_2m_above_ground) AS min_temp,
    MAX(temperature_2m_above_ground) AS max_temp,
    'FORECAST' AS value_type
  FROM ForecastValuesDailyAll
  WHERE rn = 1
  -- Only select dates for which we have forecast for full day is available.
  -- For nearby dates, we have forecast for each hour (0, 1, 2,..., 22, 23).
  -- For later dates, we have forecast for every three hours (0, 3, 6, ...., 18, 21).
  GROUP BY country, postcode, date
  HAVING MAX(EXTRACT(HOUR FROM forecast_time)) IN (21, 23)
);

-- Check for the uniqueness of the data.
ASSERT (
  SELECT COUNT(*) = 0
  FROM (
    SELECT country, postcode, date
    FROM ForecastValues
    GROUP BY country, postcode, date
    HAVING COUNT(*) > 1
  )
) AS 'ForecastValues temp table uniqueness requirements not met.';

-- Estimated (future) values, where forecast is not available.
-- Estimated is done using prior values collected with observed + forecast daily values
CREATE TEMP TABLE EstimatedValues
AS (
  WITH
    PastDailyValues AS (
      SELECT country, postcode, date, min_temp, max_temp
      FROM `{{ project_id_src }}.{{ k9_datasets_processing }}.weather_daily`
      WHERE value_type = 'OBSERVED'
      UNION DISTINCT
      SELECT country, postcode, date, min_temp, max_temp
      FROM ObservedValues
      UNION DISTINCT
      SELECT country, postcode, date, min_temp, max_temp
      FROM ForecastValues
    ),
    -- All the dates for which we need data.
    AllDates AS (
      SELECT date
      FROM UNNEST(GENERATE_DATE_ARRAY(observed_start_date, forecast_end_date, INTERVAL 1 DAY)) AS date
    ),
    AllPossibleValues AS (
      SELECT
        DistValues.country, DistValues.postcode, AllDates.date
      FROM (
        SELECT DISTINCT country, postcode FROM PastDailyValues
      ) AS DistValues CROSS JOIN AllDates
    ),
    AllMissingValues AS (
      SELECT country, postcode, date, FORMAT_DATE('%m-%d', date) AS month_day
      FROM AllPossibleValues A
      WHERE NOT EXISTS (
        SELECT 1
        FROM PastDailyValues AS P
        WHERE
          P.country = A.country
          AND P.postcode = A.postcode
          AND P.date = A.date
      )
    ),
    PastAvgTempByDay AS (
      SELECT
        country,
        postcode,
        FORMAT_DATE('%m-%d', date) AS month_day,
        AVG(min_temp) AS avg_min_temp,
        AVG(max_temp) AS avg_max_temp
      FROM PastDailyValues
      GROUP BY country, postcode, month_day
    )
  SELECT
    country,
    postcode,
    M.date,
    T.avg_min_temp AS min_temp,
    T.avg_max_temp AS max_temp,
    'ESTIMATED' AS value_type
  FROM AllMissingValues AS M
  JOIN PastAvgTempByDay AS T USING (country, postcode, month_day)
);

-- Check for the uniqueness of the data.
ASSERT (
  SELECT COUNT(*) = 0
  FROM (
    SELECT country, postcode, date
    FROM EstimatedValues
    GROUP BY country, postcode, date
    HAVING COUNT(*) > 1)
) AS 'EstimatedValues temp table uniqueness requirements not met.';

-- Bring all the data together.
CREATE TEMP TABLE AllValues
AS (
  SELECT country, postcode, date, min_temp, max_temp, value_type
  FROM ObservedValues
  UNION ALL
  SELECT country, postcode, date, min_temp, max_temp, value_type
  FROM ForecastValues
  UNION ALL
  SELECT country, postcode, date, min_temp, max_temp, value_type
  FROM EstimatedValues
);

-- Let's make sure the uniqueness of the data.
ASSERT (
  SELECT COUNT(*) = 0
  FROM (
    SELECT country, postcode, date
    FROM AllValues
    GROUP BY country, postcode, date
    HAVING COUNT(*) > 1)
) AS 'AllValues temp table uniqueness requirements not met.';

SET current_ts = CURRENT_TIMESTAMP();

-- Update the Daily weather table.
MERGE `{{ project_id_src }}.{{ k9_datasets_processing }}.weather_daily` AS C
USING AllValues AS N
  ON C.country = N.country AND C.postcode = N.postcode AND C.date = N.date
WHEN MATCHED
  THEN UPDATE SET
    C.min_temp = N.min_temp,
    C.max_temp = N.max_temp,
    C.value_type = N.value_type,
    C.update_timestamp = current_ts
WHEN NOT MATCHED
  THEN INSERT (
    country, postcode, date,
    min_temp, max_temp, value_type, insert_timestamp)
  VALUES (
    N.country, N.postcode, N.date,
    N.min_temp, N.max_temp, N.value_type, current_ts);

-- Cleanup.
DROP TABLE ObservedValues;
DROP TABLE ForecastValues;
DROP TABLE EstimatedValues;
DROP TABLE AllValues;
