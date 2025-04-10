# Couchbase Time Series SQL++ Queries

## Description

This walkthrough demonstrates how to import a regular Time Series dataset into Capella using Capella UI, how to convert it into the Time Series documents format and query these data subsequently using SQL++ in the Capella Query Workbench.  
<br>


## Explanations for the Time Series dataset

The regular Time Series dataset used in the following steps can be found in this repository under the name 'regular_timeseries_weather_2024.csv'.
The dataset contains daily values for minimum and maximum temperature in 2024 for several locations within Munich (as well as some other data that will not be used in this walkthrough).  
These data (per month) were downloaded from the follwing website and combined into one file: https://opendata.muenchen.de/de/dataset/daten-der-raddauerzaehlstellen-muenchen-2024/resource/f5c738c0-161c-480e-900c-dbb858a3f40d. 
<br><br>


## Create a bucket, a scope and a collection for the Time Series data

1. In Capella UI: create a Bucket with the name ‘time_series’ and select to create a default scope and collection.
2. In Capella Query Workbench: create a new scope for the Time Series data.  

```sql
CREATE SCOPE `time_series`.time IF NOT EXISTS;
```

3. In Capella Query Workbench: create a new collection for the raw time series data within the new scope.  

```sql
CREATE COLLECTION `time_series`.time.regular IF NOT EXISTS;
```

4. Refresh the Browser and check that the new scope and collection were created.
<br>


## Import the Time Series dataset into Capella

In Capella UI: use the Import tool to import the regular Time Series dataset:
  1. Go to the `Data Tools` tab → `Import` tab.
  2. In the Import tab, select `Load from your browser` and choose the file ‘regular_timeseries_weather_2024.csv’ from your computer.
  3. Select the `time_series bucket`, select the `time` scope and the `regular` collection.
  4. Select `UUID` to generate a random Universally Unique Identifier for each document key.
  5. Click `Import`.

In Capella UI: switch from the `Import` tab to the `Documents` tab and check that the documents were imported correctly.
> Notice that each data point (each row from the .csv file) was imported into a new JSON document. 
<br>


## Convert the imported regular Time Series data into Time Series documents ready for Querying

1. In Capella Query Workbench: set the `Query Context` to the `time` scope in the `time_series` dataset.
   
2. In Capella Query Workbench: create a new collection that will contain the converted regular Time Series data.
   
```sql
CREATE COLLECTION weather;
```

3. In Capella Query Workbench: create a primary index on the imported regular time series data in order to query it later.

```sql
CREATE PRIMARY INDEX ON regular;
```

4. In Capella Query Workbench: convert the imported regular Time Series data to Time Series documents.

```sql
-- Insert data into the 'weather' collection, creating a document per each month
INSERT INTO weather
  (KEY _k, VALUE _v, OPTIONS {"expiration": 60*60*24*30})
SELECT 
  "temp:munich:2024:" || LPAD(MONTH_STR, 2, "0") || location _k, -- Create a key for each month
  {
    "location":    location,
    "ts_start":    MIN(timestamp),
    "ts_end":      MAX(timestamp),
    "ts_interval": 1000*60*60*24,
    "ts_data":     ARRAY [t[1],t[2]] FOR t IN
                  ARRAY_AGG([timestamp, TONUMBER(r.`min-temp`), TONUMBER(r.`max-temp`)])
                  END
  } _v
FROM regular AS r
LET timestamp = STR_TO_MILLIS(r.datum, "YYYY.MM.DD"),
    MONTH_STR = SUBSTR(r.datum, 5, 2), -- Extract the month part from the date
    location = r.zaehlstelle
WHERE timestamp
  BETWEEN STR_TO_MILLIS("2024.01.01", "YYYY.MM.DD")
      AND STR_TO_MILLIS("2024.12.31", "YYYY.MM.DD")
GROUP BY location, MONTH_STR -- Group by location and month
RETURNING *;
```

- Result: This query creates one JSON Time Series document per location per month (period: January 2024 till December 2024).
- The query sets the Time Series interval as 1 day
- The `ARRAY_AGG` function aggregates the required Time Series into a single time series data array. Within the time series data array, each time point is constructed as a nested array, containing the date-time stamp, the min and the max temperature data.
- As this is a regular time series, the `ARRAY` operator then strips out the date-time stamps to save storage space. This two-step process ensures that the time series data points are preserved in the correct order.

5. In Capella Query Workbench: create a Secondary index (GSI) for the converted Time Series data in order to query it later.
   
```sql
CREATE INDEX idx_temp ON weather(location, ts_end, ts_start);
```
<br>


## Query the regular Time Series data using SQL++ 

### Show the daily low and high temperatures for the time period from Jan, 1st 2024 till Jan 10th 2024 for the 'Olympia' location.
In Capella Query Workbench:

```sql
-- Define the start and end range for the query
WITH range_start AS (1704067200000), -- Start timestamp in milliseconds (01.01.2024)
     range_end AS (1704927600000) -- End timestamp in milliseconds (11.01.2024)
-- Select the required fields from the weather collection
SELECT MILLIS_TO_TZ(t._t,"UTC") AS day, -- Convert timestamp to UTC
       t._v0 AS low, -- Low temperature
       t._v1 AS high -- High temperature
FROM weather AS d -- Alias `for` the weather collection
UNNEST _timeseries(d, {"ts_ranges": [[range_start, range_end]]}) AS t -- Unnest the time series data with the specified range
WHERE d.location = 'Olympia' -- Filter by location
    AND (d.ts_start <= range_end -- Ensure the data's start timestamp is within the range
        AND d.ts_end >= range_start) -- Ensure the data's end timestamp is within the range
-- Order by timestamp
ORDER BY t._t;
```
- This query uses a CTE (common table expression) to store the date-time range.
- For each time point, the _TIMESERIES function calculates the date-time stamp `_t` and returns the values `_v0` and `_v1`.
- The query adds aliases to the data returned by the _TIMESERIES function and converts the date-time stamp to a readable date-time string.
<br>

### Show the daily low and high temperatures for several non-consecutive time periods for the 'Olympia' location.
In Capella Query Workbench:

`Query Option 1`

```sql
-- Define the date ranges for the query
WITH datarange AS (
     [
       [STR_TO_MILLIS("2024-03-01", "YYYY-MM-DD"), -- Start of first range
        STR_TO_MILLIS("2024-03-07", "YYYY-MM-DD")], -- End of first range
       [STR_TO_MILLIS("2024-05-01", "YYYY-MM-DD"), -- Start of second range
        STR_TO_MILLIS("2024-05-05", "YYYY-MM-DD")], -- End of second range
       [STR_TO_MILLIS("2024-07-01", "YYYY-MM-DD"), -- Start of third range
        STR_TO_MILLIS("2024-07-03", "YYYY-MM-DD")] -- End of third range
     ])
-- Select the required fields from the weather collection
SELECT MILLIS_TO_STR(t._t, "YYYY-MM-DD") AS day, -- Convert timestamp to string format
       t._v0 AS low, -- Low temperature
       t._v1 AS high -- High temperature
FROM weather AS d -- Alias `for` the weather collection
UNNEST _timeseries(d, {"ts_ranges": datarange}) AS t -- Unnest the time series data
WHERE d.location = 'Olympia' -- Filter by location
-- Order by timestamp
ORDER BY t._t;
```

- This query uses a CTE (common table expression) to store the date-time ranges.
- For each time point, the _TIMESERIES function calculates the date-time stamp `_t` and returns the values `_v0` and `_v1`.
- The query adds aliases to the data returned by the _TIMESERIES function and converts the date-time stamp to a readable date-time string.


`Query Option 2`

```sql
-- Define the date ranges for the query
WITH datarange AS (
     [
       [STR_TO_MILLIS("2024-03-01", "YYYY-MM-DD"), -- Start of first range
        STR_TO_MILLIS("2024-03-07", "YYYY-MM-DD")], -- End of first range
       [STR_TO_MILLIS("2024-05-01", "YYYY-MM-DD"), -- Start of second range
        STR_TO_MILLIS("2024-05-05", "YYYY-MM-DD")], -- End of second range
       [STR_TO_MILLIS("2024-07-01", "YYYY-MM-DD"), -- Start of third range
        STR_TO_MILLIS("2024-07-03", "YYYY-MM-DD")] -- End of third range
     ]),
     -- Select distinct document IDs that match the location and time range criteria
     docs AS (
      SELECT DISTINCT RAW META(d).id
      FROM datarange AS tr
      JOIN weather AS d
      ON d.location = 'Olympia' AND (d.ts_start <= tr[1] AND d.ts_end >= tr[0]))
-- Select the required fields from the weather collection using the document IDs
SELECT MILLIS_TO_STR(t._t, "YYYY-MM-DD") AS day, -- Convert timestamp to string format
       t._v0 AS low, -- Low temperature
       t._v1 AS high -- High temperature
FROM weather AS d -- Alias `for` the weather collection
USE KEYS docs -- Use the document IDs from the previous CTE
UNNEST _timeseries(d, {"ts_ranges": datarange}) AS t; -- Unnest the time series data with the specified ranges
```

- This query uses a CTE (common table expression) to store the date-time ranges as well as the distinct document IDs that match the location and time range criteria.
- For each time point, the _TIMESERIES function calculates the date-time stamp `_t` and returns the values `_v0` and `_v1`.
- The query adds aliases to the data returned by the _TIMESERIES function and converts the date-time stamp to a readable date-time string.
<br>

### Show the weeks, weekly averages (min temperature, max temperature and average temperature) and 4-week moving averages (min temperature, max temperature and average temperature) for the time period from July, 1st 2024 till August 31st 2024 for the 'Olympia' location.
In Capella Query Workbench:

```sql
-- Define the start and end of the range in milliseconds
WITH range_start AS (STR_TO_MILLIS("2024-07-01", "YYYY-MM-DD")),
     range_end AS (STR_TO_MILLIS("2024-08-31", "YYYY-MM-DD"))

-- Select the week, average for the week, 4-week moving average, first/last day of each week, and average weekly temperature
SELECT 
      week AS calendar_week, -- Week number
      MILLIS_TO_STR(MIN(t._t), "YYYY-MM-DD") AS first_day_of_week, -- First day of the week
      MILLIS_TO_STR(MAX(t._t), "YYYY-MM-DD") AS last_day_of_week, -- Last day of the week
      week_min_avg AS average_min_temp, -- Average minimum temperature for the week
      week_max_avg AS average_max_temp, -- Average maximum temperature for the week
      (week_min_avg + week_max_avg) / 2 AS avg_weekly_temp, -- Average weekly temperature
      AVG(week_min_avg) OVER (ORDER BY week ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS four_week_min_mov_avg, -- 4-week moving average of minimum temperatures
      AVG(week_max_avg) OVER (ORDER BY week ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS four_week_max_mov_avg, -- 4-week moving average of maximum temperatures
      AVG((week_min_avg + week_max_avg) / 2) OVER (ORDER BY week ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS four_week_mov_avg -- 4-week moving average of average weekly temperatures

-- From the weather collection, unnest the time series data
FROM weather AS d
UNNEST _timeseries(d, {"ts_ranges": [range_start, range_end]}) AS t

-- Filter by location and ensure the time series data falls within the range
WHERE d.location = 'Olympia'
  AND (d.ts_start <= range_end AND d.ts_end >= range_start)

-- Group by week and calculate the average for the week
GROUP BY DATE_PART_MILLIS(t._t, 'iso_week') AS week
LETTING week_min_avg = AVG(t._v0), week_max_avg = AVG(t._v1)
ORDER BY week;
```

- This query uses a CTE (common table expression) to store the date-time range.
- This query uses the Window function (AVG() OVER()) to calculate the moving average values. 
- For each time point, the _TIMESERIES function calculates the date-time stamp `_t` and returns the values `_v0` and `_v1`.
- The query adds aliases to the data returned by the _TIMESERIES function and converts the date-time stamp to a readable date-time string.
<br>


## Visualise the Time Series Query results with the Couchbase Capella Chart Functionality
In Capella UI: within the Query Workbench navigate to the Query Results (bottom) part of the screen and select the `Chart` tab.
In the `Chart` tab: select the `Multi-Connected Points by Columns` Chart type, `calendar_week` for the X-axis and e.g. `avg_weekly_temp` and `four_week_mov_avg` for the Y-axis.
