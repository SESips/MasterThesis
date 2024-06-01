--Checking data is in the tables properly
SELECT TableName, CountTotal FROM test.TABLE_COUNTS_OLD; --works
SELECT TableName, CountTotal FROM test.TABLE_COUNTS_NEW; --works
SELECT ConceptName, TableName, CountTotal FROM test.CONCEPT_COUNTS_OLD; --works
SELECT ConceptName, TableName, CountTotal FROM test.CONCEPT_COUNTS_NEW; --works


--Construct a table for the count differences
CREATE TABLE IF NOT EXISTS test.TABLE_COUNTS_DIFF (
	CountsID integer NOT NULL,
	TableName varchar(30) NOT NULL,
	ConceptID integer,
	ValueConceptID integer,
	CountTotalOld integer NOT NULL,
	CountTotalNew integer NOT NULL);


CREATE TABLE IF NOT EXISTS test.CONCEPT_COUNTS_DIFF (
	CountsID integer NOT NULL,
	TableName varchar(30) NOT NULL,
	ConceptID integer NOT NULL,
	ConceptName varchar(255) NOT NULL,
	ValueConceptID integer,
	ValueConceptName varchar(255) NOT NULL,
	CountTotalOld integer NOT NULL,
	CountTotalNew integer NOT NULL);
	
	
--Fill the table with the data from both the old and new data tables
INSERT INTO test.TABLE_COUNTS_DIFF 
(	CountsID,
	TableName,
	ConceptID,
	ValueConceptID,
	CountTotalOld,
	CountTotalNew
)
SELECT o.CountsID, o.TableName, o.ConceptID, o.ValueConceptID, o.CountTotal, n.CountTotal
FROM test.TABLE_COUNTS_OLD o
INNER JOIN test.TABLE_COUNTS_NEW n
ON o.TableName = n.TableName

--Check that it went well and look at the counts side by side
SELECT TableName, CountTotalOld, CountTotalNew FROM test.TABLE_COUNTS_DIFF; --works

------------------------------------------------------------------------
--For ETL from here
------------------------------------------------------------------------



--Add count differences
ALTER TABLE test.TABLE_COUNTS_DIFF ADD COLUMN count_differences integer;
UPDATE test.TABLE_COUNTS_DIFF d SET count_differences = d.CountTotalNew - d.CountTotalOld; 

--Check that count_differences was filled correctly
SELECT TableName, CountTotalOld, CountTotalNew, count_differences FROM test.TABLE_COUNTS_DIFF; --works

--create a days_between variable!!
-- Add a column with count_difference per day
ALTER TABLE test.TABLE_COUNTS_DIFF ADD COLUMN daily_difference real;

-- Update the daily_difference column
UPDATE test.TABLE_COUNTS_DIFF d 
SET daily_difference = d.count_differences / EXTRACT(DAY FROM '2024-02-01'::timestamp - '2023-04-07'::timestamp);

--See whether daily difference was correctly added
SELECT TableName, count_differences, daily_difference FROM test.TABLE_COUNTS_DIFF; --works


-------------------------------------------------------------------------------
-- Above the tables are altered to get all required columns
-- Below DQ checks are written, these come from the analysis for intervention 2
-------------------------------------------------------------------------------


--Checking that the changes are between specific ranges



--Idea of how to check this after the ETL has exported table counts
WITH count_differences AS (
    SELECT
        TableName,
        SUM(CASE WHEN count_differences = 0 THEN 1 ELSE 0 END) AS zero_count_diff,
        COUNT(*) AS total_count
    FROM
        test.TABLE_COUNTS_DIFF
    WHERE
        TableName IN ('CDM_SOURCE', 'DEVICE_EXPOSURE', 'DOSE_ERA', 'SPECIMEN')
    GROUP BY
        TableName
)
SELECT
    CASE
        WHEN COUNT(*) = 4 AND SUM(zero_count_diff) = 4 THEN 'Success - counts have not changed for CDM_SOURCE, DEVICE_EXPOSURE, DOSE_ERA and SPECIMEN'
        WHEN SUM(zero_count_diff) = 0 THEN 'Warning - count has changed for CDM_SOURCE, DEVICE_EXPOSURE, DOSE_ERA and SPECIMEN'
		WHEN SUM(zero_count_diff) = 1 THEN 'Warning - count has changed for 3 tables out of CDM_SOURCE, DEVICE_EXPOSURE, DOSE_ERA or SPECIMEN'
        WHEN SUM(zero_count_diff) = 2 THEN 'Warning - count has changed for 2 tables out of CDM_SOURCE, DEVICE_EXPOSURE, DOSE_ERA or SPECIMEN'
        WHEN SUM(zero_count_diff) = 3 THEN 'Warning - count has changed for 1 table out of CDM_SOURCE, DEVICE_EXPOSURE, DOSE_ERA or SPECIMEN'
		ELSE 'Warning - too many (>4) tables where the count has not changed'
    END AS result_message
FROM
    count_differences;
--Should check that this works with all defined cases (all options for results that are suspicious)


--To check that there is change for the other 12 tables
WITH count_differences AS (
    SELECT
        TableName,
        SUM(CASE WHEN count_differences = 0 THEN 1 ELSE 0 END) AS zero_count_diff,
        COUNT(*) AS total_count
    FROM
        test.TABLE_COUNTS_DIFF
    WHERE
        TableName NOT IN ('CDM_SOURCE', 'DEVICE_EXPOSURE', 'DOSE_ERA', 'SPECIMEN')
    GROUP BY
        TableName
)
SELECT
    CASE
        WHEN COUNT(*) = 12 AND SUM(zero_count_diff) = 0 THEN 'Success - counts have changed for all 12 tables where we expect change'
        WHEN SUM(zero_count_diff) = 12 THEN 'Warning - count has not changed for any table'
		ELSE 'Warning - too many tables (>4) where the count has not changed'
    END AS result_message
FROM
    count_differences;



--For ranges of change for the 12 tables where we expect change
--Check the count differences
	
--Tables for which we have 6 samples of regular change
--DEATH, EPISODE, OBSERVATION_PERIOD, PERSON
SELECT 
    CASE 
        WHEN daily_difference BETWEEN 1.3 AND 16.3 AND TableName = 'DEATH' THEN 'Success - count difference within expected range for ' || TableName
		WHEN daily_difference BETWEEN 0 AND 1.3 AND TableName = 'DEATH' THEN 'Warning - count difference lower than expected for '  || TableName
		WHEN daily_difference BETWEEN 32.9 AND 129.9 AND TableName = 'EPISODE' THEN 'Success - count difference within expected range for ' || TableName
		WHEN daily_difference BETWEEN 0 AND 32.9 AND TableName = 'EPISODE' THEN 'Warning - count difference lower than expected for '  || TableName
        WHEN daily_difference BETWEEN 0 AND 19.5 AND TableName = 'OBSERVATION_PERIOD' THEN 'Success - count difference within expected range for ' || TableName
		WHEN daily_difference BETWEEN 0 AND 19.5 AND TableName = 'PERSON' THEN 'Success - count difference within expected range for ' || TableName
        WHEN daily_difference < 0 THEN 'Warning - record count decreased for ' || TableName
		ELSE 'Warning - increase in table count per day was higher than expected with ' || daily_difference || ' for ' || TableName
    END AS result_message
FROM 
    test.TABLE_COUNTS_DIFF
WHERE 
    TableName IN ('DEATH', 'EPISODE', 'OBSERVATION_PERIOD', 'PERSON');

--DQ checks for tables where there were only two examples of a regular change
--change should not be below zero for OBSERVATION, EPISODE_EVENT 
SELECT 
    CASE
        WHEN daily_difference < 0 AND TableName = 'OBSERVATION' THEN 'Warning - table count decreased for ' || TableName
		WHEN daily_difference > 38.1 AND TableName = 'OBSERVATION' THEN 'Warning - count difference was suspiciously high with ' || daily_difference || ' for ' || TableName
        WHEN daily_difference < 0 AND TableName = 'EPISODE_EVENT' THEN 'Warning - table count decreased for ' || TableName
        ELSE 'Increase in table count per day was ' || daily_difference || ' for ' || TableName
    END AS result_message
FROM 
    test.TABLE_COUNTS_DIFF
WHERE 
    TableName IN ('OBSERVATION', 'EPISODE_EVENT');
	
SELECT 
	CASE 
		WHEN TableName = 'EPISODE_EVENT' AND count_differences = (
		SELECT 
			SUM(count_differences)
		FROM test.TABLE_COUNTS_DIFF
		WHERE
			TableName IN ('CONDITION_OCCURRENCE', 'DEVICE_EXPOSURE', 'DRUG_EXPOSURE', 'MEASUREMENT', 'OBSERVATION', 'PROCEDURE_OCCURRENCE', 'SPECIMEN'))
		THEN 'Success - count difference matches sum of differences for related tables for ' || TableName
		ELSE 'Warning - count difference does not match sum of difference for related tables for ' || TableName
	END AS result_message
FROM
	test.TABLE_COUNTS_DIFF
WHERE 
    TableName IN ('EPISODE_EVENT'); --add the tables from the calculation
	
	
	
--For tables for which we had 3 samples of regular change
--CONDITION_ERA, CONDITION_OCCURRENCE (used the 95% CI for both)
--DRUG_ERA (used 90% CI), DRUG_EXPOSURE (used 95% CI)
--MEASUREMENT, PROCEDURE_OCCURRENCE (used 95% CI for both)
SELECT 
    CASE 
        WHEN daily_difference BETWEEN 35 AND 84.5 AND TableName = 'DRUG_ERA' THEN 'Success - count difference within expected range for ' || TableName
		WHEN daily_difference BETWEEN 0 AND 35 AND TableName = 'DRUG_ERA' THEN 'Warning - count difference lower than expected for '  || TableName
		WHEN daily_difference BETWEEN 22.3 AND 103.6 AND TableName = 'DRUG_EXPOSURE' THEN 'Success - count difference within expected range for ' || TableName
		WHEN daily_difference BETWEEN 0 AND 22.3 AND TableName = 'DRUG_EXPOSURE' THEN 'Warning - count difference lower than expected for '  || TableName
		WHEN daily_difference BETWEEN 34.7 AND 549.1 AND TableName = 'MEASUREMENT' THEN 'Success - count difference within expected range for ' || TableName
		WHEN daily_difference BETWEEN 0 AND 34.7 AND TableName = 'MEASUREMENT' THEN 'Warning - count difference lower than expected for '  || TableName
        WHEN daily_difference BETWEEN 0 AND 80.3 AND TableName = 'CONDITION_ERA' THEN 'Success - count difference within expected range for ' || TableName
		WHEN daily_difference BETWEEN 0 AND 82.5 AND TableName = 'CONDITION_OCCURRENCE' THEN 'Success - count difference within expected range for ' || TableName
        WHEN daily_difference BETWEEN 0 AND 196.2 AND TableName = 'PROCEDURE_OCCURRENCE' THEN 'Success - count difference within expected range for ' || TableName
		WHEN daily_difference < 0 THEN 'Warning - record count decreased for ' || TableName
		ELSE 'Warning - increase in table count per day was higher than expected with ' || daily_difference || ' for ' || TableName
    END AS result_message
FROM 
    test.TABLE_COUNTS_DIFF
WHERE 
    TableName IN ('CONDITION_ERA', 'CONDITION_OCCURRENCE', 'DRUG_ERA', 'DRUG_EXPOSURE', 'MEASUREMENT', 'PROCEDURE_OCCURRENCE');



--SQL server version
--For tables for which we had 3 samples of regular change
SELECT 
    CASE 
        WHEN daily_difference BETWEEN 35 AND 84.5 AND TableName = 'DRUG_ERA' THEN CONCAT('Success - count difference within expected range for ', TableName)
		WHEN daily_difference BETWEEN 0 AND 35 AND TableName = 'DRUG_ERA' THEN CONCAT('Warning - count difference lower than expected for ', TableName)
		WHEN daily_difference BETWEEN 22.3 AND 103.6 AND TableName = 'DRUG_EXPOSURE' THEN CONCAT('Success - count difference within expected range for ', TableName)
		WHEN daily_difference BETWEEN 0 AND 22.3 AND TableName = 'DRUG_EXPOSURE' THEN CONCAT('Warning - count difference lower than expected for ', TableName)
		WHEN daily_difference BETWEEN 34.7 AND 549.1 AND TableName = 'MEASUREMENT' THEN CONCAT('Success - count difference within expected range for ', TableName)
		WHEN daily_difference BETWEEN 0 AND 34.7 AND TableName = 'MEASUREMENT' THEN CONCAT('Warning - count difference lower than expected for ', TableName)
        WHEN daily_difference BETWEEN 0 AND 80.3 AND TableName = 'CONDITION_ERA' THEN CONCAT('Success - count difference within expected range for ', TableName)
		WHEN daily_difference BETWEEN 0 AND 82.5 AND TableName = 'CONDITION_OCCURRENCE' THEN CONCAT('Success - count difference within expected range for ', TableName)
        WHEN daily_difference BETWEEN 0 AND 196.2 AND TableName = 'PROCEDURE_OCCURRENCE' THEN CONCAT('Success - count difference within expected range for ', TableName)
		WHEN daily_difference < 0 THEN CONCAT('Warning - record count decreased for ', TableName)
		ELSE CONCAT('Warning - increase in table count per day was higher than expected with ', daily_difference, ' for ', TableName)
    END AS result_message
FROM 
    test.TABLE_COUNTS_DIFF
WHERE 
    TableName IN ('CONDITION_ERA', 'CONDITION_OCCURRENCE', 'DRUG_ERA', 'DRUG_EXPOSURE', 'MEASUREMENT', 'PROCEDURE_OCCURRENCE');

--new split up query for OBSERVATION & EPISODE EVENT in SQL SERVER language
SELECT 
    CASE
        WHEN daily_difference < 0 AND TableName = 'OBSERVATION' THEN 'Warning - table count decreased for ' || TableName
		WHEN daily_difference > 38.1 AND TableName = 'OBSERVATION' THEN 'Warning - count difference was suspiciously high with ' || daily_difference || ' for ' || TableName
        WHEN daily_difference < 0 AND TableName = 'EPISODE_EVENT' THEN 'Warning - table count decreased for ' || TableName
        ELSE 'Increase in table count per day was ' || daily_difference || ' for ' || TableName
    END AS result_message
FROM 
    test.TABLE_COUNTS_DIFF
WHERE 
    TableName IN ('OBSERVATION', 'EPISODE_EVENT');

