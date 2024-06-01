--Concept count queries

--Inspecting the data
SELECT ConceptName, TableName, CountTotal FROM test.CONCEPT_COUNTS_OLD;
SELECT ConceptName, TableName, CountTotal FROM test.CONCEPT_COUNTS_NEW;


--SQL SERVER CODE
ALTER PROCEDURE [TESTS].[pr_CompareCountsConcepts]

AS
BEGIN
-- SET NOCOUNT ON added to prevent extra result sets from
-- interfering with SELECT statements.
SET NOCOUNT ON;

TRUNCATE TABLE [TESTS].[CountsConceptsDifference];

INSERT INTO [TESTS].[CountsConceptsDifference]
(
	CountsID,
	TableName,
	ConceptID,
	ConceptName,
	ValueConceptID,
	ValueConceptName,
	CountsOld,
	CountsNew,
	CountsDifference,
    DailyDifference,
	PercentDifference,
)
SELECT
	CT.CountsID AS CountsID,
	CT.TableName AS TableName,
	CT.ConceptID AS ConceptID,
	CT.ConceptName as ConceptName,
	CT.ValueConceptID AS ValueConceptID,
	CT.ValueConceptName AS ValueConceptName,
	CTP.Count AS CountsOld,
	CT.Count AS CountsNew,
	CT.Count - CTP.Count AS CountsDifference,
    (CT.Count - CTP.Count)/DATEDIFF(DAY, CT.Date, CTP.Date) AS DailyDifference,
	CASE 
		WHEN CTP.Count <> 0 THEN ROUND((((CT.Count - CTP.Count)::numeric) / CTP.Count) * 100, 2)
		ELSE 100 AS PercentDifference
FROM [TESTS].CountsTables CT
JOIN [TESTS].CountsTablesPrevious CTP
ON CT.TableName = CTP.TableName
AND CT.ConceptID = CTP.ConceptID
AND CT.ValueConceptID = CTP.ValueConceptID;


--Construct a table for concept count differences
CREATE TABLE IF NOT EXISTS test.CONCEPT_COUNTS_DIFF (
	CountsID integer NOT NULL,
	TableName varchar(30) NOT NULL,
	ConceptID integer NOT NULL,
	ConceptName varchar(255) NOT NULL,
	ValueConceptID integer,
	ValueConceptName varchar(255) NOT NULL,
	CountTotalOld integer NOT NULL,
	CountTotalNew integer NOT NULL);

--Determine which variable(s) to join on
SELECT ValueConceptID, COUNT(ValueConceptID)
FROM test.CONCEPT_COUNTS_OLD
GROUP BY ValueConceptID
HAVING COUNT(ValueConceptID) > 1;

--Checking for the values of 2 and 3 variables together
SELECT TableName, ConceptName, ValueConceptID, COUNT(*) AS occurrence_count
FROM test.CONCEPT_COUNTS_OLD
GROUP BY TableName, ConceptName, ValueConceptID
HAVING COUNT(*) > 1;
	
--Fill the table with the data from both the old and new data tables
INSERT INTO test.CONCEPT_COUNTS_DIFF 
(	CountsID,
	TableName,
	ConceptID,
 	ConceptName,
	ValueConceptID,
 	ValueConceptName,
	CountTotalOld,
	CountTotalNew
)
SELECT o.CountsID, o.TableName, o.ConceptID, o.ConceptName, o.ValueConceptID, o.ValueConceptName, o.CountTotal, n.CountTotal
FROM test.CONCEPT_COUNTS_OLD o
INNER JOIN test.CONCEPT_COUNTS_NEW n
ON o.TableName = n.TableName
AND o.ConceptID = n.ConceptID
AND o.ValueConceptID = n.ValueConceptID;

--Check that it went well and look at the counts side by side
SELECT TableName, ConceptID, ValueConceptID, CountTotalOld, CountTotalNew FROM test.CONCEPT_COUNTS_DIFF; --works

--Add count differences
ALTER TABLE test.CONCEPT_COUNTS_DIFF ADD COLUMN count_differences integer;
UPDATE test.CONCEPT_COUNTS_DIFF d SET count_differences = d.CountTotalNew - d.CountTotalOld; 

--Check that count_differences was filled correctly
SELECT TableName, CountTotalOld, CountTotalNew, count_differences FROM test.CONCEPT_COUNTS_DIFF; --works

-- Daily Difference
-- Add a column with count_difference per day
ALTER TABLE test.CONCEPT_COUNTS_DIFF ADD COLUMN daily_difference real;

UPDATE test.CONCEPT_COUNTS_DIFF d 
SET daily_difference = d.count_differences / EXTRACT(DAY FROM '2024-02-01'::timestamp - '2023-04-07'::timestamp);

-- See whether daily difference was correctly added
SELECT TableName, count_differences, daily_difference FROM test.CONCEPT_COUNTS_DIFF; --works


--Look at different cases for change per table
SELECT DISTINCT TableName FROM test.CONCEPT_COUNTS_DIFF; --We only have concepts from MEASUREMENT, EPISODE & OBSERVATION
SELECT TableName, ConceptName, CountTotalNew, daily_difference FROM test.CONCEPT_COUNTS_DIFF WHERE TableName = 'MEASUREMENT' ORDER BY daily_difference DESC;
SELECT TableName, ConceptName, daily_difference FROM test.CONCEPT_COUNTS_DIFF WHERE TableName = 'EPISODE' ORDER BY daily_difference DESC;
SELECT TableName, ConceptName, daily_difference FROM test.CONCEPT_COUNTS_DIFF WHERE TableName = 'OBSERVATION' ORDER BY daily_difference DESC;

---------------------------- QUERIES --------------------------------
 
--flagging concepts going to zero first, then largest (absolute) decrease
SELECT
    TableName,
    ConceptName,
    ValueConceptID,
	count_differences,
    CASE
        WHEN count_differences < -3 THEN 'Warning - record count decreased substantially, namely by ' || daily_difference || ' for ' || TableName
        WHEN daily_difference < 0 AND CountTotalNew = 0 THEN 'Warning - record count went to 0 for ' || TableName
        ELSE 'Success - count difference ' || daily_difference || ', within expected range for ' || TableName
    END AS result_message
FROM (
    SELECT
        TableName,
        ConceptName,
        ValueConceptID,
		count_differences,
        daily_difference,
        CountTotalNew
    FROM test.CONCEPT_COUNTS_DIFF
) AS subquery
ORDER BY
    CASE WHEN daily_difference < 0 AND CountTotalNew = 0 THEN 0 ELSE 1 END,  -- get all daily differences that decreased (works, tested with different CountTotalNew value)
    daily_difference ASC;  -- Then largest decrease
	
--count_differences --3 or less total decrease is fine for now

--checking specific values to test out queries
SELECT *
FROM test.CONCEPT_COUNTS_DIFF
WHERE TableName = 'EPISODE' AND ConceptName = 'Disease Episode' AND ValueConceptID = 138379; --count total new 24947


--get percental change
ALTER TABLE test.CONCEPT_COUNTS_DIFF ADD COLUMN percental_diff numeric;

UPDATE test.CONCEPT_COUNTS_DIFF d 
SET percental_diff = (CASE 
						  WHEN d.CountTotalOld <> 0 THEN ROUND(((d.count_differences::numeric) / d.CountTotalOld) * 100, 2)
						  ELSE 100 
					  END); 
--handles potential division by 0, ::numeric solves any truncation of integer division, we round up to 2 decimal pts, and use 100% for changes from 0

--check that the percental difference is calculated correctly
SELECT TableName, ConceptName, ValueConceptID, CountTotalOld, count_differences, daily_difference, percental_diff FROM test.CONCEPT_COUNTS_DIFF;


--solution with a bunch of assumptions
SELECT
    TableName,
    ConceptName,
    ValueConceptID,
	count_differences,
	percental_diff,
	daily_difference,
    CASE
        WHEN count_differences < -3 THEN 'Warning - record count decreased substantially, namely by ' || daily_difference || ' for ' || ConceptName
        WHEN daily_difference < 0 AND CountTotalNew = 0 THEN 'Warning - record count went to 0 for ' || ConceptName
		WHEN percental_diff > 100 AND daily_difference > 1 THEN 'Warning - percental difference is ' || percental_diff || ' and more than 1 per day for ' || ConceptName
        ELSE 'Success - daily difference ' || daily_difference || ', within expected range for ' || ConceptName
    END AS result_message
FROM (
    SELECT
        TableName,
        ConceptName,
        ValueConceptID,
		count_differences,
        daily_difference,
        CountTotalNew,
		percental_diff
    FROM test.CONCEPT_COUNTS_DIFF
) AS subquery
ORDER BY
    CASE 
        WHEN daily_difference < 0 AND CountTotalNew = 0 THEN 0  -- Decrease to zero
        WHEN count_differences < -3 THEN 1  -- Large decrease
        WHEN percental_diff > 100 AND daily_difference > 1 THEN 2  -- Large increase defined as 100% change or more (otherwise 100 exactly can also be from 0 to anything)
        ELSE 3  -- All other cases
    END,
    ABS(percental_diff) DESC;  -- Order by absolute value of percentual_diff


--checking what the largest increases in the data are
SELECT
    TableName,
    ConceptName,
    ValueConceptID,
    CountTotalOld,
    count_differences,
    daily_difference,
    percental_diff
FROM
    test.CONCEPT_COUNTS_DIFF
ORDER BY
    percental_diff DESC
LIMIT 10;



--without prioritization (for the demo)
SELECT
    TableName,
    ConceptName,
    ValueConceptID,
	count_differences,
	percental_diff,
	daily_difference,
    CASE
        WHEN count_differences < -3 THEN 'Warning - record count decreased substantially, namely by ' || daily_difference || ' for ' || ConceptName
        WHEN daily_difference < 0 AND CountTotalNew = 0 THEN 'Warning - record count went to 0 for ' || ConceptName
		WHEN percental_diff > 100 AND daily_difference > 1 THEN 'Warning - percental difference is ' || percental_diff || ' and more than 1 per day for ' || ConceptName
        ELSE 'Success - daily difference ' || daily_difference || ', within expected range for ' || ConceptName
    END AS result_message
FROM (
    SELECT
        TableName,
        ConceptName,
        ValueConceptID,
		count_differences,
        daily_difference,
        CountTotalNew,
		percental_diff
    FROM test.CONCEPT_COUNTS_DIFF
) AS subquery;



--selecting specific tables from MEASUREMENT, EPISODE & OBSERVATION
SELECT
    TableName,
    ConceptName,
    ValueConceptName
FROM
    test.CONCEPT_COUNTS_DIFF
WHERE
    TableName = 'MEASUREMENT'











