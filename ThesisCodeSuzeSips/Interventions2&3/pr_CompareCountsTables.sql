USE [FAIR]
GO

/****** Object:  StoredProcedure [TESTS].[pr_CompareCountsTables]    Script Date: 25-3-2024 09:11:49 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		Peter Prinsen
-- Create date: 25-03-2024
-- Description:	Fill Counts table
-- =============================================

ALTER PROCEDURE [TESTS].[pr_CompareCountsTables]

AS
BEGIN
-- SET NOCOUNT ON added to prevent extra result sets from
-- interfering with SELECT statements.
SET NOCOUNT ON;

TRUNCATE TABLE [TESTS].[CountsTablesDifference];

INSERT INTO [TESTS].[CountsTablesDifference]
(
	TableName,
	ConceptID,
	ValueConceptID,
	CountsOld,
	CountsNew,
	CountsDifference,
    DailyDifference
)
SELECT
	CT.TableName AS TableName,
	CT.ConceptID AS ConceptID,
	CT.ValueConceptID AS ValueConceptID,
	CTP.Count AS CountsOld,
	CT.Count AS CountsNew,
	CT.Count - CTP.Count AS CountsDifference,
    (CT.Count - CTP.Count)/DATEDIFF(DAY, CT.Date, CTP.Date) AS DailyDifference
FROM [TESTS].CountsTables CT
JOIN [TESTS].CountsTablesPrevious CTP
ON CT.TableName = CTP.TableName 
AND CT.ConceptID IS NULL 
AND CTP.ConceptID IS NULL 
AND CT.ValueConceptID IS NULL 
AND CTP.ValueConceptID IS NULL;

--Checking that the changes are between specific ranges

TRUNCATE TABLE [TESTS].[ResultMessage];

INSERT INTO [TESTS].[ResultMessage] 
CONCAT('Date of previous counts was ', CTP.Date,'. Date of newest counts was ', CT.Date)
--have to check that this works properly with this way of accessing CTP.Date and CT.Date

--Idea of how to check this after the ETL has exported table counts
WITH CountsDifference AS (
    SELECT
        TableName,
        SUM(CASE WHEN CountsDifference = 0 THEN 1 ELSE 0 END) AS zero_count_diff,
        COUNT(*) AS total_count
    FROM
        [TESTS].[CountsTablesDifference]
    WHERE
        TableName IN ('CDM_SOURCE', 'DEVICE_EXPOSURE', 'DOSE_ERA', 'SPECIMEN')
    GROUP BY
        TableName
)
INSERT INTO [TESTS].[ResultMessage]
SELECT
    CASE
        WHEN COUNT(*) = 4 AND SUM(zero_count_diff) = 4 THEN 'Success - counts have not changed for CDM_SOURCE, DEVICE_EXPOSURE, DOSE_ERA and SPECIMEN'
        WHEN SUM(zero_count_diff) = 0 THEN 'Warning - count has changed for CDM_SOURCE, DEVICE_EXPOSURE, DOSE_ERA and SPECIMEN'
		WHEN SUM(zero_count_diff) = 1 THEN 'Warning - count has changed for 3 tables out of CDM_SOURCE, DEVICE_EXPOSURE, DOSE_ERA or SPECIMEN'
        WHEN SUM(zero_count_diff) = 2 THEN 'Warning - count has changed for 2 tables out of CDM_SOURCE, DEVICE_EXPOSURE, DOSE_ERA or SPECIMEN'
        WHEN SUM(zero_count_diff) = 3 THEN 'Warning - count has changed for 1 table out of CDM_SOURCE, DEVICE_EXPOSURE, DOSE_ERA or SPECIMEN'
		ELSE 'Warning - too many (>4) tables where the count has not changed'
    END AS ResultMessage
FROM
    CountsDifference;

--For ranges of change for the 12 tables where we expect change
--Check the count differences
	
--Tables for which we have 6 samples of regular change
--DEATH, EPISODE, OBSERVATION_PERIOD, PERSON
INSERT INTO [TESTS].[ResultMessage]
SELECT 
    CASE 
        WHEN DailyDifference BETWEEN 1.3 AND 16.3 AND TableName = 'DEATH' THEN CONCAT('Success - count difference ',DailyDifference,', within expected range 1.3-16.3 for ', TableName)
		WHEN DailyDifference BETWEEN 0 AND 1.3 AND TableName = 'DEATH' THEN CONCAT('Warning - count difference ',DailyDifference,', lower than 1.3 for ',TableName)
		WHEN DailyDifference BETWEEN 32.9 AND 129.9 AND TableName = 'EPISODE' THEN CONCAT('Success - count difference ',DailyDifference,', within expected range 32.9-129.9 for ',TableName)
		WHEN DailyDifference BETWEEN 0 AND 32.9 AND TableName = 'EPISODE' THEN CONCAT('Warning - count difference ',DailyDifference,', lower than 32.9 for ',TableName)
        WHEN DailyDifference BETWEEN 0 AND 19.5 AND TableName = 'OBSERVATION_PERIOD' THEN CONCAT('Success - count difference ',DailyDifference,', within expected range 0-19.5 for ',TableName)
		WHEN DailyDifference BETWEEN 0 AND 19.5 AND TableName = 'PERSON' THEN CONCAT('Success - count difference ',DailyDifference,', within expected range 0-19.5 for ',TableName)
		WHEN DailyDifference > 16.3 AND TableName = 'DEATH' THEN CONCAT('Warning - count difference ',DailyDifference,', higher than 16.3 for ',TableName)
		WHEN DailyDifference > 129.9 AND TableName = 'EPISODE' THEN CONCAT('Warning - count difference ',DailyDifference,', higher than 129.9 for ',TableName)
		WHEN DailyDifference > 19.5 AND TableName = 'OBSERVATION_PERIOD' THEN CONCAT('Warning - count difference ',DailyDifference,', higher than 19.5 for ',TableName)
		WHEN DailyDifference > 19.5 AND TableName = 'PERSON' THEN CONCAT('Warning - count difference ',DailyDifference,', higher than 19.5 for ',TableName)
		WHEN DailyDifference < 0 AND CountsNew = 0 THEN CONCAT('Warning - record count went to 0 for ',TableName)
		ELSE CONCAT('Warning - record count decreased for ',TableName,', namely ',DailyDifference)
    END AS ResultMessage
FROM 
    [TESTS].[CountsTablesDifference]
WHERE
    TableName IN ('DEATH', 'EPISODE', 'OBSERVATION_PERIOD', 'PERSON');

--For tables for which we had some samples of regular change
--CONDITION_ERA, CONDITION_OCCURRENCE (used the 95% CI for both)
--DRUG_ERA (used 90% CI), DRUG_EXPOSURE (used 95% CI)
--MEASUREMENT, PROCEDURE_OCCURRENCE (used 95% CI for both)
INSERT INTO [TESTS].[ResultMessage]
SELECT 
    CASE 
        WHEN DailyDifference BETWEEN 35 AND 84.5 AND TableName = 'DRUG_ERA' THEN  CONCAT('Success - count difference ',DailyDifference,', within expected range 35-84.5 for ',TableName)
		WHEN DailyDifference BETWEEN 0 AND 35 AND TableName = 'DRUG_ERA' THEN  CONCAT('Warning - count difference ',DailyDifference,', lower than 35 for ',TableName)
		WHEN DailyDifference BETWEEN 22.3 AND 103.6 AND TableName = 'DRUG_EXPOSURE' THEN  CONCAT('Success - count difference ',DailyDifference,', within expected range 22.3-103.6 for ',TableName)
		WHEN DailyDifference BETWEEN 0 AND 22.3 AND TableName = 'DRUG_EXPOSURE' THEN  CONCAT('Warning - count difference ',DailyDifference,', lower than 22.3 for ',TableName)
		WHEN DailyDifference BETWEEN 34.7 AND 549.1 AND TableName = 'MEASUREMENT' THEN  CONCAT('Success - count difference ',DailyDifference,',  within expected range 34.7-549.1 for ',TableName)
		WHEN DailyDifference BETWEEN 0 AND 34.7 AND TableName = 'MEASUREMENT' THEN  CONCAT('Warning - count difference ',DailyDifference,', lower than 34.7 for ',TableName)
        WHEN DailyDifference BETWEEN 0 AND 80.3 AND TableName = 'CONDITION_ERA' THEN  CONCAT('Success - count difference ',DailyDifference,', within expected range 0-80.3 for ',TableName)
		WHEN DailyDifference BETWEEN 0 AND 82.5 AND TableName = 'CONDITION_OCCURRENCE' THEN  CONCAT('Success - count difference ',DailyDifference,', within expected range 0-82.5 for ',TableName)
        WHEN DailyDifference BETWEEN 0 AND 196.2 AND TableName = 'PROCEDURE_OCCURRENCE' THEN  CONCAT('Success - count difference ',DailyDifference,', within expected range 0-196.2 for ',TableName)
		WHEN DailyDifference > 84.5 AND TableName = 'DRUG_ERA' THEN CONCAT('Warning - count difference ',DailyDifference,', higher than 84.5 for ',TableName)
		WHEN DailyDifference > 103.6 AND TableName = 'DRUG_EXPOSURE' THEN CONCAT('Warning - count difference ',DailyDifference,', higher than 103.6 for ',TableName)
		WHEN DailyDifference > 549.1 AND TableName = 'MEASUREMENT' THEN CONCAT('Warning - count difference ',DailyDifference,', higher than 549.1 for ',TableName)
		WHEN DailyDifference > 80.3 AND TableName = 'CONDITION_ERA' THEN CONCAT('Warning - count difference ',DailyDifference,', higher than 80.3 for ',TableName)
		WHEN DailyDifference > 82.5 AND TableName = 'CONDITION_OCCURRENCE' THEN CONCAT('Warning - count difference ',DailyDifference,', higher than 82.5 for ',TableName)
		WHEN DailyDifference > 196.2 AND TableName = 'PROCEDURE_OCCURRENCE' THEN CONCAT('Warning - count difference ',DailyDifference,', higher than 196.2 for ',TableName)
		WHEN DailyDifference < 0 AND CountsNew = 0 THEN CONCAT('Warning - record count went to 0 for ',TableName)
		ELSE  CONCAT('Warning - record count decreased for ',TableName,', namely ',DailyDifference)
    END AS ResultMessage
FROM 
    [TESTS].[CountsTablesDifference]
WHERE 
    TableName IN ('CONDITION_ERA', 'CONDITION_OCCURRENCE', 'DRUG_ERA', 'DRUG_EXPOSURE', 'MEASUREMENT', 'PROCEDURE_OCCURRENCE');

--For tables for which there were very few samples of regular change
--OBSERVATION and EPISODE_EVENT
INSERT INTO [TESTS].[ResultMessage]
SELECT 
    CASE
        WHEN DailyDifference < 0 AND CountsNew = 0 THEN CONCAT('Warning - record count went to 0 for ',TableName) --check/ask whether this works with overlapping cases
		WHEN DailyDifference < 0 AND TableName = 'OBSERVATION' THEN CONCAT('Warning - table count decreased, namely ',DailyDifference,' for ',TableName)
		WHEN DailyDifference > 38.1 AND TableName = 'OBSERVATION' THEN CONCAT('Warning - count difference was above 38.1, thus suspiciously high with ',DailyDifference,' for ',TableName)
		WHEN DailyDifference BETWEEN 0 AND 38.1 TableName = 'OBSERVATION' THEN CONCAT('Success - count difference was ',DailyDifference,', within expected range 0-38.1 for ',TableName)
        WHEN DailyDifference < 0 AND TableName = 'EPISODE_EVENT' THEN CONCAT('Warning - table count decreased, namely ',DailyDifference,' for ',TableName)
		ELSE CONCAT('Increase in table count per day was ',DailyDifference,' for ',TableName)
    END AS ResultMessage
FROM 
    [TESTS].[CountsTablesDifference]
WHERE 
    TableName IN ('OBSERVATION', 'EPISODE_EVENT');

INSERT INTO [TESTS].[ResultMessage]
SELECT 
	CASE 
		WHEN TableName = 'EPISODE_EVENT' AND CountsDifference = (
		SELECT 
			SUM(CountsDifference)
		FROM [TESTS].[CountsTablesDifference]
		WHERE
			TableName IN ('CONDITION_OCCURRENCE', 'DEVICE_EXPOSURE', 'DRUG_EXPOSURE', 'MEASUREMENT', 'OBSERVATION', 'PROCEDURE_OCCURRENCE', 'SPECIMEN'))
		THEN CONCAT('Success - count difference matches sum of differences for related tables for ',TableName)
		ELSE CONCAT('Warning - count difference does not match sum of difference for related tables for ',TableName)
	END AS ResultMessage
FROM
	[TESTS].[CountsTablesDifference]
WHERE 
    TableName IN ('EPISODE_EVENT'); --add the tables from the calculation	

END;
GO