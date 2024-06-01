--Creating a table for table and concept counts separately

CREATE TABLE IF NOT EXISTS test.TABLE_COUNTS_OLD (
	CountsID integer NOT NULL,
	TableName varchar(30) NOT NULL,
	ConceptID integer,
	ValueConceptID integer,
	CountTotal integer NOT NULL);
	
CREATE TABLE IF NOT EXISTS test.TABLE_COUNTS_NEW (
	CountsID integer NOT NULL,
	TableName varchar(30) NOT NULL,
	ConceptID integer,
	ValueConceptID integer,
	CountTotal integer NOT NULL);


CREATE TABLE IF NOT EXISTS test.CONCEPT_COUNTS_OLD (
	CountsID integer NOT NULL,
	TableName varchar(30) NOT NULL,
	ConceptID integer NOT NULL,
	ConceptName varchar(255) NOT NULL,
	ValueConceptID integer,
	ValueConceptName varchar(255) NOT NULL,
	CountTotal integer NOT NULL);

CREATE TABLE IF NOT EXISTS test.CONCEPT_COUNTS_NEW (
	CountsID integer NOT NULL,
	TableName varchar(30) NOT NULL,
	ConceptID integer NOT NULL,
	ConceptName varchar(255) NOT NULL,
	ValueConceptID integer,
	ValueConceptName varchar(255) NOT NULL,
	CountTotal integer NOT NULL);

--SELECT TableName, CountTotal FROM test.TABLE_COUNTS_OLD; --works


--Filling in the data from the two most recent runs of NCR --> OMOP

DROP FUNCTION IF EXISTS copyifcomma;

CREATE FUNCTION copyifcomma(tablename text, filename text) RETURNS VOID AS
		$func$
		BEGIN
		EXECUTE (
			format('DO
			$do$
			BEGIN
			IF NOT EXISTS (SELECT FROM %s) THEN
				COPY %s FROM ''%s'' WITH DELIMITER E'','' CSV HEADER NULL '''' ;
			END IF;
			END
			$do$
			', tablename, tablename, filename));
			END
			$func$ LANGUAGE plpgsql;

-- Reading in the data

SELECT copyifcomma('test.TABLE_COUNTS_OLD', 'C:\Users\ssi2309.60050\OneDrive - IKNL\Documenten\results_230407\countstables.csv');
--SELECT TableName, CountTotal FROM test.TABLE_COUNTS_OLD; --works
SELECT copyifcomma('test.TABLE_COUNTS_NEW', 'C:\Users\ssi2309.60050\OneDrive - IKNL\Documenten\results_240201\countstables.csv');
--SELECT TableName, CountTotal FROM test.TABLE_COUNTS_NEW; --works


SELECT copyifcomma('test.CONCEPT_COUNTS_OLD', 'C:\Users\ssi2309.60050\OneDrive - IKNL\Documenten\results_230407\countsconcepts.csv');
--SELECT ConceptName, TableName, CountTotal FROM test.CONCEPT_COUNTS_OLD; --works
SELECT copyifcomma('test.CONCEPT_COUNTS_NEW', 'C:\Users\ssi2309.60050\OneDrive - IKNL\Documenten\results_240201\countsconcepts.csv');
SELECT ConceptName, TableName, CountTotal FROM test.CONCEPT_COUNTS_NEW; --works

