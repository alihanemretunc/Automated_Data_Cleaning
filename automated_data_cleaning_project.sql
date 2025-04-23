-- Automated Data Cleaning

SELECT *
FROM bakery.us_household_income_for_index;

SELECT * 
FROM bakery.us_household_income_clean;

DELIMITER $$
DROP PROCEDURE IF EXISTS Copy_and_Clean_Data;
CREATE PROCEDURE Copy_and_Clean_Data()
BEGIN
	CREATE TABLE IF NOT EXISTS `us_household_income_clean` (
	  `row_id` int DEFAULT NULL,
	  `id` int DEFAULT NULL,
	  `State_Code` int DEFAULT NULL,
	  `State_Name` text,
	  `State_ab` text,
	  `County` text,
	  `City` text,
	  `Place` text,
	  `Type` text,
	  `Primary` text,
	  `Zip_Code` int DEFAULT NULL,
	  `Area_Code` int DEFAULT NULL,
	  `ALand` int DEFAULT NULL,
	  `AWater` int DEFAULT NULL,
	  `Lat` double DEFAULT NULL,
	  `Lon` double DEFAULT NULL,
	  `TimeStamp` TIMESTAMP DEFAULT NULL
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- COPY DATA TO NEW TABLE
	INSERT INTO us_household_income_clean
    SELECT *, CURRENT_TIMESTAMP
	FROM bakery.us_household_income_for_index;
    
    -- Data Cleaning Steps

	-- 1. Remove Duplicates

	DELETE FROM us_household_income_clean 
	WHERE 
		row_id IN (
		SELECT row_id
	FROM (
		SELECT row_id, id,
			ROW_NUMBER() OVER (
				PARTITION BY id, `TimeStamp`
				ORDER BY id, `TimeStamp`) AS row_num
		FROM 
			us_household_income_clean
	) duplicates
	WHERE 
		row_num > 1
	);

	-- 2. Standardization

	-- Fixing some data quality issues by fixing typos and general standardization

	UPDATE us_household_income_clean
	SET State_Name = 'Georgia'
	WHERE State_Name = 'georia';

	UPDATE us_household_income_clean
	SET County = UPPER(County);

	UPDATE us_household_income_clean
	SET City = UPPER(City);

	UPDATE us_household_income_clean
	SET Place = UPPER(Place);

	UPDATE us_household_income_clean
	SET State_Name = UPPER(State_Name);

	UPDATE us_household_income_clean
	SET `Type` = 'CDP'
	WHERE `Type` = 'CPD';

	UPDATE us_household_income_clean
	SET `Type` = 'Borough'
	WHERE `Type` = 'Boroughs';

END $$
DELIMITER ;

CALL Copy_and_Clean_Data();

SELECT DISTINCT TimeStamp
FROM us_household_income_clean;

SELECT * 
FROM bakery.us_household_income_clean;

-- CREATING EVENT 
DROP EVENT run_data_cleaning;
CREATE EVENT run_data_cleaning 
	ON SCHEDULE EVERY 30 DAY
	DO CALL Copy_and_Clean_Data();

SHOW EVENTS;


-- DEBUGGING OR CHECKING WHETHER Start Proc. WORKS

	SELECT row_id, id, row_num
	FROM (
		SELECT row_id, id,
			ROW_NUMBER() OVER (
				PARTITION BY id
				ORDER BY id) AS row_num
		FROM 
			us_household_income_clean
	) duplicates
	WHERE 
		row_num > 1;


SELECT COUNT(row_id)
FROM us_household_income_clean;

SELECT State_Name, COUNT(State_Name)
FROM us_household_income_clean
GROUP BY State_Name;















