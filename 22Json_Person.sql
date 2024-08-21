USE practise
GO

/******************************************************************************************************************************
										Simple Database Schema 
												For
											PERSON & ADDRESS
												Using
												JSON
******************************************************************************************************************************/

CREATE TABLE Person
(
	PersonId INT IDENTITY(101,1),
	PersonName NVARCHAR(100),
	PersonCategory NVARCHAR(100),
	SSN BIGINT,
	UserPerson INT DEFAULT 1,
	InsertDate DATE DEFAULT GETDATE()
);

CREATE TABLE Address
(
	AddressId INT IDENTITY,
	Address NVARCHAR(100),
	Country NVARCHAR(100),
	Zipcode BIGINT,
	UserPersonId INT DEFAULT 1,
	InsertDate DATE DEFAULT GETDATE()
);

CREATE TABLE PersonAddress
(
	PersonAddressId INT IDENTITY,
	PersonId INT,
	AddressId INT,
	UserPersonid INT,
	InsertDate DATE DEFAULT GETDATE()
);

/****************************  INS FOR PERSON  *********************************/

CREATE OR ALTER PROCEDURE SpPersonIns
@inputjson NVARCHAR(MAX) OUTPUT
AS
BEGIN
	BEGIN TRY 
		BEGIN TRANSACTION
			CREATE TABLE #insertPerson
			(
				PersonId INT ,
				PersonName NVARCHAR(100),
				PersonCategory NVARCHAR(100),
				SSN BIGINT,
				UserPersonId INT ,
				InsertDate DATE 
			);

			CREATE TABLE #tempTable
			(
				PersonName NVARCHAR(100),
				PersonCategory NVARCHAR(100),
				SSN BIGINT,
				UserPersonId INT ,
				InsertDate DATE,
				Address NVARCHAR(100),
				Country NVARCHAR(100),
				Zipcode BIGINT
			);

			INSERT INTO #tempTable (PersonName, PersonCategory, SSN, UserPersonId, InsertDate, Address, Country, Zipcode)
			SELECT DISTINCT personName, PersonCategory, SSN, UserPersonId, InsertDate, Address, Country, Zipcode
			FROM 
			OPENJSON(@inputjson)
			WITH
			(
				PersonName NVARCHAR(100),
				PersonCategory NVARCHAR(100),
				SSN BIGINT,
				UserPersonId INT ,
				InsertDate DATE,
				Address NVARCHAR(100),
				Country NVARCHAR(100),
				Zipcode BIGINT
			);

			
			DECLARE @SSN INT = (SELECT TOP 1 SSN FROM #tempTable)
			IF @SSN IN (SELECT DISTINCT SSN FROM Person)
			BEGIN
			    RAISERROR('MULTIPLE PERSON CANNOT HAVE SAME SSN',16,1);
			END
			ELSE
			BEGIN
			INSERT INTO Person(PersonName, PersonCategory, SSN, UserPersonId, InsertDate)
			OUTPUT inserted .* INTO #insertPerson
			SELECT t.PersonName, t.PersonCategory, t.SSN, t.UserPersonId, t.InsertDate
			FROM #tempTable t
			LEFT JOIN Person p
			ON
			(p.PersonName = t.PersonName OR(P.PersonName IS NULL AND t.PersonName IS NULL) )AND
			(p.PersonCategory = t.PersonCategory OR(P.PersonCategory IS NULL AND t.PersonCategory IS NULL) )AND
			(p.SSN = t.SSN OR(P.SSN IS NULL AND t.SSN IS NULL) )
			WHERE p.PersonName IS NULL;
			
			SET @inputjson = (SELECT i.PersonId, t.* FROM #insertPerson i INNER JOIN #tempTable t ON i.PersonName= t.PersonName AND i.SSN = t.SSN AND i.PersonCategory = t.PersonCategory FOR JSON PATH);

			SELECT * FROM #insertPerson;
			END
			DROP TABLE #insertPerson;
			DROP TABLE #tempTable;

		COMMIT TRANSACTION
	END TRY

	BEGIN CATCH
		IF @@TRANCOUNT>0
		PRINT ERROR_MESSAGE();
		THROW;
		ROLLBACK;
	END CATCH
END



/****************************  INS FOR Address  *********************************/ 

CREATE OR ALTER PROCEDURE SpAddressIns
@inputjson NVARCHAR(MAX) OUTPUT
AS
BEGIN
	BEGIN TRY 
		BEGIN TRANSACTION
			CREATE TABLE #insertAddress
			(
				AddressId INT ,
				Address NVARCHAR(100),
				Country NVARCHAR(100),
				Zipcode BIGINT,
				UserPersonId INT ,
				InsertDate DATE 
			);

			CREATE TABLE #tempTable
			(
				PersonId INT,
				PersonName NVARCHAR(100),
				PersonCategory NVARCHAR(100),
				SSN BIGINT,
				UserPersonId INT ,
				InsertDate DATE,
				Address NVARCHAR(100),
				Country NVARCHAR(100),
				Zipcode BIGINT
			);

			INSERT INTO #tempTable (PersonId, PersonName, PersonCategory, SSN, UserPersonId, InsertDate, Address, Country, Zipcode)
			SELECT DISTINCT PersonId, personName, PersonCategory, SSN, UserPersonId, InsertDate, Address, Country, Zipcode
			FROM 
			OPENJSON(@inputjson)
			WITH
			(
				PersonId INT,
				PersonName NVARCHAR(100),
				PersonCategory NVARCHAR(100),
				SSN BIGINT,
				UserPersonId INT ,
				InsertDate DATE,
				Address NVARCHAR(100),
				Country NVARCHAR(100),
				Zipcode BIGINT
			);-- SELECT * FROM PersonAddress	 EXEC sp_rename 'Person.UserPerson', 'UserPersonId', 'COLUMN';

			--DECLARE @Address NVARCHAR(100) = (SELECT TOP 1 Address FROM #tempTable);
			--DECLARE @Country NVARCHAR(100) = (SELECT TOP 1 Country FROM #tempTable);
			--DECLARE @ZipCode BIGINT = (SELECT TOP 1 Zipcode FROM #tempTable);
			--DECLARE @AddressId INT;

			--IF EXISTS (SELECT 1 FROM Address WHERE [Address] = @Address AND Country = @Country AND Zipcode = @ZipCode) 
			--BEGIN
			--    SELECT @AddressId = AddressId FROM Address WHERE [Address] = @Address AND Country = @Country AND Zipcode = @ZipCode;
			--    SET @inputjson = (SELECT @AddressId AS AddressId, t.* FROM #tempTable t FOR JSON PATH);
			--END
			--ELSE
			--BEGIN
			INSERT INTO #insertAddress (AddressId, Address, Country, Zipcode, UserPersonId, InsertDate)
			SELECT a.AddressId, t.Address, t.Country, t.Zipcode, t.UserPersonId, t.InsertDate
			FROM #tempTable t
			LEFT JOIN Address a ON t.Address = a.Address AND t.Country = a.Country AND t.Zipcode = a.Zipcode;

			DECLARE @NewAddress TABLE (AddressId INT);

			INSERT INTO Address (Address, Country, Zipcode)
			OUTPUT inserted.AddressId INTO @NewAddress (AddressId)
			SELECT t.Address, t.Country, t.Zipcode
			FROM #tempTable t
			LEFT JOIN Address a ON t.Address = a.Address AND t.Country = a.Country AND t.Zipcode = a.Zipcode
			WHERE a.AddressId IS NULL;

			-- Update #insertAddress with the AddressId of newly inserted addresses
			UPDATE ia
			SET ia.AddressId = na.AddressId
			FROM #insertAddress ia
			JOIN @NewAddress na ON ia.Address = (SELECT Address FROM Address WHERE AddressId = na.AddressId);

			SET @inputjson = (
			    SELECT ia.AddressId, t.*
			    FROM #insertAddress ia
			    JOIN #tempTable t ON ia.Address = t.Address AND ia.Country = t.Country AND ia.Zipcode = t.Zipcode
			    FOR JSON PATH
			);
		--end
			SELECT * FROM #insertAddress;

			DROP TABLE #insertAddress;
			DROP TABLE #tempTable;

		COMMIT TRANSACTION
	END TRY

	BEGIN CATCH
		IF @@TRANCOUNT>0
		PRINT ERROR_MESSAGE();
		THROW;
		ROLLBACK;
	END CATCH
END


/****************************  INS FOR PersonAddress  *********************************/ 

CREATE OR ALTER PROCEDURE SpPersonAddressIns
@inputjson NVARCHAR(MAX)
AS
BEGIN
	BEGIN TRY 
		BEGIN TRANSACTION
			CREATE TABLE #insertPersonAddress
			(
				PersonAddressId INT ,
				PersonId INT,
				Address INT,
				UserPersonId INT ,
				InsertDate DATE 
			);

			CREATE TABLE #tempTable
			(
				AddressId INT,
				PersonId INT,
				PersonName NVARCHAR(100),
				PersonCategory NVARCHAR(100),
				SSN BIGINT,
				UserPersonId INT ,
				InsertDate DATE,
				Address NVARCHAR(100),
				Country NVARCHAR(100),
				Zipcode BIGINT
			);

			INSERT INTO #tempTable (AddressId, PersonId, PersonName, PersonCategory, SSN, UserPersonId, InsertDate, Address, Country, Zipcode)
			SELECT DISTINCT AddressId, PersonId, personName, PersonCategory, SSN, UserPersonId, InsertDate, Address, Country, Zipcode
			FROM 
			OPENJSON(@inputjson)
			WITH
			(
				AddressId INT,
				PersonId INT,
				PersonName NVARCHAR(100),
				PersonCategory NVARCHAR(100),
				SSN BIGINT,
				UserPersonId INT ,
				InsertDate DATE,
				Address NVARCHAR(100),
				Country NVARCHAR(100),
				Zipcode BIGINT
			);-- SELECT * FROM Person	 EXEC sp_rename 'Person.UserPerson', 'UserPersonId', 'COLUMN';
			 
			
			INSERT INTO PersonAddress(PersonId, AddressId, UserPersonId, InsertDate)
			OUTPUT inserted .* INTO #insertPersonAddress
			SELECT t.PersonId, t.AddressId, t.UserPersonId, t.InsertDate
			FROM #tempTable t;
			
			
			SELECT * FROM #insertPersonAddress;

			DROP TABLE #insertPersonAddress;
			DROP TABLE #tempTable;

		COMMIT TRANSACTION
	END TRY

	BEGIN CATCH
		IF @@TRANCOUNT>0
		PRINT ERROR_MESSAGE();
		THROW;
		ROLLBACK;
	END CATCH
END


/****************************  TSK FOR Person  *********************************/ 

CREATE OR ALTER PROCEDURE SpPersonTsk
@inputjson NVARCHAR(MAX)
AS
BEGIN
	BEGIN TRY 
		BEGIN TRANSACTION
			DECLARE @entity NVARCHAR(MAX)= JSON_VALUE(@inputjson, '$.Entity');
			IF @entity <> 'Person'
			BEGIN
				RAISERROR('Only person can be inserted.',16,1);
			END
			ELSE
			BEGIN
			EXEC SpPersonIns @inputjson = @inputjson OUTPUT;

			EXEC SpAddressIns @inputjson = @inputjson OUTPUT;

			EXEC SpPersonAddressIns @inputjson = @inputjson;
			END
		COMMIT TRANSACTION
	END TRY

	BEGIN CATCH
		IF @@TRANCOUNT>0
		PRINT ERROR_MESSAGE();
		THROW;
		ROLLBACK;
	END CATCH
END

DECLARE @inputjson NVARCHAR(MAX) = '[
    {
        "Entity": "Person",
        "UserPersonId": 1,
        "InsertDate": "2024-06-22",
        "PersonName": "Mohit Khadka",
        "PersonCategory": "Student",
        "SSN": 63820,
        "Address": "83 BlackRoad",
        "Country": "PP",
        "Zipcode": 79305
    },
    {
        "Entity": "Person",
        "UserPersonId": 1,
        "InsertDate": "2024-05-14",
        "PersonName": "Savana Thapa",
        "PersonCategory": "SInger",
        "SSN": 64839,
        "Address": "23 OldTown",
        "Country": "Ny",
        "Zipcode": 6473
    }
]
'
;
EXECUTE SpPersonTsk @inputjson;
select @inputjson;


									SELECT * FROM Person
									SELECT * FROM Address
									SELECT * FROM PersonAddress

