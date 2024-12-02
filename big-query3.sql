CREATE TABLE `playground-s-11-1b5c1ab9.test_dataset.student_records`
 (
   rollNo INT OPTIONS(description="student roll no"),
   personalInfo STRUCT<first_name STRING, last_name STRING, age INT, bankDetails STRUCT<balance DECIMAL, accountNo INT>>,
   department STRING
 )
 OPTIONS(
   description="student table with a struct column"
 );

INSERT INTO `playground-s-11-1b5c1ab9.test_dataset.student_records` (rollNo, personalInfo, department) VALUES (1,STRUCT("Yash","Raj",36, STRUCT(10000.45, 1234567890)),"commerce");
INSERT INTO `playground-s-11-1b5c1ab9.test_dataset.student_records` (rollNo, personalInfo, department) VALUES (2,STRUCT("Joe","Biden",37, STRUCT(11000.55, 1234567891)),"arts");
INSERT INTO `playground-s-11-1b5c1ab9.test_dataset.student_records` (rollNo, personalInfo, department) VALUES (3,STRUCT("Validmir","Puntin",40, STRUCT(12000.65, 1234567892)),"science");

CREATE OR REPLACE FUNCTION `test_dataset.strcon`(input_column STRING)
RETURNS STRING AS ( CONCAT(input_column, 'xyz'));

SELECT test_dataset.strcon(CAST(rollNo AS STRING)) AS rollNo FROM `playground-s-11-1b5c1ab9.test_dataset.student_records`;

select rollNo as rollNo, STRUCT(personalInfo.first_name as first_name, personalInfo.last_name as last_name, personalInfo.age as age, STRUCT(personalInfo.bankDetails.balance as balance, personalInfo.bankDetails.accountNo as accountNo) as bankDetails) as personalInfo, department as department from `playground-s-11-1b5c1ab9.test_dataset.student_records`;

select rollNo as rollNo, STRUCT(personalInfo.first_name as first_name, personalInfo.last_name as last_name, personalInfo.age as age, STRUCT(personalInfo.bankDetails.balance as balance, personalInfo.bankDetails.accountNo as accountNo) as bankDetails) as personalInfo, test_dataset.strcon(CAST(department AS STRING)) as department from `playground-s-11-1b5c1ab9.test_dataset.student_records`;

CREATE TABLE `playground-s-11-1b5c1ab9.test_dataset.student_records_encrypted`
AS
(SELECT rollNo AS rollNo,
        STRUCT(personalInfo.first_name AS first_name,
               personalInfo.last_name AS last_name,
               personalInfo.age AS age,
               STRUCT(personalInfo.bankDetails.balance AS balance,
                      personalInfo.bankDetails.accountNo AS accountNo) AS bankDetails) AS personalInfo,
        test_dataset.strcon(CAST(department AS STRING)) AS department
        FROM `playground-s-11-1b5c1ab9.test_dataset.student_records`);


SELECT
rollNo as rollNo,
STRUCT(
    personalInfo.first_name as first_name,
	personalInfo.last_name as last_name,
	personalInfo.age as age,
	STRUCT(
	      personalInfo.bankDetails.balance as balance,
		  personalInfo.bankDetails.accountNo as accountNo
		  ) as bankDetails
	) as personalInfo,
department as department
FROM
`playground-s-11-1b5c1ab9.test_dataset.student_records`;
