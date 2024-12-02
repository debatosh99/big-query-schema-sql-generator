CREATE TABLE `playground-s-11-04b55e04.test_dataset.student_records`
 (
   rollNo INT OPTIONS(description="student roll no"),
   personalInfo STRUCT<first_name STRING, last_name STRING, age INT, bankDetails STRUCT<balance DECIMAL, account STRUCT<accountNo INT, ifsc STRING>, bankname STRING>, gender STRING>,
   department STRING,
   college STRING
 )
 OPTIONS(
   description="student table with a struct column"
 );

INSERT INTO `playground-s-11-04b55e04.test_dataset.student_records` (rollNo, personalInfo, department, college) VALUES (1,STRUCT("Yash","Raj",36, STRUCT(10000.45, STRUCT(00061234,"ICIC01112"),"ICICI"),"male"),"commerce","sp jain");
INSERT INTO `playground-s-11-04b55e04.test_dataset.student_records` (rollNo, personalInfo, department, college) VALUES (2,STRUCT("Joe","Biden",37, STRUCT(11000.55, STRUCT(00061234,"HDFC01112"),"HDFC"),"female"),"arts","xlri");
INSERT INTO `playground-s-11-04b55e04.test_dataset.student_records` (rollNo, personalInfo, department, college) VALUES (3,STRUCT("Validmir","Puntin",40, STRUCT(12000.65, STRUCT(00061234,"SBI01112"),"SBI"),"male"),"science","iitb");

CREATE OR REPLACE FUNCTION `test_dataset.strcon`(input_column STRING)
RETURNS STRING AS ( CONCAT(input_column, 'xyz'));

SELECT test_dataset.strcon(CAST(rollNo AS STRING)) AS rollNo FROM `playground-s-11-04b55e04.test_dataset.student_records`;

select rollNo as rollNo, STRUCT(personalInfo.first_name as first_name, personalInfo.last_name as last_name, personalInfo.age as age, STRUCT(personalInfo.bankDetails.balance as balance, STRUCT(personalInfo.bankDetails.account.accountNo as accountNo, personalInfo.bankDetails.account.ifsc as ifsc) as account) as bankDetails, personalInfo.gender as gender) as personalInfo, department as department, college as college from `playground-s-11-04b55e04.test_dataset.student_records`;

select rollNo as rollNo, STRUCT(personalInfo.first_name as first_name, personalInfo.last_name as last_name, personalInfo.age as age, STRUCT(personalInfo.bankDetails.balance as balance, STRUCT(personalInfo.bankDetails.account.accountNo as accountNo, personalInfo.bankDetails.account.ifsc as ifsc) as account) as bankDetails, personalInfo.gender as gender) as personalInfo, test_dataset.strcon(CAST(department AS STRING)) as department, college as college from `playground-s-11-04b55e04.test_dataset.student_records`;


CREATE TABLE `playground-s-11-04b55e04.test_dataset.student_records_encrypted`
AS
(SELECT
rollNo as rollNo,
STRUCT(
    personalInfo.first_name as first_name,
	personalInfo.last_name as last_name,
	personalInfo.age as age,
	STRUCT(
	      personalInfo.bankDetails.balance as balance,
		  STRUCT(
		        personalInfo.bankDetails.account.accountNo as accountNo
		        personalInfo.bankDetails.account.ifsc as ifsc
				) as account,
		  personalInfo.bankDetails.bankname as bankname
		  ) as bankDetails,
	personalInfo.gender as gender
	) as personalInfo,
test_dataset.strcon(CAST(department AS STRING)) AS department,
college as college
FROM
`playground-s-11-42147a98.test_dataset.student_records`);


SELECT
rollNo as rollNo,
STRUCT(
    personalInfo.first_name as first_name,
	personalInfo.last_name as last_name,
	personalInfo.age as age,
	STRUCT(
	      personalInfo.bankDetails.balance as balance,
		  STRUCT(
		        personalInfo.bankDetails.account.accountNo as accountNo
		        personalInfo.bankDetails.account.ifsc as ifsc
				) as account,
		  personalInfo.bankDetails.bankname as bankname
		  ) as bankDetails,
	personalInfo.gender as gender
	) as personalInfo,
department as department,
college as college
FROM
`playground-s-11-42147a98.test_dataset.student_records`;