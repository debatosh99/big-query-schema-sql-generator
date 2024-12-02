rollNo as rollNo
personalInfo.first_name as first_name
personalInfo.last_name as last_name
personalInfo.age as age
personalInfo.bankDetails.balance as balance
personalInfo.bankDetails.accountNo as accountNo
department as department



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
`playground-s-11-42147a98.test_dataset.student_records`;




-----------------------------------------------------------
I have the below elements in a list:  (bq-schema3.json)

'rollNo as rollNo'
'personalInfo.first_name as first_name'
'personalInfo.last_name as last_name'
'personalInfo.age as age'
'personalInfo.bankDetails.balance as balance'
'personalInfo.bankDetails.accountNo as accountNo'
'department as department'

Write a python program which accepts this list as parameter and generates a generic sql statement as per the below format, by reading from the list passed as parameter:

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
`playground-s-11-42147a98.test_dataset.student_records`;

-------------------------------------------------------------


-----------------------------------------------------------
I have the below elements in a list: (bq-schema4.json)

'rollNo as rollNo'
'personalInfo.first_name as first_name'
'personalInfo.last_name as last_name'
'personalInfo.age as age'
'personalInfo.bankDetails.balance as balance'
'personalInfo.bankDetails.accountNo as accountNo'
'personalInfo.gender as gender'
'department as department'
'college as college'

Write a python program which accepts this list as parameter and generates a generic sql statement as per the below format, by reading from the list passed as parameter:

SELECT
rollNo as rollNo,
STRUCT(
    personalInfo.first_name as first_name,
	personalInfo.last_name as last_name,
	personalInfo.age as age,
	STRUCT(
	      personalInfo.bankDetails.balance as balance,
		  personalInfo.bankDetails.accountNo as accountNo
		  ) as bankDetails,
	personalInfo.gender as gender
	) as personalInfo,
department as department,
college as college
FROM
`playground-s-11-42147a98.test_dataset.student_records`;

-----------------------------------------------------------
I have the below elements in a list: (bq-schema5.json)

'rollNo as rollNo'
'personalInfo.first_name as first_name'
'personalInfo.last_name as last_name'
'personalInfo.age as age'
'personalInfo.bankDetails.balance as balance'
'personalInfo.bankDetails.account.accountNo as accountNo'
'personalInfo.bankDetails.account.ifsc as ifsc'
'personalInfo.bankDetails.bankname as bankname'
'personalInfo.gender as gender'
'department as department'
'college as college'

Write a python program which accepts this list as parameter and generates a generic sql statement as per the below format, by reading from the list passed as parameter:

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


