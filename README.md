# dynamic-insert-query
# Purpose : Import table data to Postgres , which is first exported from Oracle Database .
Hi, This is a simple java program which export all rows of a given table from Oracle datbase .
The exported rows will be in form of sql insert query .
Those rows then can be inserted in Postgres database .
Generate sql insert statement dynamically using jdbc and java programming . Used postgres database .
- For example :
insert into HR.EMPLOYEES(EMPLOYEE_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER, HIRE_DATE, JOB_ID, SALARY, COMMISSION_PCT, MANAGER_ID, DEPARTMENT_ID) values (100, 'Steven', 'King', 'SKING', '515.123.4567', to_timestamp('17/06/2003 00:00:00', 'dd/mm/yyyy hh24:mi:ss'), 'AD_PRES', 24000, , , 90);
insert into HR.EMPLOYEES(EMPLOYEE_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER, HIRE_DATE, JOB_ID, SALARY, COMMISSION_PCT, MANAGER_ID, DEPARTMENT_ID) values (101, 'Neena', 'Kochhar', 'NKOCHHAR', '515.123.4568', to_timestamp('21/09/2005 00:00:00', 'dd/mm/yyyy hh24:mi:ss'), 'AD_VP', 17000, , 100, 90);

- View the generated data in HR.EMPLOYEES.sql .
