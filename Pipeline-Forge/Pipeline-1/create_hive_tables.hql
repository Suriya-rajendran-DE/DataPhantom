-- Create or replace raw table
CREATE OR REPLACE TABLE raw_emp_temp_table (
    id INT,
    employee_id INT,
    department_id INT,
    name STRING,
    age INT,
    gender STRING,
    salary DECIMAL(10,2),
    hire_date STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count'='1'); -- Skips header row if present

-- Load CSV data into raw table from HDFS
LOAD DATA INPATH 'hdfs://localhost:9000/datasets/emp.csv' INTO TABLE raw_emp_temp_table;


-- Create or replace stage table
CREATE OR REPLACE TABLE stage_emp_temp_table (
    sn_id INT,
    emp_id INT,
    dept_id INT,
    emp_name STRING,
    emp_age INT,
    emp_gender STRING,
    emp_salary DOUBLE,
    emp_hireData STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;




