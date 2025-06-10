-- Create or replace raw table
CREATE OR REPLACE TABLE raw_emp_temp_table (
    employee_id INT,
    department_id INT,
    name STRING,
    age INT,
    gender STRING,
    salary DECIMAL(10,2),
    hire_date DATE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Load CSV data into raw table from HDFS
LOAD DATA INPATH 'hdfs://localhost:9000/datasets/emp.csv' INTO TABLE raw_emp_temp_table;


-- Create or replace stage table
CREATE OR REPLACE TABLE stage_emp_temp_table (
    employee_id INT,
    department_id INT,
    name STRING,
    age INT,
    gender STRING,
    normalized_salary DOUBLE,
    hire_year INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;




