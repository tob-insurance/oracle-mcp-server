-- Wait for database to be ready
WHENEVER SQLERROR EXIT SQL.SQLCODE;

-- Connect as SYSDBA to create test user
CONNECT sys/Welcome123 as sysdba

-- Create PDB if not exists and set it as current container
ALTER SESSION SET CONTAINER = FREEPDB1;

-- Create test user with more privileges for large-scale operations
CREATE USER testuser IDENTIFIED BY "testpass"
    DEFAULT TABLESPACE USERS
    TEMPORARY TABLESPACE TEMP;

-- Grant necessary privileges
GRANT CONNECT, RESOURCE TO testuser;
GRANT CREATE SESSION TO testuser;
GRANT CREATE TABLE TO testuser;
GRANT CREATE VIEW TO testuser;
GRANT CREATE SEQUENCE TO testuser;
GRANT UNLIMITED TABLESPACE TO testuser;
GRANT CREATE PROCEDURE TO testuser;
GRANT CREATE TRIGGER TO testuser;

-- Connect as test user
CONNECT testuser/testpass@//localhost:1521/FREEPDB1;

-- Create sequences for all primary keys
DECLARE
    v_sql VARCHAR2(1000);
BEGIN
    FOR i IN 1..1200 LOOP  -- Creating more sequences than needed
        v_sql := 'CREATE SEQUENCE seq_' || i || ' START WITH 1 INCREMENT BY 1 NOCACHE';
        EXECUTE IMMEDIATE v_sql;
    END LOOP;
END;
/

-- HR Domain (50 tables)
CREATE TABLE departments (
    dept_id NUMBER PRIMARY KEY,
    name VARCHAR2(100) NOT NULL,
    location VARCHAR2(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE employees (
    employee_id NUMBER PRIMARY KEY,
    dept_id NUMBER,
    first_name VARCHAR2(50) NOT NULL,
    last_name VARCHAR2(50) NOT NULL,
    email VARCHAR2(100) UNIQUE NOT NULL,
    hire_date DATE,
    salary NUMBER(10,2),
    CONSTRAINT fk_dept FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
);

-- Generate 48 more HR-related tables...
DECLARE
    v_sql VARCHAR2(4000);
BEGIN
    -- Generate HR attribute tables
    FOR i IN 1..48 LOOP
        v_sql := 'CREATE TABLE hr_attribute_' || i || ' (
            id NUMBER PRIMARY KEY,
            employee_id NUMBER,
            attribute_value VARCHAR2(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT fk_emp_' || i || ' FOREIGN KEY (employee_id) REFERENCES employees(employee_id)
        )';
        EXECUTE IMMEDIATE v_sql;
    END LOOP;
END;
/

-- Sales Domain (200 tables)
CREATE TABLE customers (
    customer_id NUMBER PRIMARY KEY,
    name VARCHAR2(100) NOT NULL,
    email VARCHAR2(100) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Generate 199 more sales-related tables...
DECLARE
    v_sql VARCHAR2(4000);
BEGIN
    FOR i IN 1..199 LOOP
        v_sql := 'CREATE TABLE sales_data_' || i || ' (
            id NUMBER PRIMARY KEY,
            customer_id NUMBER,
            transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            amount NUMBER(10,2),
            CONSTRAINT fk_cust_' || i || ' FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        )';
        EXECUTE IMMEDIATE v_sql;
    END LOOP;
END;
/

-- Inventory Domain (200 tables)
CREATE TABLE products (
    product_id NUMBER PRIMARY KEY,
    name VARCHAR2(100) NOT NULL,
    price NUMBER(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Generate 199 more inventory-related tables...
DECLARE
    v_sql VARCHAR2(4000);
BEGIN
    FOR i IN 1..199 LOOP
        v_sql := 'CREATE TABLE inventory_data_' || i || ' (
            id NUMBER PRIMARY KEY,
            product_id NUMBER,
            quantity NUMBER,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT fk_prod_' || i || ' FOREIGN KEY (product_id) REFERENCES products(product_id)
        )';
        EXECUTE IMMEDIATE v_sql;
    END LOOP;
END;
/

-- Finance Domain (200 tables)
CREATE TABLE accounts (
    account_id NUMBER PRIMARY KEY,
    account_number VARCHAR2(20) UNIQUE NOT NULL,
    balance NUMBER(15,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Generate 199 more finance-related tables...
DECLARE
    v_sql VARCHAR2(4000);
BEGIN
    FOR i IN 1..199 LOOP
        v_sql := 'CREATE TABLE finance_data_' || i || ' (
            id NUMBER PRIMARY KEY,
            account_id NUMBER,
            transaction_amount NUMBER(15,2),
            transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT fk_acc_' || i || ' FOREIGN KEY (account_id) REFERENCES accounts(account_id)
        )';
        EXECUTE IMMEDIATE v_sql;
    END LOOP;
END;
/

-- Operations Domain (200 tables)
CREATE TABLE facilities (
    facility_id NUMBER PRIMARY KEY,
    name VARCHAR2(100) NOT NULL,
    location VARCHAR2(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Generate 199 more operations-related tables...
DECLARE
    v_sql VARCHAR2(4000);
BEGIN
    FOR i IN 1..199 LOOP
        v_sql := 'CREATE TABLE operations_data_' || i || ' (
            id NUMBER PRIMARY KEY,
            facility_id NUMBER,
            operation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status VARCHAR2(20),
            CONSTRAINT fk_fac_' || i || ' FOREIGN KEY (facility_id) REFERENCES facilities(facility_id)
        )';
        EXECUTE IMMEDIATE v_sql;
    END LOOP;
END;
/

-- Customer Service Domain (150 tables)
CREATE TABLE tickets (
    ticket_id NUMBER PRIMARY KEY,
    customer_id NUMBER,
    status VARCHAR2(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_ticket_cust FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Generate 149 more customer service related tables...
DECLARE
    v_sql VARCHAR2(4000);
BEGIN
    FOR i IN 1..149 LOOP
        v_sql := 'CREATE TABLE service_data_' || i || ' (
            id NUMBER PRIMARY KEY,
            ticket_id NUMBER,
            resolution_time TIMESTAMP,
            satisfaction_score NUMBER(2),
            CONSTRAINT fk_tick_' || i || ' FOREIGN KEY (ticket_id) REFERENCES tickets(ticket_id)
        )';
        EXECUTE IMMEDIATE v_sql;
    END LOOP;
END;
/

-- Insert sample data into main tables
DECLARE
    v_sql VARCHAR2(4000);
BEGIN
    -- Insert departments
    INSERT INTO departments 
    SELECT LEVEL, 
           'Department ' || LEVEL, 
           'Location ' || LEVEL, 
           CURRENT_TIMESTAMP 
    FROM dual 
    CONNECT BY LEVEL <= 10;

    -- Insert employees
    INSERT INTO employees 
    SELECT LEVEL,
           MOD(LEVEL, 10) + 1,
           'FirstName' || LEVEL,
           'LastName' || LEVEL,
           'email' || LEVEL || '@example.com',
           SYSDATE - MOD(LEVEL, 365),
           50000 + MOD(LEVEL, 50000)
    FROM dual 
    CONNECT BY LEVEL <= 100;

    -- Insert customers
    INSERT INTO customers 
    SELECT LEVEL,
           'Customer ' || LEVEL,
           'customer' || LEVEL || '@example.com',
           CURRENT_TIMESTAMP
    FROM dual 
    CONNECT BY LEVEL <= 1000;

    -- Insert products
    INSERT INTO products 
    SELECT LEVEL,
           'Product ' || LEVEL,
           99.99 + MOD(LEVEL, 900),
           CURRENT_TIMESTAMP
    FROM dual 
    CONNECT BY LEVEL <= 1000;

    -- Insert accounts
    INSERT INTO accounts 
    SELECT LEVEL,
           'ACC' || LPAD(LEVEL, 10, '0'),
           10000 + MOD(LEVEL, 90000),
           CURRENT_TIMESTAMP
    FROM dual 
    CONNECT BY LEVEL <= 1000;

    -- Insert facilities
    INSERT INTO facilities 
    SELECT LEVEL,
           'Facility ' || LEVEL,
           'Location ' || LEVEL,
           CURRENT_TIMESTAMP
    FROM dual 
    CONNECT BY LEVEL <= 100;

    -- Insert tickets
    INSERT INTO tickets 
    SELECT LEVEL,
           MOD(LEVEL, 1000) + 1,
           CASE MOD(LEVEL, 3) 
               WHEN 0 THEN 'OPEN'
               WHEN 1 THEN 'IN_PROGRESS'
               ELSE 'CLOSED'
           END,
           CURRENT_TIMESTAMP
    FROM dual 
    CONNECT BY LEVEL <= 1000;

    -- Insert sample data into generated tables
    FOR i IN 1..48 LOOP
        v_sql := 'INSERT INTO hr_attribute_' || i || ' 
            SELECT seq_' || i || '.NEXTVAL, 
                   MOD(LEVEL, 100) + 1, 
                   ''Value '' || LEVEL, 
                   CURRENT_TIMESTAMP 
            FROM dual 
            CONNECT BY LEVEL <= 100';
        EXECUTE IMMEDIATE v_sql;
    END LOOP;

    COMMIT;
END;
/

-- Populate all generated tables with realistic data
DECLARE
    v_sql VARCHAR2(4000);
    v_random_amount NUMBER;
    v_random_date DATE;
BEGIN
    -- Populate HR attribute tables with realistic employee attributes
    FOR i IN 1..48 LOOP
        v_sql := 'INSERT INTO hr_attribute_' || i || ' 
            SELECT 
                seq_' || i || '.NEXTVAL, 
                e.employee_id,
                CASE MOD(lvl.col, 5) 
                    WHEN 0 THEN ''Experience Level '' || TRUNC(DBMS_RANDOM.VALUE(1, 10))
                    WHEN 1 THEN ''Certification '' || TRUNC(DBMS_RANDOM.VALUE(1, 5))
                    WHEN 2 THEN ''Skill Rating '' || TRUNC(DBMS_RANDOM.VALUE(1, 100))
                    WHEN 3 THEN ''Training Score '' || TRUNC(DBMS_RANDOM.VALUE(60, 100))
                    ELSE ''Performance Index '' || TRUNC(DBMS_RANDOM.VALUE(1, 5))
                END,
                SYSTIMESTAMP - NUMTODSINTERVAL(DBMS_RANDOM.VALUE(1, 365), ''DAY'')
            FROM employees e,
                 (SELECT LEVEL as col FROM dual CONNECT BY LEVEL <= 10) lvl';
        EXECUTE IMMEDIATE v_sql;
    END LOOP;

    -- Populate Sales tables with transaction data
    FOR i IN 1..199 LOOP
        v_sql := 'INSERT INTO sales_data_' || i || '
            SELECT 
                seq_' || (i+100) || '.NEXTVAL,
                c.customer_id,
                SYSTIMESTAMP - NUMTODSINTERVAL(DBMS_RANDOM.VALUE(1, 730), ''DAY''),
                ROUND(DBMS_RANDOM.VALUE(10, 5000), 2)
            FROM customers c,
                 (SELECT LEVEL as col FROM dual CONNECT BY LEVEL <= 50) l';
        EXECUTE IMMEDIATE v_sql;
    END LOOP;

    -- Populate Inventory tables with stock data
    FOR i IN 1..199 LOOP
        v_sql := 'INSERT INTO inventory_data_' || i || '
            SELECT 
                seq_' || (i+300) || '.NEXTVAL,
                p.product_id,
                TRUNC(DBMS_RANDOM.VALUE(0, 1000)),
                SYSTIMESTAMP - NUMTODSINTERVAL(DBMS_RANDOM.VALUE(1, 90), ''DAY'')
            FROM products p,
                 (SELECT LEVEL as col FROM dual CONNECT BY LEVEL <= 20) l';
        EXECUTE IMMEDIATE v_sql;
    END LOOP;

    -- Populate Finance tables with transaction data
    FOR i IN 1..199 LOOP
        v_sql := 'INSERT INTO finance_data_' || i || '
            SELECT 
                seq_' || (i+500) || '.NEXTVAL,
                a.account_id,
                CASE WHEN DBMS_RANDOM.VALUE(0, 1) < 0.5 
                    THEN ROUND(-DBMS_RANDOM.VALUE(100, 10000), 2)
                    ELSE ROUND(DBMS_RANDOM.VALUE(100, 10000), 2)
                END,
                SYSTIMESTAMP - NUMTODSINTERVAL(DBMS_RANDOM.VALUE(1, 365), ''DAY'')
            FROM accounts a,
                 (SELECT LEVEL as col FROM dual CONNECT BY LEVEL <= 30) l';
        EXECUTE IMMEDIATE v_sql;
    END LOOP;

    -- Populate Operations tables with facility operations data
    FOR i IN 1..199 LOOP
        v_sql := 'INSERT INTO operations_data_' || i || '
            SELECT 
                seq_' || (i+700) || '.NEXTVAL,
                f.facility_id,
                SYSTIMESTAMP - NUMTODSINTERVAL(DBMS_RANDOM.VALUE(1, 180), ''DAY''),
                CASE TRUNC(DBMS_RANDOM.VALUE(1, 4))
                    WHEN 1 THEN ''OPERATIONAL''
                    WHEN 2 THEN ''MAINTENANCE''
                    WHEN 3 THEN ''SHUTDOWN''
                    ELSE ''STARTUP''
                END
            FROM facilities f,
                 (SELECT LEVEL as col FROM dual CONNECT BY LEVEL <= 40) l';
        EXECUTE IMMEDIATE v_sql;
    END LOOP;

    -- Populate Customer Service tables with service interaction data
    FOR i IN 1..149 LOOP
        v_sql := 'INSERT INTO service_data_' || i || '
            SELECT 
                seq_' || (i+900) || '.NEXTVAL,
                t.ticket_id,
                t.created_at + NUMTODSINTERVAL(DBMS_RANDOM.VALUE(1, 72), ''HOUR''),
                TRUNC(DBMS_RANDOM.VALUE(1, 11))
            FROM tickets t,
                 (SELECT LEVEL as col FROM dual CONNECT BY LEVEL <= 5) l
            WHERE t.status = ''CLOSED''';
        EXECUTE IMMEDIATE v_sql;
    END LOOP;

    COMMIT;
END;
/

-- Create indexes for better performance
DECLARE
    v_sql VARCHAR2(4000);
BEGIN
    -- Create indexes for HR tables
    FOR i IN 1..48 LOOP
        EXECUTE IMMEDIATE 'CREATE INDEX idx_hr_' || i || '_emp ON hr_attribute_' || i || '(employee_id)';
    END LOOP;

    -- Create indexes for Sales tables
    FOR i IN 1..199 LOOP
        EXECUTE IMMEDIATE 'CREATE INDEX idx_sales_' || i || '_cust ON sales_data_' || i || '(customer_id)';
    END LOOP;

    -- Create indexes for Inventory tables
    FOR i IN 1..199 LOOP
        EXECUTE IMMEDIATE 'CREATE INDEX idx_inv_' || i || '_prod ON inventory_data_' || i || '(product_id)';
    END LOOP;

    -- Create indexes for Finance tables
    FOR i IN 1..199 LOOP
        EXECUTE IMMEDIATE 'CREATE INDEX idx_fin_' || i || '_acc ON finance_data_' || i || '(account_id)';
    END LOOP;

    -- Create indexes for Operations tables
    FOR i IN 1..199 LOOP
        EXECUTE IMMEDIATE 'CREATE INDEX idx_ops_' || i || '_fac ON operations_data_' || i || '(facility_id)';
    END LOOP;

    -- Create indexes for Customer Service tables
    FOR i IN 1..149 LOOP
        EXECUTE IMMEDIATE 'CREATE INDEX idx_serv_' || i || '_tick ON service_data_' || i || '(ticket_id)';
    END LOOP;
END;
/

-- Analyze tables for better query performance
BEGIN
    FOR tab IN (SELECT table_name FROM user_tables) LOOP
        EXECUTE IMMEDIATE 'ANALYZE TABLE ' || tab.table_name || ' COMPUTE STATISTICS';
    END LOOP;
END;
/

COMMIT;