Drop DATABASE assignment;
CREATE DATABASE assignment;
USE assignment;

-- Create the Invoice table with a combined "Customer_Product" column
CREATE TABLE Invoice (
  InvoiceID INT,
  Customer_Product VARCHAR(255)
);

-- Insert sample data for Invoice
INSERT INTO Invoice (InvoiceID, Customer_Product) VALUES
(1111, '1 qq'),
(1112, 'ww'),
(1113, '10 pp'),
(1114, '2 lkl'),
(1115, '2 lkl'),
(1116, '3 asas'),
(1117, '9 wef'),
(1118, 'vxh');

-- Create Customer1 table
CREATE TABLE Customer1 (
  CustomerID INT PRIMARY KEY,
  CustomerName VARCHAR(255)
);

-- Insert data for Customer1
INSERT INTO Customer1 (CustomerID, CustomerName) VALUES
(1, 'aa'),
(2, 'bb'),
(3, 'cc'),
(5, 'ee');

-- Create Customer2 table
CREATE TABLE Customer2 (
  CustomerID INT PRIMARY KEY,
  CustomerName VARCHAR(255)
);

-- Insert data for Customer2 
INSERT INTO Customer2 (CustomerID, CustomerName) VALUES
(1, 'aa'),
(4, 'ff'),
(5, 'ee');

-- Create the table
CREATE TABLE NewTable AS
SELECT
  InvoiceID AS E,
  CASE 
    WHEN Customer_Product REGEXP '^[0-9]+' 
    THEN CAST(REGEXP_SUBSTR(Customer_Product, '^[0-9]+') AS UNSIGNED)
    ELSE NULL 
  END AS F,
  TRIM(REGEXP_REPLACE(Customer_Product, '^[0-9]+', '')) AS G
FROM Invoice;

SELECT * FROM NewTable;

-- Using LEFT JOIN to find unmatched customers
SELECT 
  C1.CustomerID,
  C1.CustomerName
FROM Customer1 C1
LEFT JOIN Customer2 C2 
  ON C1.CustomerID = C2.CustomerID
WHERE C2.CustomerID IS NULL;