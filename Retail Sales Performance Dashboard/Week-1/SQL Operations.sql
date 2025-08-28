CREATE DATABASE retail_store

USE retail_store

CREATE TABLE products(
	productID INT IDENTITY(1, 1) PRIMARY KEY,
	name VARCHAR(50),
	category VARCHAR(20),
	price DECIMAL(10, 2),
	cost DECIMAL(10, 2),
	discountPercentage DECIMAL(10, 2),
	createdAt DATETIME
)

CREATE TABLE stores(
	storeID INT IDENTITY(1, 1) PRIMARY KEY,
	name VARCHAR(50),
	region VARCHAR(200),
	address VARCHAR(200),
	createdAt DATETIME
)

CREATE TABLE employees(
	employeeID INT IDENTITY(1, 1) PRIMARY KEY,
	name VARCHAR(50),
	storeID INT,
	role VARCHAR(20),
	hireDate DATETIME,
	FOREIGN KEY (storeID) REFERENCES stores(storeID)
)

CREATE TABLE sales(
	saleID INT IDENTITY(1, 1) PRIMARY KEY,
	productID INT,
	storeID INT,
	employeeID INT,
	quantity INT,
	saleDate DATETIME,
	FOREIGN KEY (productID) REFERENCES products(productID),
	FOREIGN KEY (storeID) REFERENCES stores(storeID),
	FOREIGN KEY (employeeID) REFERENCES employees(employeeID)
)

CREATE INDEX product_name ON products(name)
CREATE INDEX sales_region ON stores(region)

--insert sample values 

INSERT INTO stores
	VALUES
	('Urban Mart - NY', 'East Coast', '101 Main St, New York, NY', GETDATE()),
	('SuperSave - LA', 'West Coast', '202 Ocean Ave, LA, CA', GETDATE()),
	('FreshStore - TX', 'South', '303 Sunset Blvd, Austin, TX', GETDATE()),
	('MegaMart - IL', 'Midwest', '404 Windy Rd, Chicago, IL', GETDATE()),
	('BudgetBazaar - FL', 'Southeast', '505 Palm Dr, Miami, FL', GETDATE())

INSERT INTO products
VALUES
	('Laptop Pro 14', 'Electronics', 1200.0, 900.0, 10.00, GETDATE()),
	('Organic Apples', 'Grocery', 3.5, 2.0, 5.00, GETDATE()),
	('Cotton T-Shirt', 'Apparel', 25.0, 10.0, 15.00, GETDATE()),
	('Bluetooth Speaker', 'Electronics', 60.0, 40.0, 20.00, GETDATE()),
	('LED Bulb Pack', 'Home Goods', 15.0, 8.0, 0.00, GETDATE())

INSERT INTO employees
VALUES
	('John Smith', 1, 'Cashier', GETDATE()),
	('Alice Johnson', 2, 'Manager', GETDATE()),
	('Bob Lee', 3, 'Sales Associate', GETDATE()),
	('Eva Martinez', 4, 'Supervisor', GETDATE()),
	('David Chen', 5, 'Stock Clerk', GETDATE())

INSERT INTO sales
VALUES
	(1, 1, 1, 2, GETDATE()),
	(2, 2, 2, 100, GETDATE()),
	(3, 3, 3, 30, GETDATE()),
	(4, 4, 4, 5, GETDATE()),
	(5, 5, 5, 20, GETDATE())

CREATE PROCEDURE dailySale
AS 
BEGIN
	SELECT s1.name, s2.saleDate, SUM(s2.quantity * p1.price) AS dailySales FROM stores s1
	INNER JOIN sales s2 ON s1.storeID = s2.storeID 
	INNER JOIN products p1 ON s2.productID = p1.productID
	GROUP BY s2.saleDate, s1.name
END

EXEC dailySale

SELECT * FROM products
SELECT * FROM sales
SELECT * FROM stores
SELECT * FROM employees