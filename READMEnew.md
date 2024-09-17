# README.md

# Enhanced EMR Job Resource Recommender and Spark Code Generator

This application provides a comprehensive interface for recommending EMR job resources and generating Spark code based on user-defined parameters. It combines resource recommendation with Spark code generation, offering a streamlined workflow for data engineers and analysts.

## Features

1. EMR Job Resource Recommendation
2. Spark Code Generation
3. Parameter Management (Save/Load)
4. Dynamic Workload Type Detection

## Getting Started

### Prerequisites

- Python 3.7+
- Gradio
- NumPy

### Installation

1. Clone the repository:
   ```
   git clone https://github.com/your-repo/emr-job-recommender.git
   cd emr-job-recommender
   ```

2. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

### Running the Application

Run the following command in the project directory:
   ```
    python visial_code_gen.py
   ```

## Example Use Case: Creating a Fact Table in a Data Warehouse
### Goal:
This example demonstrates how to use the Spark Code Generator to create a Fact_Sales table in the Sales_DW schema by joining multiple dimensional tables like Orders, Product, Customer, SalesRep, and Date.

SQL Example:
   ```
INSERT INTO Sales_DW.Fact_Sales (
    FK_Order_Id,
    FK_Product_Id,
    FK_Customer_Id,
    FK_SalesRep_Id,
    FK_Date_Key,
    Quantity_Sold,
    Sales_Amount,
    Discount_Amount,
    Net_Sales_Amount,
    Profit_Amount,
    Tax_Amount,
    Total_Amount
)
SELECT
    o.Order_Id,
    p.Product_Id,
    c.Customer_Id,
    r.SalesRep_Id,
    d.Date_Key,
    s.Quantity,
    s.Quantity * p.Product_List_Price AS Sales_Amount,
    p.Product_Discount * p.Product_List_Price AS Discount_Amount,
    (s.Quantity * p.Product_List_Price) - (p.Product_Discount * p.Product_List_Price) AS Net_Sales_Amount,
    ((s.Quantity * p.Product_List_Price) - (p.Product_Discount * p.Product_List_Price)) - (s.Quantity * p.Product_Cost) AS Profit_Amount,
    CASE WHEN o.Tax_Applicable = 'Yes' THEN (Net_Sales_Amount * t.Tax_Rate) ELSE 0 END AS Tax_Amount,
    (Net_Sales_Amount + Tax_Amount) AS Total_Amount
FROM
    Prepared.Sales s
JOIN Sales_DW.DimOrders o ON s.Order_Id = o.Order_Id
JOIN Sales_DW.DimProduct p ON s.Product_Id = p.Product_Id
JOIN Sales_DW.DimCustomer c ON o.Customer_Id = c.Customer_Id
JOIN Sales_DW.DimSalesRep r ON s.SalesRep_Id = r.SalesRep_Id
JOIN Sales_DW.DimDate d ON o.Order_Date = d.Full_Date;
   ```
