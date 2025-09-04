Vendor Relations Analytics
Overview

This project analyzes vendor sales, purchase patterns, and profitability to support data-driven decision-making in procurement and vendor management. It combines Exploratory Data Analysis (EDA) and performance evaluation to identify top-performing vendors, optimize pricing strategies, and highlight renegotiation opportunities.

Features

Exploratory Data Analysis (EDA) to clean and analyze vendor-related data.

Vendor performance analysis with key KPIs (profit margin, gross profit, sales quantity).

Visual insights for profit margin distributions and purchase-price trends.

Actionable business recommendations for strategic sourcing and vendor optimization.

Structured Jupyter Notebooks for reproducible data analysis.

Technologies Used

Python

pandas & NumPy – Data processing

Matplotlib & Seaborn – Visualization

SQLAlchemy / SQLite – Database connectivity

Jupyter Notebooks

Architecture

Data extracted from multiple tables:

purchases

purchase_prices

vendor_invoice

sales

vendor_sales_summary

Clean and transform data to ensure consistency.

Perform exploratory analysis for trend identification.

Compute vendor KPIs (Profit Margin, Sales Quantity, Gross Profit).

Provide insights and recommendations for business decisions.

Setup Instructions
1. Clone the repository
git clone https://github.com/nikhil2412000/vendor-relations-analytics.git
cd vendor-relations-analytics

2. Install dependencies
pip install -r requirements.txt

3. Run the analysis

Open Exploratory_Data_Analysis.ipynb for data preparation and exploration.

Open vendor_performance_analysis.ipynb for vendor performance analysis and KPI generation.

Outputs

Cleaned vendor sales dataset

Visual insights (profit margin distributions, sales-price relationships)

Vendor KPI tables for decision-making

Recommendations

Focus on high-volume, low-margin vendors for renegotiation.

Strengthen partnerships with high-margin vendors.

Conduct regular vendor performance reviews to maintain profitability.

License

This project is licensed under the MIT License – free for personal and commercial use.

MIT License – free for commercial and personal use.
