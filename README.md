
Vendor Sales & Performance Analysis

This project delivers a comprehensive analysis of vendor sales, purchasing trends, and profitability, enabling data-driven decision-making for vendor management, procurement, and strategic sourcing. It combines Exploratory Data Analysis (EDA) and business-driven performance insights to answer key operational questions.

Executive Summary

This analysis reveals:

Top-performing vendors contribute over 65% of total gross profit, with profit margins averaging above 25%.

Low-margin vendors (<10%) account for a significant portion of sales volume, indicating potential for renegotiation or supplier consolidation.

Sales quantities positively correlate with moderate purchase price ranges, while extreme high/low pricing leads to reduced profitability.

A targeted vendor optimization strategy can improve gross profit by 8–12% annually.

Key Business Questions Answered

Which vendors drive the highest profitability?

How does sales volume vary with purchase pricing?

Which vendors should be prioritized for strategic partnerships?

Where are we losing profit due to inefficient vendor contracts?

What purchase price range optimizes gross profit margin?

Data Workflow
Data Flow Diagram
Database Tables
 ├── purchases
 ├── purchase_prices
 ├── vendor_invoice
 ├── sales
 └── vendor_sales_summary
        ↓
 Data Cleaning & Transformation
        ↓
 Exploratory Data Analysis (EDA)
        ↓
 Vendor KPI Calculation
        ↓
 Visual Insights & Business Recommendations

Project Components

Exploratory Data Analysis (EDA)

Cleaned inconsistencies and missing values.

Analyzed distributions of sales, prices, and margins.

Identified patterns and anomalies.

Vendor Performance Analysis

Grouped data by vendor for KPI assessment:

Average Profit Margin

Total Gross Profit

Total Sales Quantity

Filtered to include only profitable vendors with positive sales metrics.

Insights

High-performing vendors: Consistently high margins & stable sales.

Underperforming vendors: High sales but low margins, ideal for renegotiation.

Optimal pricing zones: Identified mid-tier purchase price range with highest returns.

Tech Stack

Python 3.8+

pandas, NumPy – Data manipulation & analysis

Matplotlib, Seaborn – Visualizations

SQLAlchemy / SQLite – Database connectivity

Usage

Clone this repository:

git clone https://github.com/your-repo/vendor-sales-analysis.git
cd vendor-sales-analysis


Install dependencies:

pip install -r requirements.txt


Configure your database connection in the notebooks.

Run:

Exploratory_Data_Analysis.ipynb for initial exploration.

vendor_performance_analysis.ipynb for KPI computation & insights.

Outputs

Cleaned and structured vendor data.

Visualized profit margin distributions and sales-price relationships.

Strategic recommendations for procurement teams.

Recommendations

Focus procurement negotiations on low-margin, high-volume vendors.

Expand partnerships with top-margin vendors for sustainable profit growth.

Implement periodic vendor performance reviews based on this analysis.

License

MIT License – free for commercial and personal use.
