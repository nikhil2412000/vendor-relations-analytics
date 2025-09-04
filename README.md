
**Vendor Sales & Performance Analysis**

**1. Overview**  
This project provides a comprehensive analysis of vendor sales, purchase patterns, and profitability. It empowers businesses to make data-driven decisions in vendor management, procurement, and strategic sourcing.  

The analysis combines:  
- **Exploratory Data Analysis (EDA)** for identifying patterns and anomalies.  
- **Vendor Performance Evaluation** to assess profitability and sales efficiency.  
- **Business Recommendations** to optimize vendor relationships and improve margins.  

**2. Executive Summary**  
- Top-performing vendors generate ~65% of total gross profit, maintaining an average margin above 25%.  
- Low-margin vendors (<10%) handle significant sales volumes, representing a key opportunity for renegotiation.  
- Sales quantities are highest in mid-tier purchase price ranges, while extreme prices lower profitability.  
- Optimization could increase gross profit by 8–12% annually.  

**3. Key Business Questions**  
This analysis answers:  
- Which vendors contribute the most to profit?  
- How does purchase pricing impact sales?  
- Who should be prioritized for long-term contracts?  
- Where are profit leaks due to inefficient vendor terms?  
- What pricing ranges maximize gross margins?  

**4. Data Pipeline**  

**4.1 Data Flow**  
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
Insights & Recommendations

**4.2 Data Sources**  
- purchases  
- purchase_prices  
- vendor_invoice  
- sales  
- vendor_sales_summary  

**5. Project Structure**  
- **Exploratory_Data_Analysis.ipynb** – Data cleaning, preparation, and initial analysis.  
- **vendor_performance_analysis.ipynb** – KPI calculation and vendor performance assessment.  

**6. Methodology**  

**6.1 Data Cleaning**  
- Removed missing or inconsistent values.  
- Converted critical columns (e.g., ProfitMargin) to numeric formats.  

**6.2 Analysis**  
- Performed statistical summaries and distributions.  
- Grouped data by vendor and product categories.  
- Filtered to include vendors with:  
  - Positive Gross Profit  
  - Positive Profit Margin  
  - Positive Sales Quantity  

**6.3 KPI Metrics**  
- Mean Profit Margin  
- Total Gross Profit  
- Total Sales Quantity  

**7. Insights & Findings**  
- **High-performing vendors**: Strong margins and stable sales.  
- **Underperforming vendors**: High volume but weak profit margins.  
- **Optimal pricing zone**: Mid-range purchase prices deliver higher returns.  

**8. Technology Stack**  
- **Programming Language**: Python 3.8+  
- **Libraries**:  
  - pandas, NumPy – Data processing and analysis  
  - Matplotlib, Seaborn – Visualization  
  - SQLAlchemy / SQLite – Database integration  

**9. Usage**  

**9.1 Clone Repository**  
```bash
git clone https://github.com/your-repo/vendor-sales-analysis.git
cd vendor-sales-analysis

pip install -r requirements.txt
