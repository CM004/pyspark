# Transaction Data Analysis Project (Local PySpark Version)

This project demonstrates how to load, process, and analyze transaction data from multiple channels (web, mobile, in-store) using PySpark and Delta Lake, all on your local machine. No Azure or Databricks required!

## Project Structure

```
week7/
│
├── main.py
├── requirements.txt
├── README.md
├── data/
│   ├── transactions_web.csv
│   ├── transactions_mobile.csv
│   ├── transactions_instore.csv
│   └── products.csv
└── output/
```

## Setup Instructions

1. **Install Python 3.8+** (recommended: 3.8 or 3.9)
2. **Install Java 8 or 11** (required for Spark)
3. **Install dependencies:**

```bash
pip install -r requirements.txt
```

4. **Run the script:**

```bash
python main.py
```

5. **Check the output:**
- Results and Delta tables will be saved in the `output/` folder.
- Key insights will be printed in the terminal.

## What This Project Does
- Loads transaction data from CSVs in `data/`
- Joins with product info
- Calculates average order value per customer
- Identifies popular products and categories
- Analyzes marketing campaign impact
- Saves results as Delta tables
- Optimizes Delta tables
- Checks for missing values and outliers

## Troubleshooting
- If you see Java errors, make sure Java 8 or 11 is installed and `JAVA_HOME` is set.
- If you see PySpark errors, try running `pip install pyspark==3.4.1` again.
- If you see Delta Lake errors, try running `pip install delta-spark==2.4.0` again.
- For any other issues, check the error message and Google it, or ask for help!

## References
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Delta Lake Documentation](https://delta.io/) 