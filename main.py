import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, isnan, when
from delta import configure_spark_with_delta_pip

# Paths
DATA_DIR = 'data'
OUTPUT_DIR = 'output'
TRANSACTION_FILES = [
    'transactions_web.csv',
    'transactions_mobile.csv',
    'transactions_instore.csv'
]
PRODUCTS_FILE = 'products.csv'

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Spark session with Delta support
builder = (
    SparkSession.builder.appName('TransactionAnalysis')
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Reusable function to load CSV
def load_csv(path):
    try:
        df = spark.read.option('header', 'true').csv(path)
        print(f"Loaded {path} with {df.count()} rows.")
        return df
    except Exception as e:
        print(f"Error loading {path}: {e}")
        return None

# Load all transactions
transaction_dfs = []
for fname in TRANSACTION_FILES:
    df = load_csv(os.path.join(DATA_DIR, fname))
    if df is not None:
        transaction_dfs.append(df)

if not transaction_dfs:
    print("No transaction data loaded. Exiting.")
    exit(1)

transactions_df = transaction_dfs[0]
for df in transaction_dfs[1:]:
    transactions_df = transactions_df.unionByName(df, allowMissingColumns=True)

# Load products
products_df = load_csv(os.path.join(DATA_DIR, PRODUCTS_FILE))
if products_df is None:
    print("No product data loaded. Exiting.")
    exit(1)

# Rename product price to avoid ambiguity
products_df = products_df.withColumnRenamed('price', 'product_price')

# Join transactions with products
joined_df = transactions_df.join(products_df, on='product_id', how='left')

# Convert transaction price to float
joined_df = joined_df.withColumn('price', col('price').cast('float'))
# Convert product price to float (if you want to use it in analysis)
joined_df = joined_df.withColumn('product_price', col('product_price').cast('float'))

# 1. Average order value per customer
avg_order_value = joined_df.groupBy('customer_id').agg(avg('price').alias('avg_order_value'))
avg_order_value.show(5)

# 2. Most popular products
popular_products = joined_df.groupBy('product_id', 'description').agg(count('*').alias('num_orders')).orderBy(col('num_orders').desc())
popular_products.show(5)

# 3. Most popular categories
popular_categories = joined_df.groupBy('category').agg(count('*').alias('num_orders')).orderBy(col('num_orders').desc())
popular_categories.show(5)

# 4. Campaign impact (if campaign_id exists)
if 'campaign_id' in joined_df.columns:
    campaign_impact = joined_df.groupBy('campaign_id').agg(count('*').alias('num_orders'), avg('price').alias('avg_order_value'))
    campaign_impact.show(5)
else:
    print("No campaign_id column found in transactions.")

# Save as Delta tables
avg_order_value.write.format('delta').mode('overwrite').save(os.path.join(OUTPUT_DIR, 'avg_order_value'))
popular_products.write.format('delta').mode('overwrite').save(os.path.join(OUTPUT_DIR, 'popular_products'))
popular_categories.write.format('delta').mode('overwrite').save(os.path.join(OUTPUT_DIR, 'popular_categories'))

# Optimize Delta tables (compacts small files)
try:
    spark.sql(f"OPTIMIZE delta.`{os.path.join(OUTPUT_DIR, 'avg_order_value')}`")
    spark.sql(f"OPTIMIZE delta.`{os.path.join(OUTPUT_DIR, 'popular_products')}`")
    spark.sql(f"OPTIMIZE delta.`{os.path.join(OUTPUT_DIR, 'popular_categories')}`")
    print("Delta tables optimized.")
except Exception as e:
    print(f"Delta optimization skipped or failed: {e}")

# Data quality: missing values
print("\nMissing values per column:")
missing = joined_df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in joined_df.columns])
missing.show()

# Data quality: outliers (price > 10000)
outliers = joined_df.filter(col('price') > 10000)
print(f"\nOutliers (price > 10000): {outliers.count()}")
outliers.show(5)

spark.stop() 