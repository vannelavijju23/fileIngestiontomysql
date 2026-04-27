from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.appName("Python").master("local[2]").getOrCreate()

# -----------------------------
# 1. Read CSV from shared path
# -----------------------------
csv_path=r"C:\bigdata\drivers\bank-full.csv"
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("sep",";").load(csv_path)
df.show()

mysql_url = "jdbc:mysql://database-1.c180weuim2zq.us-east-2.rds.amazonaws.com:3306/test_mysql"
df.write.format("jdbc").option("url", mysql_url).option("user", "admin").option("password", "yourpassword").option("dbtable", "bank_full").mode("append").save()

print("Data written to MySQL successfully!")
