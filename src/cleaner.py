from databricks.connect import DatabricksSession
from pyspark.dbutils import DBUtils

spark = DatabricksSession.builder.getOrCreate()
dbutils = DBUtils(spark)

checkpoint_location = spark.sql("DESCRIBE EXTERNAL LOCATION checkpoints").select("url").collect()[0][0]

spark.sql("Drop schema if exists dev.bronze cascade")
spark.sql("Drop schema if exists dev.silver cascade")
spark.sql("Drop schema if exists dev.gold cascade")
dbutils.fs.rm("abfss://checkpoints@jassstreamkfproject.dfs.core.windows.net/checkpoints/bronze/", True)
dbutils.fs.rm("abfss://checkpoints@jassstreamkfproject.dfs.core.windows.net/checkpoints/silver/", True)
dbutils.fs.rm("abfss://checkpoints@jassstreamkfproject.dfs.core.windows.net/checkpoints/gold/", True)

