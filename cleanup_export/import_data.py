import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType

spark = SparkSession.builder.master("local[1]") \
    .appName("l3c_ctml") \
    .getOrCreate()


# get a list of all the synthetic data files
data_path = "synthetic_data"

schema = StructType([
  StructField('table', StringType(), True),
  StructField('field', StringType(), True),
  StructField('type', StringType(), True)
  ])

all_fields = spark.createDataFrame([],schema)

for path, subdirs, files in os.walk(data_path):
	for name in files:
		if "csv" in name and name[0]!=".":
			full_file = (os.path.join(path, name))
			print(full_file)
			# read and create pyspark tables from them
			print("reading")
			datafile=spark.read.csv(full_file,header=True)


			table_name = name.replace(".csv",".parquet")
			if("training" in full_file):
				table_name = "training_" + table_name
			if("testing" in full_file):
				table_name = "testing_" + table_name
			table_path = "tmp_data/" + table_name
			print(table_name)
			print("saving")
			datafile.write.mode('overwrite').parquet(table_path)
			print("storing fields")
			table_fields = [(table_name, f.name, str(f.dataType)) for f in datafile.schema.fields]
			table_field_df =spark.createDataFrame(table_fields, schema)
			all_fields = all_fields.union(table_field_df)

all_fields.toPandas().to_csv("scratch/provided_data_fields.csv", header = True, index = False)

			