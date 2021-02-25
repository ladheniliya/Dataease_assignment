#!/usr/bin/python3
#4. What Zip Code in Virginia has the highest number of projects? 
from merge import virginiaData
import pyspark.sql.functions as f
Virginia_data  = virginiaData()
q4 = Virginia_data.groupBy("Zipcode").agg(f.count("ProjectName").alias("Count"))

q4 = q4.sort("Count", ascending = False)
q4.show()
print("\nSchema")
print(q4.schema)
