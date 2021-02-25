#!/usr/bin/python3
#3. What is the total Gross Square Feet of building space that is LEED-certified in Virginia?
from merge import virginiaData, mergedData
combined = mergedData()
combined.select("GrossSqFoot").dtypes

from pyspark.sql.types import IntegerType

Virginia_data = virginiaData()
q3 = Virginia_data.withColumn("GrossSqFoot", Virginia_data["GrossSqFoot"].cast(IntegerType())).groupBy("State").sum("GrossSqFoot")

q3.show()

print("\nSchema")
print(q3.schema)
