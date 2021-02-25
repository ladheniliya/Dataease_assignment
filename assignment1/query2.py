#!/usr/bin/python3
#2. What is the number of LEED projects in Virginia by owner type? 
from merge import virginiaData
Virginia_data = virginiaData()

q2 = Virginia_data.groupBy("OwnerTypes").count()
q2.show(truncate=False)

print("\nSchema")
print(q2.schema)
