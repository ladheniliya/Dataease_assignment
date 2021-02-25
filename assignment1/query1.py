#!/usr/bin/python3
#1. How many LEED projects are there in Virginia (including all types of project types and versions of LEED)?
from merge import virginiaData
Virginia_data = virginiaData()

q1 = Virginia_data.select("ID","ProjectName","LEEDSystemVersionDisplayName","ProjectTypes","State")
q1.show(truncate=False)

print("\nSchema")
print(q1.schema)
