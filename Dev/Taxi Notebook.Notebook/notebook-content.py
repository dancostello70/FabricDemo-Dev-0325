# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d847ff03-02e2-47f8-b79f-c0c0133e626e",
# META       "default_lakehouse_name": "DeploymentLabLakehouse",
# META       "default_lakehouse_workspace_id": "355a03d1-10be-498e-a24b-322504efd09e",
# META       "known_lakehouses": [
# META         {
# META           "id": "d847ff03-02e2-47f8-b79f-c0c0133e626e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM DeploymentLabLakehouse.green_tripdata_2022 LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.groupBy("VendorID").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
