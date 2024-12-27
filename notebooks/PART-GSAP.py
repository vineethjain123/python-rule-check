# Databricks notebook source
# DBTITLE 1,Load Part Master
from pyspark.sql.functions import to_date
from pyspark.sql.functions import *
final_df = spark.read.format('parquet').options(header='false').load("abfss://shell31eunadls2tplfcblyo@shell31eunadls2tplfcblyo.dfs.core.windows.net/PROJECT/P00002-ENRICHED/Unharmonised/Business_Terms/3rd_Party/ORCHESTR8/NonSensitive/O8_Master_Data/O8_Master_Data.parquet")
print(final_df.count())
final_df = final_df.withColumn('EL_MATERIAL_NO', lpad(final_df['EL_MATERIAL_NO'], 18, '0'))
final_df = final_df.withColumn("O8_FILE_SENT_DATE",to_date(final_df.O8_FILE_SENT_DATE, 'dd/MM/yyyy'))
max_date=final_df.select(max("O8_FILE_SENT_DATE")).first()
print(max_date[0])
final_df=final_df.filter(final_df.O8_FILE_SENT_DATE==max_date[0])
print(final_df.count())

# COMMAND ----------

from pyspark.sql.functions import count
print(final_df.select("O8_PART_NO").distinct().count())

# COMMAND ----------

# DBTITLE 1,PLANNING_STRATEGY_DESC
final_df=final_df.withColumn('PLANNING_STRATEGY_DESC',when(final_df.PLANNING_STRATEGY_GROUP_CODE.isin('Z1','Z2','Z3','Z4'),'MTO').otherwise('MTS'))
print(final_df.count())
final_df=final_df.withColumn('O8_DUAL_PART_IND',when(upper(final_df.O8_PART_NO).contains("DUAL"),'Y'))

# COMMAND ----------

# DBTITLE 1,Load T001W/T001K/T001
df_T001W=spark.read.format('parquet').options(header='false').load("abfss://shell31eunadls2tplfcblyo@shell31eunadls2tplfcblyo.dfs.core.windows.net/PROJECT/P00002-ENRICHED/Unharmonised/NonSensitive/1st_Party/GSAP_DS_HANA/PLANT_COMPCODE_MASTER/PLANT_COMPCODE_MASTER_*.parquet")
df_T001W=df_T001W.select('PLANT_CODE','SALES_ORG_CODE','PLANT_LOCAL_CURRENCY')
#display(df_T001W)

# COMMAND ----------

# DBTITLE 1,SALES_ORG_CODE FOR O8 PLANT
df_T001W3=df_T001W.select('PLANT_CODE','SALES_ORG_CODE')
final_df = final_df.join(df_T001W3, final_df.PLANT_CODE == df_T001W3.PLANT_CODE,how='left').drop(df_T001W3.PLANT_CODE)
print(final_df.count())
final_df=final_df.withColumnRenamed("SALES_ORG_CODE","SALES_ORG_CODE_PLANT")
print(final_df.count())

# COMMAND ----------

# DBTITLE 1,SALES_ORG_CODE FOR O8_SUPPLIER_CODE
df_T001W2=df_T001W.select('PLANT_CODE','SALES_ORG_CODE')
df_T001W2=df_T001W2.withColumnRenamed("PLANT_CODE","PLANT_CODE1")
final_df = final_df.join(df_T001W2, final_df.O8_SUPPLIER_CODE == df_T001W2.PLANT_CODE1,how='left').drop(df_T001W2.PLANT_CODE1)
print(final_df.count())
final_df=final_df.withColumnRenamed("SALES_ORG_CODE","SALES_ORG_CODE_SUPPLIER")
display(final_df)

# COMMAND ----------

# DBTITLE 1,GSAP DS1_MM_MT_LEAD
df_mtlead=spark.read.format('parquet').options(header='false').load("abfss://shell31eunadls2tplfcblyo@shell31eunadls2tplfcblyo.dfs.core.windows.net/PROJECT/P00002-ENRICHED/Unharmonised/NonSensitive/1st_Party/GSAP_DS_HANA/CA_LEAD_TIME_PUR_REP_GRA_002/CA_LEAD_TIME_PUR_REP_GRA_002_*.parquet")
df_mtlead=df_mtlead.select('MATERIAL_NO','PLANT_CODE','VENDOR_NO')
df_mtlead=df_mtlead.withColumn('PART_NO_LEAD', concat(col("MATERIAL_NO"),lit(' '),col('PLANT_CODE'),lit(' '),col('VENDOR_NO')))
df_mtlead=df_mtlead.select('PART_NO_LEAD','VENDOR_NO')
#display(df_mtlead)
final_df=final_df.withColumn('PART_NO_O8', concat(col('O8_PART_NO'),lit(' '),col('O8_SUPPLIER_CODE')))
final_df = final_df.join(df_mtlead, final_df.PART_NO_O8 == df_mtlead.PART_NO_LEAD,how='left').drop(df_mtlead.PART_NO_LEAD)
final_df=final_df.dropDuplicates(['O8_PART_NO'])
print(final_df.count())
#df_mtlead=df_mtlead


# COMMAND ----------

# DBTITLE 1,GSAP QMAT
df_qmat=spark.read.format('parquet').options(header='false').load("abfss://shell31eunadls2tplfcblyo@shell31eunadls2tplfcblyo.dfs.core.windows.net/PROJECT/P00002-ENRICHED/Unharmonised/NonSensitive/1st_Party/GSAP_DS_HANA/CA_INSPTYPE_MATERIAL_REP_GRA_002/CA_INSPTYPE_MATERIAL_REP_GRA_002_*.parquet")
df_qmat=df_qmat.select('MATERIAL_NO','PLANT_CODE','INSPECTION_TYPE_CODE','STOCK_POSTED_IND')
df_qmat=df_qmat.withColumn('PART_NO_QMAT', concat(col('MATERIAL_NO'),lit(' '),col('PLANT_CODE')))
df_qmat=df_qmat.select('PART_NO_QMAT','STOCK_POSTED_IND','INSPECTION_TYPE_CODE')
df_qmat=df_qmat.filter(df_qmat.INSPECTION_TYPE_CODE=='01')
final_df = final_df.join(df_qmat, final_df.O8_PART_NO == df_qmat.PART_NO_QMAT,how='left')
print(final_df.count())
#display(df_qmat)

# COMMAND ----------

# DBTITLE 1,PROCUREMENT_TYPE_DESC
final_df=final_df.withColumn('PROCUREMENT_TYPE_DESC',when((final_df.O8_SUPPLIER_CODE==final_df.PLANT_CODE),'Manufactured'))

final_df=final_df.withColumn('PROCUREMENT_TYPE_DESC',when((final_df.O8_SUPPLIER_CODE!=final_df.PLANT_CODE) & (length(final_df.O8_SUPPLIER_CODE)==4) & (final_df.O8_SUPPLIER_CODE!=final_df.SALES_ORG_CODE_PLANT) & (final_df.SALES_ORG_CODE_PLANT==final_df.SALES_ORG_CODE_SUPPLIER),'Intra-Company Networked').otherwise(final_df['PROCUREMENT_TYPE_DESC']))

final_df=final_df.withColumn('PROCUREMENT_TYPE_DESC',when((final_df.O8_SUPPLIER_CODE!=final_df.PLANT_CODE) & (length(final_df.O8_SUPPLIER_CODE)==4) & (final_df.O8_SUPPLIER_CODE!=final_df.SALES_ORG_CODE_PLANT) & (final_df.SALES_ORG_CODE_PLANT!=final_df.SALES_ORG_CODE_SUPPLIER),'Inter-Company Networked').otherwise(final_df['PROCUREMENT_TYPE_DESC']))

final_df=final_df.withColumn('PROCUREMENT_TYPE_DESC',when((final_df.PROCUREMENT_TYPE_DESC.isNull()) & (final_df.MATERIAL_TYPE_CODE=='YBSE'),'Externally Purchased - Delivered').otherwise(final_df['PROCUREMENT_TYPE_DESC']))

final_df=final_df.withColumn('PROCUREMENT_TYPE_DESC',when((final_df.PROCUREMENT_TYPE_DESC.isNull()) & (final_df.VENDOR_NO.isNull()),'Externally Purchased - Delivered').otherwise(final_df['PROCUREMENT_TYPE_DESC']))

final_df=final_df.withColumn('PROCUREMENT_TYPE_DESC',when((final_df.PROCUREMENT_TYPE_DESC.isNull()) & (final_df.STOCK_POSTED_IND=='X') & (final_df.INSPECTION_TYPE_CODE=='01'),'Externally Purchased - Collected into Quality Inspection stock').otherwise(final_df['PROCUREMENT_TYPE_DESC']))

final_df=final_df.withColumn('PROCUREMENT_TYPE_DESC',when((final_df.PROCUREMENT_TYPE_DESC.isNull()) & (final_df.STOCK_POSTED_IND!='X') & (final_df.INSPECTION_TYPE_CODE!='01'),'Externally Purchased - Collected into available stock').otherwise(final_df['PROCUREMENT_TYPE_DESC']))

final_df=final_df.withColumn('PROCUREMENT_TYPE_DESC',when((final_df.PROCUREMENT_TYPE_DESC.isNull()) & (final_df.STOCK_POSTED_IND.isNull()) & (final_df.INSPECTION_TYPE_CODE.isNull()),'Externally Purchased - Collected into available stock').otherwise(final_df['PROCUREMENT_TYPE_DESC']))
print(final_df.count())

# COMMAND ----------

# DBTITLE 1,Final Dataset
final_df=final_df.select('O8_PART_NO','MATERIAL_DESC', 'O8_SUPPLIER_CODE', 'O8_PROCUREMENT_TYPE_DESC', 'O8_REPLENISHMENT_LEAD_TIME', 'MAX_LOT_SIZE', 'O8_MIN_LOT_SIZE', 'O8_ORDER_QTY_IN_RUOM_ROUNDING_VALUE',    'O8_PHASE_IN_DATE', 'O8_PHASE_OUT_DATE', 'O8_PART_TYPE_CODE', 'STANDARD_PRICE', 'O8_ACTIVE_PART', 'MATERIAL_TYPE_CODE', 'MRP_CONTROLLER_CODE', 'O8_NO_OF_DAYS_STRATEGIC_BUFFER', 'PALLET_QTY','O8_PART_STATUS','PRODUCT_HIERARCHY_SKU_CODE', 'MATERIAL_GROUP_CODE', 'PLANNING_STRATEGY_GROUP_CODE', 'O8_SOURCE_LOCATION', 'O8_LEAD_TIME_OFFSET', 'O8_MANUFACTURING_LEAD_TIME', 'O8_RUOM_CONVERSION_FACTOR','O8_REPLENISHMENT_UOM','O8_OMAN_CURRENCY_NO_OF_DECIMAL_PLACES', 'O8_US_LOCATION_TYPE', 'PROCUREMENT_TYPE_CODE', 'PLANT_CODE', 'O8_GROSS_WEIGHT', 'O8_MATERIAL_VOL','O8_DUAL_PART_NO','O8_DECOUPLED_LEAD_TIME','O8_BASE_PART_NO','O8_STACKABLE_PRODUCT_IND','O8_HAZARDOUS_PART_IND','O8_HAUILER_LEAD_TIME','O8_HAUILER_LEAD_TIME_ADJUSTMENT_DAYS','O8_SUPPLIER_NAME', 'O8_TOTAL_BUFFER_QTY_IN_RUOM','O8_TOTAL_BUFFER_VALUE', 'O8_ON_HAND_QTY_IN_RUOM', 'O8_ON_HAND_VALUE', 'O8_OPEN_SHOP_ORDER_QTY_IN_RUOM', 'O8_OPEN_SHOP_ORDER_VALUE', 'O8_OPEN_PURCHASE_ORDER_QTY_IN_RUOM', 'O8_OPEN_PURCHASE_ORDER_VALUE', 'O8_OPEN_CUSTOMER_ORDER_QTY_IN_RUOM', 'O8_OPEN_CUSTOMER_ORDER_VALUE', 'O8_OPEN_PLANNED_DEMAND_QTY_IN_RUOM','O8_OPEN_PLANNED_DEMAND_VALUE','O8_NO_OF_BUFFER_DAYS','O8_PLANNING_RULE', 'O8_REPLACEMENT_PART', 'O8_AVG_DAILY_USAGE', 'O8_FILE_SENT_DATE', 'EL_MATERIAL_NO','PLANNING_STRATEGY_DESC','PROCUREMENT_TYPE_DESC','O8_DUAL_PART_IND')

final_df=final_df.withColumnRenamed("EL_MATERIAL_NO","MATERIAL_NO")
print(final_df.count())

# COMMAND ----------

final_df = final_df.withColumn('O8_PART_NO', regexp_replace('O8_PART_NO', r'^[0]*', ''))
final_df=final_df.dropDuplicates(['O8_PART_NO'])

# COMMAND ----------

# DBTITLE 1,Write to Enrich Harmonised
print(final_df.count())
O8EnrichFolderName = '/PROJECT/P00002-ENRICHED/Harmonised/NonSensitive/PART'
O8EnrichFileName = 'PART.parquet'
from datetime import date,timedelta
today = date.today().strftime("%d%m%Y")
destPath = "abfss://shell31eunadls2tplfcblyo@shell31eunadls2tplfcblyo.dfs.core.windows.net/" + O8EnrichFolderName + "/" + today
final_df = final_df.repartition(100)
final_df.coalesce(1).write.format("parquet").mode("overwrite").parquet(destPath)
  
destPath1 = "abfss://shell31eunadls2tplfcblyo@shell31eunadls2tplfcblyo.dfs.core.windows.net/" + O8EnrichFolderName + "/" + O8EnrichFileName
destPath = "abfss://shell31eunadls2tplfcblyo@shell31eunadls2tplfcblyo.dfs.core.windows.net/" + O8EnrichFolderName + "/" + today  
files = dbutils.fs.ls(destPath)
  #Loop through the files
for i in range (0, len(files)):
  file = files[i].name
      #If file name contains _Success, _Complete or other metadata files, then delete it from the folder 
  if file.startswith("_"):  
    dbutils.fs.rm(files[i].path , True)
    print ('deleted file name: ' + file) # this is for validation , remove the print statements in prod
      #If file name contains part, the actual output file in csv, then rename it to the desired name and save it  
  elif file.startswith("part"):
        #dbutils.fs.mv(files[i].path, "%s/O8_Master_Data2.parquet"%"dbfs:/mnt/Lubesdatalakedev/PROJECT/P00005-TEST-ENRICHED/Unharmonised/Business_Terms/3rd_Party/ORCHESTR8/NonSensitive/O8_Master_Data")
    print(files[i].path)
    dbutils.fs.mv(files[i].path, destPath1)
    print ('renamed file: ' + file) # this is for validation , remove the print statements in prod  
dbutils.fs.rm(destPath, True)

# COMMAND ----------

dbutils.notebook.exit(final_df.count())