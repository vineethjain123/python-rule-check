# Databricks notebook source
from pyspark.sql.functions import to_date, count

# COMMAND ----------

# MAGIC %run /Shared/DIF/Reusable_Functions

# COMMAND ----------

#Perameter set up from config tables
MntPath=dbutils.widgets.get("Env_Path")
TargetObjectName=dbutils.widgets.get("TargetObjectName")
TargetFolderPath=dbutils.widgets.get("TargetFolderPath")
SourceFolderPath=dbutils.widgets.get("SourceFolderPath")
TargetParentPath=dbutils.widgets.get("TargetParentPath")
# LowOffsetValue=dbutils.widgets.get("LowOffsetValue")
ItemName=dbutils.widgets.get("ItemName")
LoadStatus=dbutils.widgets.get("LoadStatus")

# COMMAND ----------

Layer_List = ['Unharmonised','Harmonised']
Sensitive_List = ['NonSensitive','Sensitive']

# COMMAND ----------

# DBTITLE 1,Load ENR Unharmonized Datasets
# final_df = spark.read.format('delta').load(MntPath+SourceFolderPath+"3rd_Party/ORCHESTR8/O8_MASTER_DATA") 
# #Using Harmonised Layer of Orchestr8 Master Data (ShelllubesA8) as to populate correct O8_SUPPLIER_NAME field values
final_df = spark.read.format('delta').load(MntPath+SourceFolderPath.replace(Layer_List[0],Layer_List[1]).replace(Sensitive_List[0],Sensitive_List[1])+'OTHERS/GC_PART_MASTER')
df_T001W = spark.read.format('delta').load(MntPath+SourceFolderPath.replace(Layer_List[0],Layer_List[1])+'OTHERS/GC_PLANT_COMPANY_CODE_MASTER').select('PLANT_CODE','SALES_ORG_CODE','PLANT_LOCAL_CURRENCY').withColumnRenamed('PLANT_CODE','T001W_PLANT_CODE')
df_mtlead = spark.read.format('delta').load(MntPath+SourceFolderPath+'1st_Party/GSAP_DS_HANA/LSC_PURCHASE_LEAD_TIME_MASTER').select('MATERIAL_NO','PLANT_CODE','VENDOR_ACCOUNT_NO').withColumnRenamed('PLANT_CODE','MTLEAD_PLANT_CODE').withColumnRenamed('MATERIAL_NO','MTLEAD_MATERIAL_NO')
df_qmat = spark.read.format('delta').load(MntPath+SourceFolderPath+'1st_Party/GSAP_DS_HANA/INSPECTION_TYPE_MATERIAL_PARAMETER').select('MATERIAL_NO','PLANT_CODE','INSPECTION_TYPE_CODE','POST_TO_INSPECTION_STOCK_IND').withColumnRenamed('PLANT_CODE','QMAT_PLANT_CODE').withColumnRenamed('MATERIAL_NO','QMAT_MATERIAL_NO')

# COMMAND ----------

# DBTITLE 1,O8_MASTER_DATA (A8)
# final_df = AddSplitColumnToDF(final_df,'O8_PART_NO'," ",'MATERIAL_NO',0)
final_df = final_df.withColumnRenamed("EL_MATERIAL_NO","MATERIAL_NO")
final_df = final_df.withColumn('MATERIAL_NO', lpad(final_df['MATERIAL_NO'], 18, '0'))
final_df = final_df.withColumn('O8_FILE_SENT_DATE',to_date(final_df['O8_FILE_SENT_DATE'], 'dd/MM/yyyy'))
max_date=final_df.select(max('O8_FILE_SENT_DATE')).first()
final_df=final_df.filter(final_df['O8_FILE_SENT_DATE']==max_date[0])
final_df=final_df.withColumn('PLANNING_STRATEGY_DESC',when(final_df['PLANNING_STRATEGY_GROUP_CODE'].isin('Z1','Z2','Z3','Z4'),'MTO').otherwise('MTS'))
final_df=final_df.withColumn('O8_DUAL_PART_IND',when(upper(final_df['O8_PART_NO']).contains("DUAL"),'Y'))

# COMMAND ----------

# DBTITLE 1,SALES_ORG_CODE FOR O8 PLANT
df_T001W3 = df_T001W.select('T001W_PLANT_CODE','SALES_ORG_CODE')
final_df = final_df.join(df_T001W3, final_df.PLANT_CODE == df_T001W3.T001W_PLANT_CODE,how='left').drop(df_T001W3.T001W_PLANT_CODE)
final_df=final_df.withColumnRenamed("SALES_ORG_CODE","SALES_ORG_CODE_PLANT")

# COMMAND ----------

# DBTITLE 1,SALES_ORG_CODE FOR O8_SUPPLIER_CODE
df_T001W2=df_T001W.select('T001W_PLANT_CODE','SALES_ORG_CODE')
final_df = final_df.join(df_T001W2, final_df.O8_SUPPLIER_CODE == df_T001W2.T001W_PLANT_CODE,how='left').drop(df_T001W2.T001W_PLANT_CODE)
final_df = final_df.withColumnRenamed("SALES_ORG_CODE","SALES_ORG_CODE_SUPPLIER")

# COMMAND ----------

# DBTITLE 1,GSAP DS1_MM_MT_LEAD
df_mtlead=df_mtlead.withColumn('PART_NO_LEAD',concat(col("MTLEAD_MATERIAL_NO"),lit(' '),col('MTLEAD_PLANT_CODE'),lit(' '),col('VENDOR_ACCOUNT_NO')))
df_mtlead=df_mtlead.select('PART_NO_LEAD','VENDOR_ACCOUNT_NO')
final_df=final_df.withColumn('PART_NO_O8', concat(col('O8_PART_NO'),lit(' '),col('O8_SUPPLIER_CODE')))
final_df = final_df.join(df_mtlead, final_df.PART_NO_O8 == df_mtlead.PART_NO_LEAD,how='left').drop(df_mtlead.PART_NO_LEAD)
final_df=final_df.dropDuplicates(['O8_PART_NO'])

# COMMAND ----------

# DBTITLE 1,GSAP QMAT
df_qmat=df_qmat.withColumn('PART_NO_QMAT', concat(col('QMAT_MATERIAL_NO'),lit(' '),col('QMAT_PLANT_CODE')))
df_qmat=df_qmat.select('PART_NO_QMAT','POST_TO_INSPECTION_STOCK_IND','INSPECTION_TYPE_CODE')
df_qmat=df_qmat.filter(df_qmat.INSPECTION_TYPE_CODE=='01')
final_df = final_df.join(df_qmat, final_df.O8_PART_NO == df_qmat.PART_NO_QMAT,how='left')

# COMMAND ----------

# DBTITLE 1,PROCUREMENT_TYPE_DESC
final_df=final_df.withColumn('PROCUREMENT_TYPE_DESC',when((final_df['O8_SUPPLIER_CODE']==final_df['PLANT_CODE']),'Manufactured'))

final_df=final_df.withColumn('PROCUREMENT_TYPE_DESC',when((final_df['O8_SUPPLIER_CODE'] != final_df['PLANT_CODE']) & (length(final_df['O8_SUPPLIER_CODE'])==4) & (final_df['O8_SUPPLIER_CODE']!=final_df['SALES_ORG_CODE_PLANT']) & (final_df['SALES_ORG_CODE_PLANT']==final_df['SALES_ORG_CODE_SUPPLIER']),'Intra-Company Networked').otherwise(final_df['PROCUREMENT_TYPE_DESC']))

final_df=final_df.withColumn('PROCUREMENT_TYPE_DESC',when((final_df['O8_SUPPLIER_CODE']!=final_df['PLANT_CODE']) & (length(final_df['PLANT_CODE'])==4) & (final_df['PLANT_CODE']!=final_df['SALES_ORG_CODE_PLANT']) & (final_df['SALES_ORG_CODE_PLANT']!=final_df['SALES_ORG_CODE_SUPPLIER']),'Inter-Company Networked').otherwise(final_df['PROCUREMENT_TYPE_DESC']))

final_df=final_df.withColumn('PROCUREMENT_TYPE_DESC',when((final_df['PROCUREMENT_TYPE_DESC'].isNull()) & (final_df['MATERIAL_TYPE_CODE']=='YBSE'),'Externally Purchased - Delivered').otherwise(final_df['PROCUREMENT_TYPE_DESC']))

final_df=final_df.withColumn('PROCUREMENT_TYPE_DESC',when((final_df['PROCUREMENT_TYPE_DESC'].isNull()) & (final_df['VENDOR_ACCOUNT_NO'].isNull()),'Externally Purchased - Delivered').otherwise(final_df['PROCUREMENT_TYPE_DESC']))

final_df=final_df.withColumn('PROCUREMENT_TYPE_DESC',when((final_df['PROCUREMENT_TYPE_DESC'].isNull()) & (final_df['POST_TO_INSPECTION_STOCK_IND']=='X') & (final_df['INSPECTION_TYPE_CODE']=='01'),'Externally Purchased - Collected into Quality Inspection stock').otherwise(final_df['PROCUREMENT_TYPE_DESC']))

final_df=final_df.withColumn('PROCUREMENT_TYPE_DESC',when((final_df['PROCUREMENT_TYPE_DESC'].isNull()) & (final_df['POST_TO_INSPECTION_STOCK_IND']!='X') & (final_df['INSPECTION_TYPE_CODE']!='01'),'Externally Purchased - Collected into available stock').otherwise(final_df['PROCUREMENT_TYPE_DESC']))

final_df=final_df.withColumn('PROCUREMENT_TYPE_DESC',when((final_df['PROCUREMENT_TYPE_DESC'].isNull()) & (final_df['POST_TO_INSPECTION_STOCK_IND'].isNull()) & (final_df['INSPECTION_TYPE_CODE'].isNull()),'Externally Purchased - Collected into available stock').otherwise(final_df['PROCUREMENT_TYPE_DESC']))

# COMMAND ----------

# DBTITLE 1,Final Dataset
final_df=final_df.select('O8_PART_NO','MATERIAL_DESC', 'O8_SUPPLIER_CODE', 'O8_PROCUREMENT_TYPE_DESC', 'O8_REPLENISHMENT_LEAD_TIME', 'MAX_LOT_SIZE', 'O8_MIN_LOT_SIZE', 'O8_ORDER_QTY_IN_RUOM_ROUNDING_VALUE',    'O8_PHASE_IN_DATE', 'O8_PHASE_OUT_DATE', 'O8_PART_TYPE_CODE', 'STANDARD_PRICE', 'O8_ACTIVE_PART', 'MATERIAL_TYPE_CODE', 'MRP_CONTROLLER_CODE', 'O8_NO_OF_DAYS_STRATEGIC_BUFFER', 'PALLET_QTY','O8_PART_STATUS','PRODUCT_HIERARCHY_SKU_CODE', 'MATERIAL_GROUP_CODE', 'PLANNING_STRATEGY_GROUP_CODE', 'O8_SOURCE_LOCATION', 'O8_LEAD_TIME_OFFSET', 'O8_MANUFACTURING_LEAD_TIME', 'O8_RUOM_CONVERSION_FACTOR','O8_REPLENISHMENT_UOM','O8_OMAN_CURRENCY_NO_OF_DECIMAL_PLACES', 'O8_US_LOCATION_TYPE', 'PROCUREMENT_TYPE_CODE', 'PLANT_CODE', 'O8_GROSS_WEIGHT', 'O8_MATERIAL_VOL','O8_DUAL_PART_NO','O8_DECOUPLED_LEAD_TIME','O8_BASE_PART_NO','O8_STACKABLE_PRODUCT_IND','O8_HAZARDOUS_PART_IND','O8_HAUILER_LEAD_TIME','O8_HAUILER_LEAD_TIME_ADJUSTMENT_DAYS','O8_SUPPLIER_NAME', 'O8_TOTAL_BUFFER_QTY_IN_RUOM','O8_TOTAL_BUFFER_VALUE', 'O8_ON_HAND_QTY_IN_RUOM', 'O8_ON_HAND_VALUE', 'O8_OPEN_SHOP_ORDER_QTY_IN_RUOM', 'O8_OPEN_SHOP_ORDER_VALUE', 'O8_OPEN_PURCHASE_ORDER_QTY_IN_RUOM', 'O8_OPEN_PURCHASE_ORDER_VALUE', 'O8_OPEN_CUSTOMER_ORDER_QTY_IN_RUOM', 'O8_OPEN_CUSTOMER_ORDER_VALUE', 'O8_OPEN_PLANNED_DEMAND_QTY_IN_RUOM','O8_OPEN_PLANNED_DEMAND_VALUE','O8_NO_OF_BUFFER_DAYS','O8_PLANNING_RULE', 'O8_REPLACEMENT_PART', 'O8_AVG_DAILY_USAGE', 'O8_FILE_SENT_DATE', 'MATERIAL_NO','PLANNING_STRATEGY_DESC','PROCUREMENT_TYPE_DESC','O8_DUAL_PART_IND')
final_df = final_df.withColumn('O8_PART_NO', regexp_replace('O8_PART_NO', r'^[0]*', ''))
final_df  =  final_df.dropDuplicates(['O8_PART_NO'])
intMergeCount = final_df.count()
final_df.write.option('overwriteschema','true').format("delta").mode("overwrite").option("mergeSchema",'true').save(MntPath+TargetFolderPath)

# COMMAND ----------

df_part = spark.read.format('delta').load(MntPath+TargetFolderPath)
intColumnCount = len(df_part.columns)

dbutils.notebook.exit(json.dumps({
"TargetRecordCount":intMergeCount ,
"TargetColumnCount":intColumnCount
}))