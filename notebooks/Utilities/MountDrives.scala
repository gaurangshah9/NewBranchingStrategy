// Databricks notebook source
// MAGIC %md Mount the Alerts filesystem in Alert Team Storage Account
// COMMAND ----------

val mount = "/mnt/sadlsrccode_dataplatform-retail-alertgeneration"
if (!dbutils.fs.mounts.map(mnt => mnt.mountPoint).contains(mount)) {
  val src = dbutils.secrets.get(scope = "blob", key = "dbr-storage-alertgeneration-source")

  val key = dbutils.secrets.get(scope = "blob", key = "dbr-storage-extraconfigs-key") 
  val storageAccountKey = dbutils.secrets.get(scope = "blob", key = "dbr-storage-extraconfigs-value") 
  dbutils.fs.mount(
    source = src,
    mountPoint = mount,
    extraConfigs = Map(key -> storageAccountKey)
  )
}