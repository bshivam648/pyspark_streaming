import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('abcd').getOrCreate()
schema = StructType([StructField('ID',LongType(),True),StructField('first_name',StringType(),True),
                         StructField('last_name',StringType(),True),StructField('gender',StringType(),True),
                         StructField('dob',StringType(),True),StructField('email',StringType(),True),
                         StructField('user_name',StringType(),True),StructField('phone',LongType(),True),
                         StructField('city',StringType(),True),StructField('Country',StringType(),True)])

from pyspark.streaming import StreamingContext
sc = spark.sparkContext
ssc = StreamingContext(sc, 2)
ssc.checkpoint("/Users/shivam/IdeaProjects/albenaro_data/checkpoint/")

ds = spark.readStream.schema(schema).format("csv").load(f"/Users/shivam/IdeaProjects/albenaro_data/raw_data/datacsv/")

# ds = ds.withColumn('timestamp', lit(datetime.datetime.now())).withWatermark("timestamp", "10 minutes")
# d1 = ds.groupBy(col("phone"), col("timestamp")).agg(count('phone').alias('count'))
#               .orderBy('timestamp', ascending=False)

# ds = ds.groupby('phone').agg(count('phone').alias('count'))
# ds = ds.filter("email = cwmln@gmail.com")
# ds = ds.join(d1, ['phone', 'timestamp'], how='left')
# ds = ds.filter('count < 2')
# d1 = ds.dropDuplicates(subset=('phone', ))
# ds = ds.dropDuplicates(subset=('phone', ))
# ds_1 = ds.na.drop()

# tl = [i for i in ds.select('phone').distinct().collect()]
# ll = ll.append(tl)

# 3 question
# df_3 = ds.withColumn('count', lit(ds.count())).withColumn('timestamp', lit(datetime.datetime.now())).select('count', 'timestamp').limit(1)
#

# 4 question
# df_4 = ds_1.withColumn('count', lit(ds_1.count())).withColumn('timestamp', lit(datetime.datetime.now())).select('count', 'timestamp').limit(1)
#


# def foreach_batch_function(df, epochId):
    # Transform and write batchDF
#     df_1 = df.dropDuplicates(subset=('phone'))
#     df.coalesce(1).write.option("header", True).format("csv").mode("append").csv(f"/Users/shivam/IdeaProjects/albenaro_data/final/data_pro_temp/")

#     df_1.write.option("header",True).mode('append').csv(f"/Users/shivam/IdeaProjects/albenaro_data/temp/")
#     pass

# ds.writeStream.foreachBatch(foreach_batch_function).outputMode("update").start()

# trigger(processingTime="10 seconds")
w = ds.coalesce(1).writeStream.option("header", True).format("csv").option("checkpointLocation", "/Users/shivam/IdeaProjects/albenaro_data/checkpoint/").option("path", "/Users/shivam/IdeaProjects/albenaro_data/final/data_pro").outputMode("append")
# w_2 = ds_1.coalesce(1).writeStream.option("header", True).format("csv").option("checkpointLocation", "/Users/shivam/IdeaProjects/albenaro_data/checkpoint_2/").option("path", "/Users/shivam/IdeaProjects/albenaro_data/final/data_pro_2").outputMode("append")
# w_3 = df_3.coalesce(1).writeStream.option("header", True).format("csv").option("checkpointLocation", "/Users/shivam/IdeaProjects/albenaro_data/checkpoint_2/").option("path", "/Users/shivam/IdeaProjects/albenaro_data/final/data_pro_3").outputMode("append")
# w_4 = df_4.coalesce(1).writeStream.option("header", True).format("csv").option("checkpointLocation", "/Users/shivam/IdeaProjects/albenaro_data/checkpoint_2/").option("path", "/Users/shivam/IdeaProjects/albenaro_data/final/data_pro_4").outputMode("append")
w.start()
# w_2.start()
# w_3.start()
# w_4.start()
