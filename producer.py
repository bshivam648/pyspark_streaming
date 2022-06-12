import string
import random

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import time

def randomString(length):
    letters = string.ascii_lowercase
    first_name = ''.join(random.choice(letters) for i in range(length))
    last_name = ''.join(random.choice(letters) for i in range(length))
    gender = random.choice(('M', 'F'))
    year = str(random.randint(2000, 2021))
    month = str(random.randint(1,12))
    date = str(random.randint(1,28))
    dob = year+'-'+month+'-'+date
    email = ''.join(random.choice(letters) for i in range(length)) + '@' + 'gmail.com'
    user_name = ''.join(random.choice(letters) for i in range(length))
    phone = random.randint(1000000000, 9999999999)
    city = ''.join(random.choice(letters) for i in range(length))
    Country = ''.join(random.choice(letters) for i in range(length))
    return first_name, last_name, gender, dob, email, user_name, phone, city, Country


spark = SparkSession.builder.appName('test').getOrCreate()
sc = spark.sparkContext

schema = StructType([StructField('ID',LongType(),True),StructField('first_name',StringType(),True),
                         StructField('last_name',StringType(),True),StructField('gender',StringType(),True),
                         StructField('dob',StringType(),True),StructField('email',StringType(),True),
                         StructField('user_name',StringType(),True),StructField('phone',LongType(),True),
                         StructField('city',StringType(),True),StructField('Country',StringType(),True)])

while True:
    rdd = sc.parallelize(range(1, random.randint(3, 5))).map(lambda x: (x, randomString(5)))
    df = rdd.toDF().withColumnRenamed("_1", "ID"). \
        withColumnRenamed("_2", "values").cache()
    cols = ['ID', 'first_name', 'last_name', 'gender', 'dob', 'email', 'user_name', 'phone', 'city', 'Country']
    df = df.select('ID', 'values.*').toDF(*cols)

    #     for duplicate record
    phone1 = df.select(df.phone).collect()[0][0]
    data = [('1', 'jtiba', 'chxqu', 'M', '2009-12-6', 'cwmln@gmail.com', 'jtiba', f'{phone1}', 'iuwii', 'trmml')]
    df_1 = spark.createDataFrame(data, cols)
    df = df.unionAll(df_1)

    df.show()
    df.coalesce(1).write.option("header", True).mode('append').csv(
        f"/Users/shivam/IdeaProjects/albenaro_data/raw_data/datacsv/")
    time.sleep(1)