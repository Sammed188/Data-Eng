from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
import os
import urllib.request

data = """
{
  "id": 1,
  "trainer": "sai",
  "zeyoStudents": [
    {
      "user": {
        "name": "Archana",
        "age": 32,
        "tools": [
          "hadoop",
          "spark"
        ]
      }
    },
    {
      "user": {
        "name": "arpit",
        "age": 30,
        "tools": [
          "hive",
          "spark"
        ]
      }
    }
  ]
}

"""


df = spark.read.json(sc.parallelize([data]))
df.show()
df.printSchema()

arraydf = df.withColumn("zeyostudents", expr("explode(zeyostudents)"))
arraydf.show()
arraydf.printSchema()

structdf = arraydf.selectExpr(
    "id",
    "trainer",
    "zeyostudents.user.age",
    "zeyostudents.user.name",
    "zeyostudents.user.tools",
).withColumn("tools",expr("explode(tools)"))

structdf.show()
structdf.printSchema()
