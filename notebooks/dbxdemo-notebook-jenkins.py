# Databricks notebook source
import sys
import pyspark.sql.functions as F
sys.path.append("/databricks/python3/lib/python3.8/site-packages")

# COMMAND ----------

import unittest
# from addcol import *
# add more

# add method here instead of import
def with_status(df):
    return df.withColumn("status", F.lit("checked"))

class TestNotebook(unittest.TestCase):
  
  def test_with_status(self):
    source_data = [
      ("pete", "pan", "peter.pan@databricks.com"),
      ("jason", "argonaut", "jason.argonaut@databricks.com")
    ]
    # test a build
    source_df = spark.createDataFrame(
      source_data,
      ["first_name", "last_name", "email"]
    )

    actual_df = with_status(source_df)


    expected_data = [
      ("pete", "pan", "peter.pan@databricks.com", "checked"),
      ("jason", "argonaut", "jason.argonaut@databricks.com", "checked")
    ]

    expected_df = spark.createDataFrame(
      expected_data,
      ["first_name", "last_name", "email", "status"]
    )

    self.assertEqual(expected_df.collect(), actual_df.collect())

unittest.main(argv = [''], verbosity = 2, exit = False)
