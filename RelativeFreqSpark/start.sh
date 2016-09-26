#!/bin/bash

mvn clean package
spark-submit --class edu.mum.bigdata.spark.mo.App --master local target/ProductsRelativeFreq-0.0.1-SNAPSHOT.jar /user/hive/warehouse/apache_access_logs
