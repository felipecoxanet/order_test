#!/bin/bash
sudo pip install -U \
    matplotlib \
    pandas \
    spark-nlp
sudo aws s3 cp s3://emr-temp/jars/spark-redshift_2.11-2.0.1.jar /usr/lib/spark/jars/
sudo aws s3 cp s3://emr-temp/jars/aws_java_sdk_1_12_134.jar /usr/lib/spark/jars/
sudo aws s3 cp s3://emr-temp/jars/log4j-web-2.17.0.jar /usr/lib/spark/jars/