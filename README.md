# Summary

I choose show three different solution instead only one, I thing is better bring more than one scenarios to avoid choose one with more personal preference. Here I am presenting three solutions:

**Scenario 01**\
Here the propouse is a solution using AWS and Databricks woth Delta Lake. Using Delta Lake is possible working in datalake as a lakehouse, doing merge directly in the S3.
Here was used Airflow as job workflow.\
With this approuch we can run ELT process faster and easier, avoiding wasting time in figureout how to write the data in datalake.\
Here is possible too use the autoloader with cloudfiles, that bring a very good aprouch to process easily only new data on streaming and batch.\
The example process:\
`S3 file > Airflow > Databricks > S3 lakehouse > Redshift > Slack Notification`

**Scenario 02**\
Here the approach was creating a native AWS solution, using S3, Lambda, Glue and Redshift.\
Was created a S3 Trigger what calls a Lambda every time an new object is copied in S3, on this case when the json file is put in a pre-determinated folder, the S3 notification callsa lambda that calls the Glue job what will process all data in datalake and send the final data to Redshift.\
The example process:\
`S3 file > S3 tigger > Lambda > Glue > S3 datalake > Redshift`

**Scenario 03**
Here the approach is use EMR using a CLI, but here can use an airflow operator too, or other workflow.\
The example process:\
`S3 file > any workflow > EMR > S3 datalake > Redshift`


In all scenarios was used a datalake with three layers:\
>**Raw:** Here we receive the raw data, like events, log, csv, json, etc.\
**Transformed:** Here the data was already processed one time, cleaned and normalized if is needed.\
**Analytic:** Here the data was store as an analytic table, already agreagated and normalized to final user uses. Here the data cam be consume by Redshift Spectrum, Athena and/or be copied to a Redshift or other Data Warehouse. The best approuch is mantaind the final analytic data in this layer because we are able to change the Data Wahewouse , if we want in some point,change the data warehouse ou even not use any more and access the data directly from datalake, is possible easily. The data will be fresh and realiable in the datalake analytic layer.


Was created the Terraform code for both solutions\
Was used S3 as datalake storage\
Was used Redshift as data wharehouse. Was created a report table in Redshift to be easily accessible by user.


In general the code can be improovend in different ways:
- Adapt the variables in the code to be able reused in multiple environments like, dev, hom and prod;
- In scenario 02 and 03 we can add a previous checkpoint to check if already exist the partition/data before processing the ETL (in case of reprocessing)
- Improve the spark in case of processing a bigger data volume

In summary, I choose the Scenario 01. This option allow us to execute easily merges directly in the datalake(S3) providing a lakehouse arquiteture.\
Here we can use too the autoloader, usign even  S3 list or cloudfiles optios.