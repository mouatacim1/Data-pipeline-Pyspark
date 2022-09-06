# Data-pipeline-Pyspark

This repository is to build a data pipeline using PySpark. The original is data is about 


## Pre-processing
The aim of the pre-processing operation is to get the same granularity of the data target. To achieve that, we did data pivoting of "AttributeNames" and "AttributeValues" columns. New columns are created with the name of "AttributeNames" data and their data is fulfilled by "AttributeValues".

## Data normalization
The aim of data normalization step is to unify some data values ("language", "units"…) and transform it to the same format as the target data.
In the case of our pre-processed data, we translated the "BodyColorTextvalues" from german to english and also "MakeTextvalues" to match it with the target data. 

## Data extraction
Data extraction is needed to get specific information from an already existing data column. In this case, we extracted from ConsumptionTotalTextcolumn the value of consumption and also its unit. We put them in new columns called "extracted-value-ConsumptionTotalText" and "extracted-unit-ConsumptionTotalText"

## Data integration
Data integration is the last step where we rename columns names, remove unneeded columns and integrate all the data we pre-processed, normalized and extracted respecting the same attributes names.

## Data matching

After pre-processing, normalizing, extracting and integrating data, in order to match products from supplier data with target data there are two cases : data of products already existing in the target data that need to get updated by adding new features(columns) and data of new products that need to be inserted for the first time.
To know if a product already exist in the target data, we can look for products in the target data that have exactly same values. If there are some, we can update it by adding new columns, if needed, and updating the data rows by the new ones. Otherwise, we can add the columns, if needed, and insert the new data rows.
This approach is challenging when it comes to a high number of columns because verifying if all the features of the data that we would like to insert matches some of the data in the target data is computationally expensive.
Product matching can be applied just after data integration. To apply it correctly, the data should be pre-processed and normalized correctly to match already existing data.



