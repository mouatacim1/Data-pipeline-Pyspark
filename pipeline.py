from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,BooleanType,DoubleType
from pyspark.sql import functions as F
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[1]").appName("Onedot.com").getOrCreate()

def main() : 

    """**Data importation**"""
    df = spark.read.json("supplier_car.json")
    integration(extraction(normalization(preprocesing(df))))



def preprocesing(input_data) :
    
    pivoted_df = input_data.groupBy("ID").pivot("Attribute Names").agg(F.first("Attribute Values"))
    preprocessed_df = pivoted_df.join(input_data,pivoted_df.ID==input_data.ID,'inner').drop(pivoted_df.ID)
    preprocessed_df = preprocessed_df.drop_duplicates(subset=['ID'])

    """Saving preprocessed data in a csv file"""
    preprocessed_df.toPandas().to_csv("pre-processing.csv", header=True, encoding="utf-8")

    return preprocessed_df


def normalization(input_data) :

    """**Normalisation**"""
    colors_german_english = {"orange":"Orange", "grün":"Green", "schwarz":"black", "grau":"Gray", "gelb":"Yellow", "braun":"Brown", "weiss":"White", "blau":"Blue", "gold":"Gold", "beige":"Beige", "violett":"Violet", "silber":"Silver", "anthrazit":"Anthracite", "rot":"Red", "bordeaux":"Bordeaux" }
    normalised_supplier_data = input_data.withColumn('BodyColorText', regexp_replace('BodyColorText', ' mét.', ''))
    normalised_supplier_data = normalised_supplier_data.na.replace(colors_german_english,1,"BodyColorText")

    normalised_supplier_data = normalised_supplier_data.withColumn("MakeText", F.expr(r"""array_join(transform(split(regexp_replace(MakeText, '(\\s|\\(|-|\\/)(.)', '$1#$2'), '#'), x -> initcap(x)),"")"""))

    normalize_Make = {"Pgo":"PGO", "Austin-healey":"Austin-Healey", "Nsu":"NSU", "Bmw":"BMW", "Agm":"AGM", "Vw":"VW", "Mg":"MG", "Bmw-Alpina":"Alpina", "Ford (usa)": "Ford"}
    normalised_supplier_data = normalised_supplier_data.na.replace(normalize_Make,1,"MakeText")

    """Saving normalized data in a csv file"""
    normalised_supplier_data.toPandas().to_csv("normalisation.csv", header=True, encoding="utf-8")

    return normalised_supplier_data


def extraction(input_data) :


    """**Extraction**"""
    split_col = split(input_data['ConsumptionTotalText'], ' ')
    extracted_supplier_data = input_data.withColumn('extracted-value-ConsumptionTotalText', split_col.getItem(0))
    extracted_supplier_data = extracted_supplier_data.withColumn('extracted-unit-ConsumptionTotalText', split_col.getItem(1))

    """Saving extracted data in a csv file"""

    extracted_supplier_data.toPandas().to_csv("extraction.csv", header=True, encoding="utf-8")

    return extracted_supplier_data


def integration(input_data) :

    """**Integration**"""
    integrated_supplier_data = input_data.select(
        col("MakeText").alias("make"), 
        col("ModelText").alias("model"),
        col("BodyColorText").alias("color"), 
        col("TypeName").alias("model_variant"), 
        col("City").alias("city")
    )

    """Saving integrated data in a csv file"""

    integrated_supplier_data.toPandas().to_csv("integration.csv", header=True, encoding="utf-8")


if __name__ == '__main__':
    main()