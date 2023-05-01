import os

from get_spark_session import spark_session
from pyspark.sql.functions import concat_ws
from load_file import load
from extract_file import src_file

if __name__ == '__main__':
    print("Calling spark session")
    #src_path = "/home/hadoop/data/retail_db_json/customers"
    #tgt_path = "/home/hadoop/data/retail_db_parquet/customers"
    src_path = os.getenv('src_path')
    tgt_path = os.getenv('tgt_path')

    spark = spark_session("deployApp","local")
    input_file = src_file(src_path,"part-r-00000-70554560-527b-44f6-9e80-4e2031af5994","json")

    df = spark.read.format("json").option("header",True).option("inferSchema",True).load(input_file)
    df_full_name = df.select(df.customer_id,concat_ws(' , ',df.customer_fname,df.customer_lname) \
                             .alias("full_name"),df.customer_street)
    #loading data into target path with parquet format
    load(df_full_name,tgt_path)

    print(spark)


