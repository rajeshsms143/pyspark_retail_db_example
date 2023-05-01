def load(df,tgt_path):
    df.coalesce(1).write.option("header", True).mode('append') \
            .parquet(f'file:{tgt_path}')
    return "successfully data loaded"
        #parquet('/home/hadoop/data/retail_db_parquet/')