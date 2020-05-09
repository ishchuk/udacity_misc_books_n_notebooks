from multiprocessing import Pool
# Function to apply a function over multiple cores
@print_timing
def parallel_apply(apply_func, groups, nb_cores):
    with Pool(nb_cores) as p:
        results = p.map(apply_func, groups)
    return pd.concat(results)

# Parallel apply using 1 core
parallel_apply(take_mean_age, athlete_events.groupby('Year'), 1)

#_______________________________________________________________________
#same but in dask

import dask.dataframe as dd
athlete_dask_df = dd.from_pandas(athlete_df, npartitions=4)

#testing
athlete_dask_df.groupby('Year').mean().compute()

#_______________________________________________________________________
#basic spark code should by put into .py file

from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    athlete_events_spark = (spark
        .read
        .csv("/home/repl/datasets/athlete_events.csv",
             header=True,
             inferSchema=True,
             escape='"'))

    athlete_events_spark = (athlete_events_spark
        .withColumn("Height",
                    athlete_events_spark.Height.cast("integer")))

    print(athlete_events_spark
        .groupBy('Year')
        .mean('Height')
        .orderBy('Year')
        .show())
    
#run spark job from your terminal
spark-submit --master local[4] /home/repl/spark-script.py
#here we are having a local cluster of 4 instances
