import json

import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
from fbprophet import Prophet
from fbprophet.serialize import model_to_json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import current_date
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import *
from sklearn.metrics import mean_squared_error

mpl.rcParams['figure.figsize'] = (10, 8)
mpl.rcParams['axes.grid'] = False


@pandas_udf(StructType([
    StructField('ds', TimestampType()),
    StructField('cat', StringType()),
    StructField('y', DoubleType()),
    StructField('yhat', DoubleType()),
    StructField('yhat_upper', DoubleType()),
    StructField('yhat_lower', DoubleType())
]), PandasUDFType.GROUPED_MAP)
def insample_forecast(cat_pd):
    category = cat_pd['cat'].iloc[0]
    model = Prophet()
    train = cat_pd.iloc[:cat_pd.shape[0] - 12]
    model.fit(train)
    forecast_pd = model.predict(cat_pd)
    f_pd = forecast_pd[['ds', 'yhat', 'yhat_upper', 'yhat_lower']].set_index('ds')
    ct_pd = cat_pd[['ds', 'cat', 'y']].set_index('ds')
    result_pd = f_pd.join(ct_pd, how='left')
    result_pd.reset_index(level=0, inplace=True)
    result_pd['cat'] = category
    return result_pd[['ds', 'cat', 'y', 'yhat', 'yhat_upper', 'yhat_lower']]


@pandas_udf(StructType([
    StructField('cat', StringType()),
    StructField('MSE', DoubleType()),
    StructField('RMSE', DoubleType())
]), PandasUDFType.GROUPED_MAP)
def performance(cat_pd):
    category = cat_pd['cat'].iloc[0]
    model = Prophet()
    train = cat_pd.iloc[:cat_pd.shape[0] - 12]
    model.fit(train)
    forecast_pd = model.predict(cat_pd)
    mse = mean_squared_error(cat_pd['y'].values, forecast_pd['yhat'].values)
    rmse = mean_squared_error(cat_pd['y'].values, forecast_pd['yhat'].values, squared=False)
    return pd.DataFrame(data={'cat': [category], 'MSE': mse, 'RMSE': rmse})


@pandas_udf(StructType([
    StructField('ds', TimestampType()),
    StructField('cat', StringType()),
    StructField('y', DoubleType()),
    StructField('yhat', DoubleType()),
    StructField('yhat_upper', DoubleType()),
    StructField('yhat_lower', DoubleType())
]), PandasUDFType.GROUPED_MAP)
def forecast_sales(cat_pd):
    category = cat_pd['cat'].iloc[0]
    model = Prophet()
    model.fit(cat_pd)
    with open(f'/tmp/models/serialized_model-{category}.json', 'w') as fout:
        json.dump(model_to_json(model), fout)  # Save model
    future_pd = model.make_future_dataframe(periods=12, freq='MS')
    forecast_pd = model.predict(future_pd)
    f_pd = forecast_pd[['ds', 'yhat', 'yhat_upper', 'yhat_lower']].set_index('ds')
    ct_pd = cat_pd[['ds', 'cat', 'y']].set_index('ds')
    result_pd = f_pd.join(ct_pd, how='left')
    result_pd.reset_index(level=0, inplace=True)
    result_pd['cat'] = category
    return result_pd[['ds', 'cat', 'y', 'yhat', 'yhat_upper', 'yhat_lower']]


def main():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("BDM - ML") \
        .getOrCreate()
    df = spark.read.format("jdbc").option("url", "jdbc:postgresql://clefable.fib.upc.edu:9700/bdm") \
        .option("driver", "org.postgresql.Driver").option("dbtable", "agricultural_exports_agg_full") \
        .option("user", "postgres").option("password", "postgres").load()

    # A CSV with the same data of the database is provided in case the code has to be run and the database is not available
    # This CSV is an exact copy of the Materialized View mentioned in the report, but only with data from Brazil
    # df = spark.read.option("header", "true").csv("./bdm_public_agricultural_exports_agg_full.csv")

    df.orderBy(["category", col("month").desc()]).show(4)

    df.createOrReplaceTempView("exports")

    cat_df = spark.sql(
        "SELECT category as cat, month as ds, total_price as y FROM exports WHERE month > '2018' AND origin = 'BRA' ORDER BY cat, month")
    pdf = cat_df.toPandas()

    cat_part = (cat_df.repartition(spark.sparkContext.defaultParallelism, ['ds'])).cache()
    cat_part.explain()

    performance = (cat_part.groupby('cat').apply(performance).withColumn('training_date', current_date()))
    testing = (cat_part.groupby('cat').apply(insample_forecast).withColumn('training_date', current_date()))
    results = (cat_part.groupby('cat').apply(forecast_sales).withColumn('training_date', current_date()))
    results.cache()

    performance.coalesce(1)
    performance.createOrReplaceTempView('testPerformance')

    testing.coalesce(1)
    testing.createOrReplaceTempView('test')

    results.coalesce(1)
    results.createOrReplaceTempView('forecasted')

    spark.sql("SELECT * FROM testPerformance ORDER BY MSE ASC").show(4)

    category = '0706'  ## Edit to the desired category

    spark.sql(f"SELECT * FROM test WHERE cat = '{category}'").show(4)

    spark.sql(f"SELECT * FROM forecasted WHERE cat = '{category}'").show(4)

    final_df = results.toPandas()
    fdf_cat = final_df[final_df['cat'] == category].copy().set_index('ds')

    pdf_cat = pdf[pdf['cat'] == category].copy()
    pdf_cat['ds'] = pd.to_datetime(pdf_cat['ds'])
    pdf_cat = pdf_cat.set_index('ds')

    fdf_cat = fdf_cat.combine_first(pdf_cat).fillna(0.0)
    fdf_cat['y'] = fdf_cat['y'].astype(float)

    fig, ax = plt.subplots()
    fdf_cat[['y', 'yhat']].plot(ax=ax)
    plt.title(f'Category {category} predictions')
    plt.ylabel('Total Price')
    plt.xlabel('Month')
    ax.legend(["Real value", "Forecast"])
    plt.show()


if __name__ == '__main__':
    main()
