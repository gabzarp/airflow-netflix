from pyspark.sql import SparkSession, functions
from pyspark.sql.types import IntegerType
from pyspark import SparkConf, SparkContext

spark = SparkSession(SparkContext(conf=SparkConf()).getOrCreate())

netflix_data = spark.read.csv("../files/netflix_cleaned.csv", header=True, inferSchema=True)

directors = netflix_data.select('director').distinct().withColumn('director_id', functions.monotonically_increasing_id() + 1)
countries = netflix_data.select('country').distinct().withColumn('country_id', functions.monotonically_increasing_id() + 1)
rating = netflix_data.select('rating').distinct().withColumn('rating_id', functions.monotonically_increasing_id() + 1)
type = netflix_data.select('type').distinct().withColumn('type_id', functions.monotonically_increasing_id() + 1)

split_genres = netflix_data.select(functions.split(netflix_data['listed_in'], ', ').alias('genres')).withColumn('genre', functions.explode('genres')).select('genre').distinct().withColumn('genre_id', functions.monotonically_increasing_id() + 1)


directors.write.csv("../files/dim_directors.csv", header=True, mode='overwrite')
countries.write.csv("../files/dim_countries.csv", header=True, mode='overwrite')
rating.write.csv("../files/dim_rating.csv", header=True, mode='overwrite')
type.write.csv("../files/dim_type.csv", header=True, mode='overwrite')
split_genres.write.csv("../files/dim_genres.csv", header=True, mode='overwrite')