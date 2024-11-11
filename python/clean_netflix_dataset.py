from pyspark.sql import SparkSession, functions
from pyspark.sql.types import IntegerType
from pyspark import SparkConf, SparkContext

spark = SparkSession(SparkContext(conf=SparkConf()).getOrCreate())

netflix_data = spark.read.csv("../files/netflix1.csv", header=True, inferSchema=True)


#Duplicates
netflix_duplicates = netflix_data.groupby('title').agg(functions.count('title').alias('count_title'), functions.max('show_id').alias('show_id')).where(functions.count('title') > 1)
netflix_data =  netflix_data.join(netflix_duplicates, ['show_id'], how='left_anti')

#Missing values flying fortress
show_id_max = netflix_data.agg(functions.max('show_id').alias('show_id_max'))

flying_fortress = netflix_data.filter(netflix_data['show_id'] == 'Flying Fortress"') \
    .withColumn('listed_in', netflix_data['rating']) \
    .withColumn('duration', netflix_data['release_year']) \
    .withColumn('rating', netflix_data['date_added']) \
    .withColumn('release_year', netflix_data['country']) \
    .withColumn('date_added', netflix_data['director']) \
    .withColumn('country', netflix_data['title']) \
    .withColumn('director', netflix_data['type']) \
    .withColumn('title', functions.regexp_replace('show_id', '"', '')) \
    .withColumn('type', functions.lit('Movie')) \
    .crossJoin(show_id_max)

flying_fortress = flying_fortress.withColumn('show_id', functions.concat(flying_fortress['show_id_max'], functions.lit('1'))).drop('show_id_max')
        
netflix_data = netflix_data.union(flying_fortress).filter(~netflix_data['show_id'].isin('Flying Fortress"', 's8420'))

# Filter out specific show_ids
netflix_data = netflix_data.withColumn('release_year', netflix_data['release_year'].cast(IntegerType()))

netflix_data.write.csv("../files/netflix_cleaned.csv", header=True, mode='overwrite')