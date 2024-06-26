<img width="1440" alt="Screenshot 2024-06-26 at 9 50 35 PM" src="https://github.com/DianaKahar/Data_management4/assets/149170546/2dfdecd2-c89d-4e4d-97e3-d9ee261c0717"># Data_management4

# STQD6324 DATA MANAGEMENT

# INTRODUCTION


The MovieLens dataset is a widely used dataset in the field of data science and machine learning for performing various analytical and predictive tasks related to movie recommendations. This project leverages the MovieLens 100k dataset to perform data processing, analysis, and querying using Apache Spark and Apache Cassandra.

Apache Cassandra is scalable, distributed by NoSQL database which is designed to handle multiple nodes across multiple data centres. It uses a peer to peer distribuion system across a cluster of nodes where each nodes is able to communicate with other nodes in the cluster without needing a master node. Cassandra is also able to provide linear scalability and designed to be fault tolerant. CQL is the quary language to interact with Cassandra. Similarly to SQL and provides syntax for creating tables, inserting data, querying data and managing the database schema.


# DATASET INFORMATION

The dataset was collected by Grouplens Research and made available rating datasets from MovieLens website. The dataset was collected over a period of time and the dataset used in this project is from MovieLens 100k Dataset. This dataset was released at Febuary 2003, it has 100,000 ratings from 1000 users on 1700 movies. Each user rated at least 20 movies and the users demographic was collected (age, gender, occupation, zip). for this project onlu u.user, u.item and u.data was used.


# PROJECT OBJECTIVES

i) Calculate the average rating for each movie.

ii) Identify the top ten movies with the highest average ratings.

iii) Find the users who have rated at least 50 movies and identify their favourite movie genres.

iv) Find all the users with age that is less than 20 years old.

v) Find all the users who have the occupation “scientist” and their age is between 30 and 40 years old.


# CODE IN PYTHON 

    from pyspark.sql import SparkSession
    from pyspark.sql import Row
    from pyspark.sql.functions import avg, col, explode, array
    
    #Initialize Spark session
    spark = SparkSession.builder \
    .appName("MovieLens Analysis") \
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .getOrCreate()
    
    #Parse the u.user file
    def parse_user(line):
    fields = line.split('|')
    return Row(user_id=int(fields[0]), age=int(fields[1]), gender=fields[2], occupation=fields[3], zip=fields[4])
    
    def parse_data(line):
    fields = line.split("\t")
    return Row(user_id=int(fields[0]), movie_id=int(fields[1]), rating=int(fields[2]), timestamp=int(fields[3]))
    
    def parse_item(line):
    fields = line.split("|")
    genres = list(map(int, fields[5:]))
    return Row(movie_id=int(fields[0]), title=fields[1], release_date=fields[2], 
               vid_release_date=fields[3], url=fields[4], genres=genres)
    
    if __name__ == "__main__":

    # Parse data
    lines1 = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.user")
    user = line1.map(parse_user)

    line2 = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.data")
    rating = line2.map(parse_data)

    line3 = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.item")
    name = line3.map(parse_item)

    # Convert to Dataframe
    userDT = spark.createDataFrame(user)
    ratingDT = spark.createDataFrame(rating)
    nameDT = spark.createDataFrame(name)

    # Write DataFrames to Cassandra
    userDT.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="user", keyspace="movielen") \
        .save()

    ratingDT.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="rating", keyspace="movielen") \
        .save()

    nameDT.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="name", keyspace="movielen") \
        .save()

    # Read DataFrames from Cassandra
    readUser = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="user", keyspace="movielen").load()

    readRating = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="rating", keyspace="movielen").load()

    readName = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="name", keyspace="movielen").load()

    # Create temporary views for DataFrames
    readUser.createOrReplaceTempView("user")
    readRating.createOrReplaceTempView("rating")
    readName.createOrReplaceTempView("name")

    # i) Calculate the average rating for each movie
    avg_ratings = readRating.groupBy("movie_id").agg(avg("rating").alias("avg_rating")).orderBy(col("avg_rating").desc())
    avg_ratings.show(10)

    # ii) Identify the top ten movies with the highest average ratings
    top_movies = avg_ratings.join(readName, "movie_id").select("title", "avg_rating").orderBy(col("avg_rating").desc()).limit(10)
    top_movies.show()

    # iii) Find the users who have rated at least 50 movies and identify their favourite movie genres
    user_ratings_count = readRating.groupBy("user_id").count().filter(col("count") >= 50)
    user_genre_ratings = readRating.join(readName, "movie_id").withColumn("genre", explode(array([col(f).alias(f) for f in [
        "unknown", "action", "adventure", "animation", "children", "comedy", "crime", "documentary",
        "drama", "fantasy", "film_noir", "horror", "musical", "mystery", "romance", "sci_fi", "thriller", "war", "western"]])))
    user_genre_ratings = user_genre_ratings.groupBy("user_id", "genre").count()
    frequent_users_genres = user_ratings_count.join(user_genre_ratings, "user_id").orderBy("user_id", "count", ascending=[1, 0])
    frequent_users_genres.show(10)

    # iv) Find all the users with age that is less than 20 years old
    young_users = readUser.filter(col("age") < 20)
    young_users.show(10)

    # v) Find all the users who have the occupation “scientist” and their age is between 30 and 40 years old
    scientists = readUser.filter((col("occupation") == "scientist") & (col("age").between(30, 40)))
    scientists.show(10)


    spark.stop()


# RESULTS

1. Average Rating of Each Movie.
<img width="274" alt="Screenshot 2024-06-26 at 9 25 28 PM" src="https://github.com/DianaKahar/Data_management4/assets/149170546/748938e2-11b9-41c7-9c51-060502ca7196">

2. Top Ten Movies with the Highest Average Ratings.
<img width="249" alt="Screenshot 2024-06-26 at 9 27 15 PM" src="https://github.com/DianaKahar/Data_management4/assets/149170546/3439bf26-3d8e-4671-9c5e-da62260f2b8f">

   
3. Users Rated at least 50 Movies and Their Favourite Movie Genres.
<img width="380" alt="Screenshot 2024-06-26 at 9 29 59 PM" src="https://github.com/DianaKahar/Data_management4/assets/149170546/86ac2221-caca-4a88-adff-b63c524061a5">


4. Users with Age Less than 20 Years Old.
<img width="403" alt="Screenshot 2024-06-26 at 9 32 43 PM" src="https://github.com/DianaKahar/Data_management4/assets/149170546/d5d13b70-6061-41f1-9092-553c48854f2c">


5. Users Occupation “Scientist” and Age Between 30 and 40 Years Old.
<img width="371" alt="Screenshot 2024-06-26 at 9 33 16 PM" src="https://github.com/DianaKahar/Data_management4/assets/149170546/d83d765e-6bab-4e83-aab8-49e2449ecf67">
