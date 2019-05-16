# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC *** Read Catalog Data - Southridge

# COMMAND ----------

# MAGIC %scala
# MAGIC display(dbutils.fs.ls("mnt/adlsg2"))

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS SR_Movies;
# MAGIC CREATE TABLE SR_Movies(
# MAGIC _attachments string,
# MAGIC _etag string,
# MAGIC _rid string,
# MAGIC _self string,
# MAGIC _ts long,
# MAGIC actors string,
# MAGIC availabilityDate string,
# MAGIC genre string,
# MAGIC id string,
# MAGIC rating string,
# MAGIC releaseYear long,
# MAGIC runtime long,
# MAGIC streamingAvailabilityDate string,
# MAGIC tier long,
# MAGIC title string
# MAGIC )
# MAGIC USING json
# MAGIC LOCATION '/mnt/adlsg2/movies/movieTitles';

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from SR_Movies limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS va_Actor;
# MAGIC CREATE TABLE va_Actor(
# MAGIC Actorid	String,
# MAGIC actorname	String,
# MAGIC Gender	String
# MAGIC )
# MAGIC USING csv
# MAGIC LOCATION '/mnt/adlsg2/VanArsdel/dbo.Actors.txt/';

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from va_Actor limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS va_movies;
# MAGIC CREATE TABLE va_movies(
# MAGIC MovieID	String,
# MAGIC MovieTitle	String,
# MAGIC Category	String,
# MAGIC Rating	String,
# MAGIC RunTimeMin	String,
# MAGIC ReleaseDate	String)
# MAGIC USING csv
# MAGIC LOCATION '/mnt/adlsg2/VanArsdel/dbo.Movies.txt/';

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from va_movies limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS va_moviesActors;
# MAGIC CREATE TABLE va_moviesActors(
# MAGIC 	MovieActorID string,
# MAGIC 	MovieID string,
# MAGIC     ActorID string)
# MAGIC USING csv
# MAGIC LOCATION '/mnt/adlsg2/VanArsdel/dbo.MovieActors.txt';

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from va_moviesActors limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT 
# MAGIC Cast(2 as int) as SourceID,
# MAGIC Cast('UUID' as string) as CatalogID,
# MAGIC cast(a.ActorID as string) as ActorID,
# MAGIC cast(a.ActorName as string) as Actor,
# MAGIC cast('' as string) as ReleaseDate,
# MAGIC cast('' as string) as Genre,
# MAGIC Cast(m.Rating as string) as Rating,
# MAGIC year(m.ReleaseDate) AS AvailabilityYear,
# MAGIC Cast(m.ReleaseDate as timestamp) as AvailabilityDate,
# MAGIC Cast(null as int) as MovieTier,
# MAGIC Cast(m.MovieTitle as string) as MovieTitle,
# MAGIC cast(m.MovieID as string) as MovieID
# MAGIC from va_movies m
# MAGIC JOIN va_moviesActors ma
# MAGIC   on m.MovieID = ma.MovieID
# MAGIC JOIN va_Actor a
# MAGIC   ON a.ActorID = ma.ActorID;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT 
# MAGIC CAST(1 as int) as SourceID,
# MAGIC cast('UUID' as string) as CatalogID,
# MAGIC CAST('' as string) as ActorID,
# MAGIC CAST(actors as string) as Actor,
# MAGIC Cast(AvailabilityDate as timestamp) as ReleaseDate,
# MAGIC Cast(Genre as String) as Genre,
# MAGIC Cast(Rating as String) as Rating,
# MAGIC Cast(ReleaseYear as int) AS AvailabilityYear,
# MAGIC Cast(StreamingAvailabilityDate as timestamp) as AvailabilityDate,
# MAGIC Cast(Tier as int) as MovieTier,
# MAGIC Cast(Title as String) as Title,
# MAGIC Cast('' as string) as MovieID
# MAGIC from SR_Movies 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS catalog;
# MAGIC 
# MAGIC CREATE TABLE catalog
# MAGIC USING csv
# MAGIC LOCATION '/mnt/adlsg2/conformed/catalog.txt'
# MAGIC AS 
# MAGIC SELECT 
# MAGIC CAST(1 as int) as SourceID,
# MAGIC cast('UUID' as string) as CatalogID,
# MAGIC CAST('' as string) as ActorID,
# MAGIC CAST(actors as string) as Actor,
# MAGIC Cast(AvailabilityDate as timestamp) as ReleaseDate,
# MAGIC Cast(Genre as String) as Genre,
# MAGIC Cast(Rating as String) as Rating,
# MAGIC Cast(ReleaseYear as int) AS AvailabilityYear,
# MAGIC Cast(StreamingAvailabilityDate as timestamp) as AvailabilityDate,
# MAGIC Cast(Tier as int) as MovieTier,
# MAGIC Cast(Title as String) as Title,
# MAGIC Cast('' as string) as MovieID
# MAGIC from SR_Movies 
# MAGIC UNION ALL
# MAGIC 
# MAGIC SELECT 
# MAGIC Cast(2 as int) as SourceID,
# MAGIC Cast('UUID' as string) as CatalogID,
# MAGIC cast(a.ActorID as string) as ActorID,
# MAGIC cast(a.ActorName as string) as Actor,
# MAGIC cast('' as string) as ReleaseDate,
# MAGIC cast('' as string) as Genre,
# MAGIC Cast(m.Rating as string) as Rating,
# MAGIC year(m.ReleaseDate) AS AvailabilityYear,
# MAGIC Cast(m.ReleaseDate as timestamp) as AvailabilityDate,
# MAGIC Cast(null as int) as MovieTier,
# MAGIC Cast(m.MovieTitle as string) as MovieTitle,
# MAGIC cast(m.MovieID as string) as MovieID
# MAGIC from va_movies m
# MAGIC JOIN va_moviesActors ma
# MAGIC   on m.MovieID = ma.MovieID
# MAGIC JOIN va_Actor a
# MAGIC   ON a.ActorID = ma.ActorID;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS catalog;
# MAGIC 
# MAGIC CREATE TABLE catalog
# MAGIC USING parquet
# MAGIC LOCATION '/mnt/adlsg2/conformed/catalog'
# MAGIC AS 
# MAGIC SELECT 
# MAGIC CAST(1 as int) as SourceID,
# MAGIC cast('UUID' as string) as CatalogID,
# MAGIC CAST('' as string) as ActorID,
# MAGIC CAST(actors as string) as Actor,
# MAGIC Cast(AvailabilityDate as timestamp) as ReleaseDate,
# MAGIC Cast(Genre as String) as Genre,
# MAGIC Cast(Rating as String) as Rating,
# MAGIC Cast(ReleaseYear as int) AS AvailabilityYear,
# MAGIC Cast(StreamingAvailabilityDate as timestamp) as AvailabilityDate,
# MAGIC Cast(Tier as int) as MovieTier,
# MAGIC Cast(Title as String) as Title,
# MAGIC Cast('' as string) as MovieID
# MAGIC from SR_Movies 
# MAGIC UNION ALL
# MAGIC 
# MAGIC SELECT 
# MAGIC Cast(2 as int) as SourceID,
# MAGIC Cast('UUID' as string) as CatalogID,
# MAGIC cast(a.ActorID as string) as ActorID,
# MAGIC cast(a.ActorName as string) as Actor,
# MAGIC cast('' as string) as ReleaseDate,
# MAGIC cast('' as string) as Genre,
# MAGIC Cast(m.Rating as string) as Rating,
# MAGIC year(m.ReleaseDate) AS AvailabilityYear,
# MAGIC Cast(m.ReleaseDate as timestamp) as AvailabilityDate,
# MAGIC Cast(null as int) as MovieTier,
# MAGIC Cast(m.MovieTitle as string) as MovieTitle,
# MAGIC cast(m.MovieID as string) as MovieID
# MAGIC from va_movies m
# MAGIC JOIN va_moviesActors ma
# MAGIC   on m.MovieID = ma.MovieID
# MAGIC JOIN va_Actor a
# MAGIC   ON a.ActorID = ma.ActorID;