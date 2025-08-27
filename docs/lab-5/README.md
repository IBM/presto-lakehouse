# Create and Query Delta Lake Tables

In this section, you will create different a Delta Lake table with Spark and query it with Presto.

This section is comprised of the following steps:

- [Create and Query Delta Lake Tables](#create-and-query-delta-lake-tables)
  - [1. Create a Delta Lake table](#1-create-a-delta-lake-table)
  - [2. Query table with Presto](#2-query-table-with-presto)
  - [3. Add data to table and query](#3-add-data-to-table-and-query)
    - [Shutdown](#shutdown)

If you previously stopped your lakehouse containers, restart them now with:

```sh
docker compose up -d
```

## 1. Create a Delta Lake table

In this section we'll explore Delta Lake tables. Currently, it is not possible to create Delta tables from Presto, so we will use Spark to create our tables. To do so, we'll enter the Spark container and start the `spark-shell`:

```sh
docker exec -it spark /opt/spark/bin/spark-shell
```

It may take a few moments to initialize before you see the `scala>` prompt, indicating that the shell is ready to accept commands. Enter "paste" mode by typing the following and pressing enter:

```sh
:paste
```

For example:

```sh
scala> :paste

// Entering paste mode (ctrl-D to finish)
```

Copy and paste the below code, which imports required packages, creates a Spark session, and defines some variables that we will reference in subsequent code.

!!! note
    Note: while the code looks similar, it is a bit different from that in our labs 2 and 3. Don't skip this step.

```scala
import org.apache.spark.sql.{SparkSession, SaveMode}
import scala.util.Random
import java.util.UUID

val spark = SparkSession.builder()
  .appName("DeltaToMinIO")
  .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  .config("spark.sql.catalogImplementation", "hive")
  .config("hive.metastore.uris", "thrift://hive-metastore:9083")
  .config("spark.sql.hive.convertMetastoreParquet", "false")
  .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
  .config("spark.hadoop.fs.s3a.access.key", "minio")
  .config("spark.hadoop.fs.s3a.secret.key", "minio123")
  .config("spark.hadoop.fs.s3a.path.style.access", "true")
  .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
  .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
  .enableHiveSupport()
  .getOrCreate()

import spark.implicits._
import org.apache.hudi.QuickstartUtils._
import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._

val basePath = "s3a://warehouse/delta-tables"
val dbName = "default"
```

Make sure you include a newline character at the very end. Press `Ctrl+D` to begin executing the pasted code.

We will complete the same process with our next code block, which will create and populate our table with randomly generated data about taxi trips. Notice that we are including an extra column, `commit_num` that will show us the commit in which any given row was added.

```scala
val dataGen = new DataGenerator
val inserts = convertToStringList(dataGen.generateInserts(10))
val data = spark.read.json(spark.sparkContext.parallelize(inserts, 2))

val tableName = "trips_table"

data.withColumn("commit_num", lit("update1")).write.format("delta").
    mode(Overwrite).
    save(s"$basePath/$tableName");
```

Before we go on to query these tables, let's take a look at what files and directories have been created for this table in our s3 storage. Go to MinIO UI [http://localhost:9091](http://localhost:9091) and log in with the username and password that we defined in `docker-compose.yaml` (`minio`/`minio123`). Under the `delta-tables` path, there should be a sub-path called `trips_table`. Click into this path and explore the created files and directory structure, include those in the `_delta_log` directory. This is where Delta keeps metadata for the `trips_table`. TODO add more details here including potential screenshot.

## 2. Query table with Presto

Now let's query these tables with Presto. In a new terminal tab or window, exec into the Presto container and start the Presto CLI to query our table.

```sh
 docker exec -it coordinator presto-cli
```

There are a handful of ways to query a Delta Lake table with Presto. The first is by registering a table with an external location that corresponds to the path where the table is stored.

```sh
CREATE TABLE delta.default.trips_table (dummyColumn INT) WITH (external_location = 's3a://warehouse/delta-tables/trips_table');
```

TODO explain the dummy column and otherwise why we're doing this.

Now we can list the available tables:

```sql
show tables;
```

For example:

```sh
presto> show tables in delta.default;
       Table        
--------------------
 trips_table        
(1 rows)
```

and also read from our table with a statement such as:

```sql
select commit_num, fare, begin_lon, begin_lat from delta.default.trips_table;
```

We can also query our table using a special syntax that supplies the direct path to the table. Note how in this command, we don't specify a table name at all, just the path to the data.

```sql
select commit_num, fare, begin_lon, begin_lat from delta."$path$"."s3a://warehouse/delta-tables/trips_table";
```

For example:

```sh
presto:default> select commit_num, fare, begin_lon, begin_lat from delta."$path$"."s3a://warehouse/delta-tables/trips_table";
 commit_num |        fare        |      begin_lon       |      begin_lat       
------------+--------------------+----------------------+----------------------
 update1    | 34.158284716382845 |  0.46157858450465483 |   0.4726905879569653 
 update1    |   43.4923811219014 |   0.8779402295427752 |   0.6100070562136587 
 update1    |  64.27696295884016 |   0.4923479652912024 |   0.5731835407930634 
 update1    |  93.56018115236618 |  0.14285051259466197 |  0.21624150367601136 
 update1    | 17.851135255091155 |   0.5644092139040959 |     0.40613510977307 
 update1    |  33.92216483948643 |   0.9694586417848392 |   0.1856488085068272 
 update1    |  66.62084366450246 |  0.03844104444445928 |   0.0750588760043035 
 update1    |  41.06290929046368 |   0.8192868687714224 |    0.651058505660742 
 update1    |  27.79478688582596 |   0.6273212202489661 |  0.11488393157088261 
(10 rows)
```

TODO show other ways to query the table

## 3. Add data to table and query

Now, let's go back to our `spark-shell` terminal tab and add more data to our tables using paste mode. Note that our `commit_num` column value has changed.

```scala
val updates = convertToStringList(dataGen.generateUpdates(10))
val updatedData = spark.read.json(spark.sparkContext.parallelize(updates, 2));

updatedData.withColumn("commit_num", lit("update2")).write.format("delta").
    mode(Append).
    save(s"$basePath/$tableName");
```

Now we can query the table in the Presto CLI using the snapshot identifier. Since we've added data to our table twice, we now have 2 snapshots - a `v0` snapshot and a `v1` snapshot. Let's query them to see the difference.

```sql
select commit_num, fare, begin_lon, begin_lat from delta.default."trips_table@v1"
```

For example:

```sh
presto> select commit_num, fare, begin_lon, begin_lat from delta.default."trips_table@v1";
 commit_num |        fare        |      begin_lon       |      begin_lat       
------------+--------------------+----------------------+----------------------
 update1    | 34.158284716382845 |  0.46157858450465483 |   0.4726905879569653 
 update1    |   43.4923811219014 |   0.8779402295427752 |   0.6100070562136587 
 update1    |  64.27696295884016 |   0.4923479652912024 |   0.5731835407930634 
 update1    |  93.56018115236618 |  0.14285051259466197 |  0.21624150367601136 
 update1    | 17.851135255091155 |   0.5644092139040959 |     0.40613510977307 
 update1    |  33.92216483948643 |   0.9694586417848392 |   0.1856488085068272 
 update1    |  66.62084366450246 |  0.03844104444445928 |   0.0750588760043035 
 update1    |  41.06290929046368 |   0.8192868687714224 |    0.651058505660742 
 update1    |  27.79478688582596 |   0.6273212202489661 |  0.11488393157088261 
 update2    |  9.384124531808036 |   0.6999655248704163 |  0.16603428449020086 
 update2    |  91.99515909032544 |   0.2783086084578943 |   0.2110206104048945 
 update2    | 49.527694252432056 |   0.5142184937933181 |   0.7340133901254792 
 update2    |  29.47661370147079 | 0.010872312870502165 |   0.1593867607188556 
 update2    |  86.75932789048282 |  0.13755354862499358 |   0.7180196467760873 
 update1    | 19.179139106643607 |   0.7528268153249502 |   0.8742041526408587 
 update2    |   98.3428192817987 |   0.3349917833248327 |   0.4777395067707303 
 update2    |  2.375516772415698 |  0.42849372303000655 | 0.014159831486388885 
 update2    |   90.9053809533154 |  0.19949323322922063 |  0.18294079059016366 
 update2    |  63.72504913279929 |    0.888493603696927 |   0.6570857443423376 
 update2    |  90.25710109008239 |   0.4006983139989222 |  0.08528650347654165 
(20 rows)
```

We can see that this table includes commits both from update 1 and update 2. Let's see what version `v0` looks like.

```sql
select commit_num, fare, begin_lon, begin_lat from delta.default."trips_table@v0"
```

For example:

```sh
presto> select commit_num, fare, begin_lon, begin_lat from delta.default."trips_table@v1";
 commit_num |        fare        |      begin_lon       |      begin_lat       
------------+--------------------+----------------------+----------------------
 update1    | 34.158284716382845 |  0.46157858450465483 |   0.4726905879569653 
 update1    |   43.4923811219014 |   0.8779402295427752 |   0.6100070562136587 
 update1    |  64.27696295884016 |   0.4923479652912024 |   0.5731835407930634 
 update1    |  93.56018115236618 |  0.14285051259466197 |  0.21624150367601136 
 update1    | 17.851135255091155 |   0.5644092139040959 |     0.40613510977307 
 update1    |  33.92216483948643 |   0.9694586417848392 |   0.1856488085068272 
 update1    |  66.62084366450246 |  0.03844104444445928 |   0.0750588760043035 
 update1    |  41.06290929046368 |   0.8192868687714224 |    0.651058505660742 
 update1    |  27.79478688582596 |   0.6273212202489661 |  0.11488393157088261 
(10 rows)
```

Here we see the data only from our first commit, which was the original creation of the table.

Similar to Iceberg, you can also query snapshots by timestamp as well. To make this query, you'll have to choose a time between the first and second commit to the table. One easy way to determine this is by looking at the Minio UI. Look at the time when the `0000000000000.json` file was created in your local time. Convert this to 12 hours time, and then also add or subtract an offset to determine the GMT time of this timestamp. So, for example, I created my table at 5:43 pm CDT. This means that I created my table at 17:43 CDT and there is a -5 hours offset between CDT and GMT, which means my final timestamp is 22:43 GMT. This means I will make the following query:

```sql
select commit_num, fare, begin_lon, begin_lat from delta.default."trips_table@t2025-08-27 22:45";
```

For example:

```sh
presto> select commit_num, fare, begin_lon, begin_lat from delta.default."trips_table@t2025-08-27 22:45";
 commit_num |        fare        |      begin_lon      |      begin_lat      
------------+--------------------+---------------------+---------------------
 update1    | 34.158284716382845 | 0.46157858450465483 |  0.4726905879569653 
 update1    |   43.4923811219014 |  0.8779402295427752 |  0.6100070562136587 
 update1    |  64.27696295884016 |  0.4923479652912024 |  0.5731835407930634 
 update1    |  93.56018115236618 | 0.14285051259466197 | 0.21624150367601136 
 update1    | 17.851135255091155 |  0.5644092139040959 |    0.40613510977307 
 update1    |  33.92216483948643 |  0.9694586417848392 |  0.1856488085068272 
 update1    |  66.62084366450246 | 0.03844104444445928 |  0.0750588760043035 
 update1    |  41.06290929046368 |  0.8192868687714224 |   0.651058505660742 
 update1    |  27.79478688582596 |  0.6273212202489661 | 0.11488393157088261 
 update1    | 19.179139106643607 |  0.7528268153249502 |  0.8742041526408587 
(10 rows)
```

We can once again see that we're only given the data from our first commit.

We can also look in the MinIO UI again to see the different files that have been created. Notice in the `_delta_log` path that we have two `json` metadata files, the name of which corresponds to the snapshot number.

From here, you can experiment with adding data to our table and exploring how the queries and s3 storage files change.

### Shutdown

When you're all done with the labs, to clean up your environment you can do these steps:

In the `spark-shell` terminal, to exit the scala prompt, you enter `ctrl-c`

In the `presto-cli` terminal, to exit the presto prompt, you enter `ctrl-d`

Then, to stop all your running Docker/Podman containers, you issue:

```sh
docker compose down -v
```

!!! note
    Note: you need to be in the src or the src/conf folders while issuing the docker compose

This command will stop all containers and remove the volumes.
