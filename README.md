The examples in this repository are support to the **[Spark in Action, 2nd edition](http://jgp.net/sia)** book by Jean-Georges Perrin and published by Manning. Find out more about the book on [Manning's website](http://jgp.net/sia).

# Spark in Action, 2nd edition – Java, Python, and Scala code for chapter 11

Welcome to Spark in Action, 2nd edition, chapter 11. This chapter is about working with data using Apache Spark SQL.

The dataset used in this chapter is Data.gov's [Population by Country (1980 - 2010)](https://catalog.data.gov/dataset/population-by-country-1980-2010)

This code is designed to work with Apache Spark v3.1.2.

## Labs

Each chapter has one or more labs. Labs are examples used for teaching in the [book](https://www.manning.com/books/spark-in-action-second-edition?a_aid=jgp). You are encouraged to take ownership of the code and modify it, experiment with it, hence the use of the term **lab**.
 
### Lab \#100

The `SimpleSelectApp` application does the following:

1.	It acquires a session (a `SparkSession`).
2.	It asks Spark to load (ingest) a dataset in CSV format.
3.	It demonstrates Spark SQL's select on ingested data.

### Lab \#200

TBD

### Lab \#201

TBD

### Lab \#300

TBD

### Lab \#400

TBD

## Running the lab in Java

For information on running the Java lab, see chapter 11 in [Spark in Action, 2nd edition](http://jgp.net/sia).

## Running the lab using PySpark

Prerequisites:

You will need:
 * `git`.
 * Apache Spark (please refer Appendix P - "Spark in production: installation and a few tips").

1. Clone this project

```
git clone https://github.com/jgperrin/net.jgp.books.spark.ch11
```

2. Go to the lab in the Python directory

```
cd net.jgp.books.spark.ch11/src/main/python/lab100_simple_select/
```

3. Execute the following spark-submit command to create a jar file to our this application

```
spark-submit simpleSelectApp.py
```

## Running the lab in Scala

Prerequisites:

You will need:
 * `git`.
 * Apache Spark (please refer Appendix P - "Spark in production: installation and a few tips"). 

1. Clone this project

```
git clone https://github.com/jgperrin/net.jgp.books.spark.ch11
```

2. Go to the lab directory

```
cd net.jgp.books.spark.ch11
```

3. Package application using the `sbt` command

```
sbt clean assembly
```

4. Run Spark/Scala application using spark-submit command as shown below:

```
spark-submit --class net.jgp.books.spark.ch11.lab100_simple_select.SimpleSelectScalaApp target/scala-2.11/SparkInAction2-Chapter11-assembly-1.0.0.jar
```

## News

 1. [2020-06-13] Updated the `pom.xml` to support Apache Spark v3.1.2. 
 1. [2020-06-13] As we celebrate the first anniversary of Spark in Action, 2nd edition is the best-rated Apache Spark book on [Amazon](https://amzn.to/2TPnmOv). 
 
## Notes

 1. [Java] Due to renaming the packages to match more closely Java standards, this project is not in sync with the book's MEAP prior to v10 (published in April 2019).
 1. [Scala, Python] As of MEAP v14, we have introduced Scala and Python examples (published in October 2019).
 1. The master branch contains the last version of the code running against the latest supported version of Apache Spark. Look in specifics branches for specific versions.

---

Follow me on Twitter to get updates about the book and Apache Spark: [@jgperrin](https://twitter.com/jgperrin). Join the book's community on [Facebook](https://fb.com/SparkInAction/) or in [Manning's community site](https://forums.manning.com/forums/spark-in-action-second-edition?a_aid=jgp).
