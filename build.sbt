name := "TwitterStreaming"

version := "0.1"

scalaVersion := "2.12.11"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.1"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.1"
libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.4.0"
libraryDependencies += "org.elasticsearch" % "elasticsearch-spark_2.10" % "2.2.0-m1" % "compile"
libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "3.0.3"
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.5.1"

libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.5.1" classifier "models"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "v3.0.0-alpha2"

libraryDependencies+="com.datastax.cassandra" % "cassandra-driver-core" % "3.3.0"
libraryDependencies+="org.mongodb.spark" %% "mongo-spark-connector" % "2.4.1"



