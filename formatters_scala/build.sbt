ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

//resolvers += "Apache HBase" at "https://repository.apache.org/content/repositories/releases"
//resolvers += "Thirft" at "https://people.apache.org/~rawson/repo/"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-sql_2.11" % "2.0.1" excludeAll(ExclusionRule(organization="joda-time"), ExclusionRule(organization="org.slf4j"), ExclusionRule(organization="com.sun.jersey.jersey-test-framework"), ExclusionRule(organization="org.apache.hadoop")),
  "org.apache.spark" % "spark-core_2.11" % "2.0.1" excludeAll(ExclusionRule(organization="joda-time"), ExclusionRule(organization="org.slf4j"), ExclusionRule(organization="com.sun.jersey.jersey-test-framework"), ExclusionRule(organization="org.apache.hadoop")),
  "it.nerdammer.bigdata" % "spark-hbase-connector_2.10" % "1.0.3",
  "org.apache.hadoop" % "hadoop-client" % "2.7.0" excludeAll(ExclusionRule(organization="joda-time"), ExclusionRule(organization="org.slf4j")),
  "org.apache.commons" % "commons-exec" % "1.3",
  "com.typesafe" % "config" % "1.3.0",
  "org.slf4j" % "slf4j-api" % "1.7.7"
)

//ThisBuild / assemblyMergeStrategy := {
//  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
//  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
//  case "application.conf"                            => MergeStrategy.concat
//  case "unwanted.txt"                                => MergeStrategy.discard
//  case x =>
//    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
//    oldStrategy(x)
//}

lazy val root = (project in file("."))
  .settings(
    name := "BDM_2S",
//    assembly / assemblyJarName := "bdm2.jar",
  )
