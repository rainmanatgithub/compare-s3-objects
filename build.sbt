lazy val commonSettings = Seq(
  name := "SkygateSparkApps",
  version := "1.0",
  scalaVersion := "2.11.12",
  test in assembly := {}
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    // mainClass in assembly := Some("org.skygate.falcon.S3Differ.Main")
  )

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

val sparkVersion = "2.3.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "com.amazonaws" % "aws-java-sdk" % "1.11.563" % "provided",
  "org.rogach" %% "scallop" % "3.3.0",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "mysql" % "mysql-connector-java" % "5.1.6",
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-csv" % "2.9.0",
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.9.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.0"
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case other => MergeStrategy.defaultMergeStrategy(other)
}

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.mysql.**" -> "shaded.@0").inAll,
  ShadeRule.rename("com.fasterxml.jackson.**" -> "shaded.@0").inAll
)