lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.datastrat",
      version := "1.0.0",
      scalaVersion := "2.11.12"
    )),
    name := "data-strat",
    libraryDependencies ++= Seq("org.scalatest" %% "scalatest" % "3.0.5" % Test,
      "org.apache.spark" %% "spark-core" % "2.4.0" % Provided,
      "org.apache.spark" %% "spark-sql" % "2.4.0" % Provided,
      "org.apache.spark" %% "spark-hive" % "2.4.0" % Provided,
      "org.apache.spark" %% "spark-catalyst" % "2.4.0" % Provided,
      "org.scala-lang" % "scala-reflect" % scalaVersion.value
    )
  )

//lazy val impalaJdbcVersion="2.5.37"


