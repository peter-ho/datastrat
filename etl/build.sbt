lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.datastrat",
      version := "1.0.0",
      scalaVersion := "2.11.8"
    )),
    name := "data-strat",
    libraryDependencies ++= Seq("org.scalatest" %% "scalatest" % "3.0.5" % Test,
      "org.apache.spark" %% "spark-core" % "2.2.0" % Provided,
      "org.apache.spark" %% "spark-sql" % "2.2.0" % Provided,
      "org.apache.spark" %% "spark-hive" % "2.2.0" % Provided,
      "org.apache.spark" %% "spark-catalyst" % "2.2.0" % Provided,
      "org.scala-lang" % "scala-reflect" % scalaVersion.value
    )
  )

//lazy val impalaJdbcVersion="2.5.37"


