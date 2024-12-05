ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"
val sparkVersion = "3.5.1"
val postgresVersion = "42.6.0"
val log4jVersion = "2.23.1"

lazy val root = (project in file("."))
  .settings(
    name := "spark-whatsapp"
  )
libraryDependencies ++= Seq(
  // spark
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // logging
  "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
  // webP images
  "com.twelvemonkeys.imageio" % "imageio-webp" % "3.7.0",
  "commons-codec" % "commons-codec" % "1.17.0" // For MD5 hashing
)