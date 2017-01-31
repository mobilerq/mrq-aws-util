import Dependencies._
import sbt.Keys._

lazy val nexusRepoUrl = sys.env.get("NEXUS_URL")
lazy val nexusRepoCredentials = sys.env.get("NEXUS_CREDENTIALS").map(path ⇒ Credentials(new File(path))).toSeq

lazy val root = (project in file(".")).
  aggregate(dynamodb, s3, kinesis).
  settings(
    inThisBuild(Seq(
      organization := "com.mobilerq",
      version := "0.1.0-SNAPSHOT",
      scalaVersion := "2.12.1",
      crossScalaVersions := Seq("2.12.1", "2.11.8", "2.10.6"),
      autoAPIMappings := true,
      publishTo := nexusRepoUrl.map(nexus ⇒ "snapshots" at nexus + "/content/repositories/snapshots"),
      credentials ++= nexusRepoCredentials
    )),
    name := "mrq-aws-util"
  )

lazy val dynamodb = project.
  settings(
    name := "mrq-aws-util-dynamodb",
    libraryDependencies ++= testDeps :+ awsJavaSdkDynamodb
  )

lazy val s3 = project.
  settings(
    name := "mrq-aws-util-s3",
    libraryDependencies ++= testDeps :+ awsJavaSdkS3
  )

lazy val kinesis = project.
  settings(
    name := "mrq-aws-util-kinesis",
    libraryDependencies ++= (testDeps ++ Seq(awsJavaSdkKinesis, awsKinesisClient, awsKinesisProducer))
  )