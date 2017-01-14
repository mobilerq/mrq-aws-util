import Dependencies._
import sbt.Keys._

lazy val root = (project in file(".")).
  aggregate(dynamodb, s3).
  settings(
    inThisBuild(Seq(
      organization := "com.mobilerq",
      version := "0.1.0-SNAPSHOT",
      scalaVersion := "2.12.1",
      crossScalaVersions := Seq("2.12.1", "2.11.8", "2.10.6")
    )),
    name := "aws-util"
  )

lazy val dynamodb = project.
  settings(
    name := "aws-util-dynamodb",
    libraryDependencies ++= testDeps :+ awsJavaSdkDynamodb
  )

lazy val s3 = project.
  settings(
    name := "aws-util-s3",
    libraryDependencies ++= testDeps :+ awsJavaSdkS3
  )