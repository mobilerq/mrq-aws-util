import sbt._

object Dependencies {
  lazy val awsVersion = "1.11.76"

  lazy val awsJavaSdkS3 = "com.amazonaws" % "aws-java-sdk-s3" % awsVersion
  lazy val awsJavaSdkDynamodb = "com.amazonaws" % "aws-java-sdk-dynamodb" % awsVersion

  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.1"
  lazy val junit = "junit" % "junit" % "4.12"
  lazy val mockito = "org.mockito" % "mockito-core" % "2.6.2"
  lazy val junitInterface = "com.novocode" % "junit-interface" % "0.11"

  val testDeps = Seq(junit % Test, mockito % Test, junitInterface % Test)
}