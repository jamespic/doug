name := "DougNG"

EclipseKeys.withSource := true

organization := "uk.me.jamespic"

version := "0.1-SNAPSHOT"

scalaVersion := "2.10.4"

resolvers ++= Seq(
  "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases/",
  "Sonatype OSS Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/"
)

libraryDependencies ++=  Seq(
  "com.orientechnologies" % "orientdb-core" % "1.7.3",
  "com.orientechnologies" % "orientdb-object" % "1.7.3",
  "com.orientechnologies" % "orient-commons" % "1.7.3",
  "com.orientechnologies" % "orientdb-server" % "1.7.3",
  "org.scalatest" % "scalatest_2.10" % "2.2.0" % "test",
  "com.typesafe.akka" %% "akka-actor" % "2.3.3",
  "com.typesafe.akka" %% "akka-testkit" % "2.3.3",
  "com.chuusai" % "shapeless" % "2.0.0" cross CrossVersion.full,
  "org.apache.commons" % "commons-math3" % "3.3",
  "com.dongxiguo" %% "fastring" % "0.2.4"
)

libraryDependencies <++= (scalaVersion)(sv =>
    Seq("org.scala-lang" % "scala-reflect" % sv)
)
