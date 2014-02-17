name := "DougNG"

EclipseKeys.withSource := true

organization := "uk.me.jamespic"

version := "0.1-SNAPSHOT"

scalaVersion := "2.10.3"

resolvers ++= Seq(
  "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases/",
  "Sonatype OSS Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/"
)

libraryDependencies ++=  Seq(
  "com.orientechnologies" % "orientdb-core" % "1.6.4",
  "com.orientechnologies" % "orientdb-object" % "1.6.4",
  "com.orientechnologies" % "orient-commons" % "1.6.4",
  "com.orientechnologies" % "orientdb-server" % "1.6.4",
  "org.scalatest" % "scalatest_2.10" % "2.0" % "test",
  "com.typesafe.akka" %% "akka-actor" % "2.3.0-RC1",
  "com.typesafe.akka" %% "akka-testkit" % "2.3.0-RC1",
  "com.chuusai" %% "shapeless" % "1.2.4",
  "org.apache.commons" % "commons-math3" % "3.2",
  "com.dongxiguo" %% "fastring" % "0.2.2"
)

libraryDependencies <++= (scalaVersion)(sv =>
    Seq("org.scala-lang" % "scala-reflect" % sv)
)
