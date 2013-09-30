name := "DougNG"

EclipseKeys.withSource := true

version := "0.1"

scalaVersion := "2.10.1"

resolvers ++= Seq(
  "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases/",
  "Sonatype OSS Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/"
)

libraryDependencies ++=  Seq(
  "com.orientechnologies" % "orientdb-core" % "1.5.1",
  "com.orientechnologies" % "orientdb-object" % "1.5.1",
  "com.orientechnologies" % "orient-commons" % "1.5.1",
  "com.orientechnologies" % "orientdb-server" % "1.5.1",
  "org.scalatest" % "scalatest_2.10" % "2.0.M5b" % "test",
  "com.typesafe.akka" %% "akka-actor" % "2.1.0",
  "com.chuusai" %% "shapeless" % "1.2.4",
  "org.scalaz" %% "scalaz-core" % "7.0.1",
  "org.apache.commons" % "commons-math3" % "3.2"
)

libraryDependencies <++= (scalaVersion)(sv =>
    Seq("org.scala-lang" % "scala-reflect" % sv)
)