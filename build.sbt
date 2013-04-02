name := "DougNG"

EclipseKeys.withSource := true

version := "0.1"

scalaVersion := "2.10.1"

libraryDependencies ++=  Seq("com.orientechnologies" % "orientdb-core" % "1.3.0",
                             "com.orientechnologies" % "orientdb-object" % "1.3.0",
                             "com.orientechnologies" % "orient-commons" % "1.3.0",
                             "com.orientechnologies" % "orientdb-server" % "1.3.0",
                             "org.scalatest" % "scalatest_2.10" % "2.0.M5b" % "test",
                             "com.typesafe.akka" %% "akka-actor" % "2.1.0"
                             )

