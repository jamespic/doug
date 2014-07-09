import java.net.URL

import com.typesafe.sbt.web.Import.WebKeys

name := "DougNG"

EclipseKeys.withSource := true

organization := "uk.me.jamespic"

version := "0.1-SNAPSHOT"

scalaVersion := "2.10.4"

sbtVersion := "0.13.5"

resolvers ++= Seq(
  Opts.resolver.sonatypeReleases,
  Opts.resolver.sonatypeSnapshots,
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"
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
  //"com.typesafe.play" %% "play-iteratees" % "2.3.1"
)

libraryDependencies <++= (scalaVersion)(sv =>
    Seq("org.scala-lang" % "scala-reflect" % sv)
)

parallelExecution in Test := false

val bootstrapZip = SettingKey[String]("bootstrap-zip-url")

bootstrapZip := "https://github.com/twbs/bootstrap/releases/download/v3.2.0/bootstrap-3.2.0-dist.zip"

val managedWebRoot = SettingKey[File]("managed-web-root")

managedWebRoot := (resourceManaged in Compile).value / "web-root"

lazy val root = (project in file(".")).enablePlugins(SbtWeb)

(mappings in (Compile, packageBin)) ++= (mappings in Assets).value

def download(url: String, location: File) = {
  val dataStream = new URL(url).getContent().asInstanceOf[java.io.InputStream]
  val data = IO.readBytes(dataStream)
  IO.write(location, data)
  Seq(location)
}

(resourceGenerators in Compile) += Def.task {
  val dataStream = new URL(bootstrapZip.value).getContent().asInstanceOf[java.io.InputStream]
  val files = IO.unzipStream(dataStream, managedWebRoot.value).toSeq
  for (file <- files) yield {
    // Strip out zip-file root directory
    val relPath = file.relativeTo(managedWebRoot.value).get.toPath
    val newPath = managedWebRoot.value / relPath.subpath(1, relPath.getNameCount).toString
    IO.move(file, newPath)
    newPath
  }
}.taskValue

