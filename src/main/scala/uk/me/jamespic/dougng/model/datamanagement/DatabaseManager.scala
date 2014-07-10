package uk.me.jamespic.dougng.model.datamanagement

import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes

import akka.actor.{ActorRef, ActorSystem}
import java.io.{IOException, File, InputStream}
import scala.collection.breakOut
import scala.concurrent.ExecutionContext
import scala.util.Try

object DatabaseManager {
  def inMemory()(implicit system: ActorSystem) = DatabaseManager(system, name => s"memory:$name", Map.empty)

  def inDirectory(path: Path)(implicit system: ActorSystem) = {
    val absPath = path.toAbsolutePath
    val file = path.toFile
    require(Files.isDirectory(path))

    val urlFactory = (name: String) => "plocal:" + absPath.resolve(name).toString.replaceAllLiterally("\\", "/")

    val existingNames = for (subFile <- file.listFiles if subFile.isDirectory) yield {
      val subPath = subFile.toPath
      subPath.getName(subPath.getNameCount - 1).toString
    }

    val members: Map[String, ActorRef] = existingNames.map{name =>
      val url = urlFactory(name)
      name -> system.actorOf(Database.openExisting(url))
    }(breakOut)

    DatabaseManager(system, urlFactory, members)
  }

  def deleteDirectory(path: Path): Unit = {
    Files.walkFileTree(path, new SimpleFileVisitor[Path] {
      override def visitFile(file: Path, attrs: BasicFileAttributes) = {
        Files.delete(file)
        FileVisitResult.CONTINUE
      }

      override def visitFileFailed(file: Path, exc: IOException): FileVisitResult = {
        Files.delete(file)
        FileVisitResult.CONTINUE
      }

      override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = exc match {
        case null =>
          Files.delete(dir)
          FileVisitResult.CONTINUE
        case ex => throw ex
      }
    })
  }
}

case class DatabaseManager private(system: ActorSystem, urlFactory: String => String, members: Map[String, ActorRef]) {
  def create(name: String) = {
    val newActor = system.actorOf(Database.createEmpty(urlFactory(name)))
    this.copy(members = members + (name -> newActor))
  }

  def importData(name: String, input: InputStream) = {
    val newActor = system.actorOf(Database.importData(urlFactory(name), input))
    this.copy(members = members + (name -> newActor))
  }

  def delete(name: String)(implicit ec: ExecutionContext) = {
    CloseTask.closeDB(members(name))(system) onSuccess {
      case () =>
        val uri = urlFactory(name)
        if (uri.startsWith("plocal:")) {
          new File(uri.stripPrefix("plocal:")).delete()
        }
    }
  }
}
