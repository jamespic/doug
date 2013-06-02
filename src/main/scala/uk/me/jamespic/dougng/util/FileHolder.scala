package uk.me.jamespic.dougng.util

import java.io.{RandomAccessFile, File}
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import FileChannel.MapMode._

/**
 * Utility class to hold all state relevant to file. Simplifies synchronization and
 * garbage collection
 */
private[util] class FileHolder(initialWindow: Long) extends Hibernatable {
  var file: File = tryAndMaybeGc(File.createTempFile("data", ".dat"))
  var raf: RandomAccessFile = tryAndMaybeGc(new RandomAccessFile(file, "rw"))
  var channel: FileChannel = raf.getChannel()
  var currentMap: MappedByteBuffer = channel.map(READ_WRITE, 0L, initialWindow)
  var hibernating = false

  file.deleteOnExit()

  /**
   * Try to run a command. If it fails due to an IOException, perform GC and retry.
   */
  private def tryAndMaybeGc[A](f: => A) = {
    try {
      f
    } catch {
      case _: java.io.IOException =>
        System.gc()
        f
    }
  }

  def hibernate() = synchronized {
    if (!hibernating) {
      currentMap = null
      if (channel != null) channel.close()
      if (raf != null) raf.close()
      channel = null
      raf = null
      hibernating = true
    }
  }

  def unhibernate() = synchronized {
    if (hibernating) {
      raf = tryAndMaybeGc(new RandomAccessFile(file, "rw"))
      channel = raf.getChannel()
      hibernating = false
    }
  }

  def close() = synchronized {
    currentMap = null
    if (channel != null) channel.close()
    if (raf != null) raf.close()
    if (file != null) file.delete()
    channel = null
    raf = null
    file = null
  }

  protected override def finalize = close()
}