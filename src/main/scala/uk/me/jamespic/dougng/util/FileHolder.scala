package uk.me.jamespic.dougng.util

import java.io.{RandomAccessFile, File}
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import FileChannel.MapMode._

/**
 * Utility class to hold all state relevant to file. Simplifies synchronization and
 * garbage collection
 */
private[util] class FileHolder {
  var file: File = tryAndMaybeGc(File.createTempFile("data", ".dat"))
  var raf: RandomAccessFile = tryAndMaybeGc(new RandomAccessFile(file, "rw"))
  var channel: FileChannel = raf.getChannel()
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

  def close() = {
    if (channel != null) channel.close()
    if (raf != null) raf.close()
    if (file != null) file.delete()
    channel = null
    raf = null
    file = null
  }

  protected override def finalize = close()
}