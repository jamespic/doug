package uk.me.jamespic.dougng.util

import shapeless.Iso
import java.nio.channels.FileChannel.MapMode._
import java.nio.ByteBuffer

class ChannelDiskRecordSet[A](extentSize: Int = AbstractDiskRecordSet.DefaultExtentSize)
    (implicit ser: Serializer[A]) extends AbstractDiskRecordSet[A](extentSize) {
  val file = new FileHolder(0)
  val buf = ByteBuffer.allocateDirect(ser.size)

  protected def read(idx: Long) = {
    buf.clear()
    file.channel.read(buf, idx)
    buf.rewind()
    ser.deserialize(buf)
  }

  protected def write(idx: Long, value: A) = {
    buf.clear()
    ser.serialize(value, buf)
    buf.rewind()
    file.channel.write(buf, idx)
  }

  override def toString = s"ChannelDiskRecordSet(${file.file})"
  def hibernate() = file.hibernate()
  override def sync[B](f: => B) = file.synchronized(f)
}