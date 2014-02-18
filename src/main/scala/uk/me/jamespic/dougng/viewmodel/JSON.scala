package uk.me.jamespic.dougng.viewmodel

import com.orientechnologies.orient.`object`.db.OObjectDatabaseTx
import scala.collection.JavaConversions._
import com.dongxiguo.fastring.Fastring
import Fastring.Implicits._
import scala.util.parsing.json.JSONFormat.quoteString
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerJSON
import com.orientechnologies.orient.core.record.impl.ODocument
import scala.util.Try
import akka.actor.ActorRef
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone, Calendar}

/**
 * Custom Json serializer (note: NOT A DESERIALIZER). This approach made sense,
 * as it would have been tricky to integrate OrientDB's JSON support into
 * an off-the-shelf JSON library. We only support serialization, because
 * we only need this kind of general serialization support in view updates, which
 * are one-way. Controller interactions would always involve OrientDB documents,
 * so we can let OrientDB handle serialization and deserialization.
 */
object JsonSerializer {
  private val Specialised = "([^\\$]+)\\$\\w+\\$sp".r
  private val dateFormatLocal = new ThreadLocal[SimpleDateFormat] {
    override def initialValue ={
      val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX")
      df.setTimeZone(TimeZone.getTimeZone("UTC"))
      df
    }
  }
  private def dateFormat = dateFormatLocal.get
}
class JsonSerializer(db: OObjectDatabaseTx) {
  import JsonSerializer._
  private lazy val entityClasses = db.getEntityManager.getRegisteredEntities.toSet
  private lazy val orientSerializer = new ORecordSerializerJSON
  def dumps(obj: Any) = {
    dumpFastring(obj).toString
  }
  private def deSpecialise(name: String) = name match {
    case Specialised(n) => n
    case n => n
  }
  private def dumpFastring(obj: Any): Fastring = obj match {
    case null => fast"null"
    case x: String => fast"${'"'}${quoteString(x)}${'"'}"
    case _:Number|_:Int|_:Long|_:Double|_:Float|_:Short|_:Boolean => fast"$obj"
    case x: Date =>
      fast"""{"class": "Date", "value": ${dumpFastring(dateFormat.format(x))}}"""
    case x: Calendar =>
      val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX")
      formatter.setTimeZone(x.getTimeZone)
      fast"""{"class": "Calendar", "value": ${dumpFastring(formatter.format(x.getTime))}}"""
    case x: scala.collection.Map[_,_] =>
      fast"{${
        (for ((key, value) <- x) yield fast"${dumpFastring(key)}: ${dumpFastring(value)}") mkFastring ", "
      }}"
    case x: scala.collection.TraversableOnce[_] =>
      fast"[${x map dumpFastring mkFastring ", "}]"
    case x: Array[_] =>
      fast"[${x map dumpFastring mkFastring ", "}]"
    case x: ODocument =>
      Fastring(orientSerializer.toString(x, "indent:0,fetchPlan:*:-1,rid,version,class,keepTypes").toString)
    case x: ActorRef =>
      fast"""{"class": "ActorRef", "path":${dumpFastring(x.path.toSerializationFormat)}}"""
    case x: Product => // Case classes
      //TODO: Specialised case classes (with name mangling) aren't currently supported
      val clazz = x.getClass
      fast"{${
        (fast"${'"'}class${'"'}: ${dumpFastring(deSpecialise(clazz.getSimpleName()))}") +:
        (for {field <- clazz.getDeclaredFields
              name = field.getName
              accessor <- Try(clazz.getMethod(name)).toOption}
        yield fast"${dumpFastring(deSpecialise(name))}: ${dumpFastring(accessor.invoke(x))}") mkFastring ", "
      }}"
    case x if entityClasses.exists(_ isInstance x) =>
      dumpFastring(db.getRecordByUserObject(x, false))
  }
}