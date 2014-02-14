package uk.me.jamespic.dougng.viewmodel

import com.orientechnologies.orient.`object`.db.OObjectDatabaseTx
import scala.collection.JavaConversions._
import com.dongxiguo.fastring.Fastring
import Fastring.Implicits._
import scala.util.parsing.json.JSONFormat.quoteString
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerJSON
import com.orientechnologies.orient.core.record.impl.ODocument
import scala.util.Try

/**
 * Custom Json serializer (note: NOT A DESERIALIZER). This approach made sense,
 * as it would have been tricky to integrate OrientDB's JSON support into
 * an off-the-shelf JSON library. We only support serialization, because
 * we only need this kind of general serialization support in view updates, which
 * are one-way. Controller interactions would always involve OrientDB documents,
 * so we can let OrientDB handle serialization and deserialization.
 */
class JsonSerializer(db: OObjectDatabaseTx) {
  private lazy val entityClasses = db.getEntityManager.getRegisteredEntities.toSet
  private lazy val orientSerializer = new ORecordSerializerJSON
  def dumps(obj: Any) = {
    dumpFastring(obj).toString
  }
  private def dumpFastring(obj: Any): Fastring = obj match {
    case null => fast"null"
    case x: String => fast"${'"'}${quoteString(x)}${'"'}"
    case _:Number|_:Int|_:Long|_:Double|_:Float|_:Short|_:Boolean => fast"$obj"
    case x: scala.collection.Map[_,_] =>
      fast"{${
        (for ((key, value) <- x) yield fast"${dumpFastring(key)}: ${dumpFastring(value)}") mkFastring ", "
      }}"
    case x: scala.collection.Traversable[_] =>
      fast"[${x map dumpFastring mkFastring ", "}]"
    case x: Array[_] =>
      fast"[${x map dumpFastring mkFastring ", "}]"
    case x: ODocument =>
      Fastring(orientSerializer.toString(x, "indent:0,fetchPlan:*:-1,rid,version,class,keepTypes").toString)
    case x: Product => // Case classes
      //TODO: Specialised case classes (with name mangling) aren't currently supported
      val clazz = x.getClass
      fast"{${
        (fast"${'"'}class${'"'}: ${dumpFastring(clazz.getSimpleName())}") +:
        (for {field <- clazz.getDeclaredFields
              name = field.getName
              accessor <- Try(clazz.getMethod(name)).toOption}
        yield fast"${dumpFastring(name)}: ${dumpFastring(accessor.invoke(x))}") mkFastring ", "
      }}"
    case x if entityClasses.exists(_ isInstance x) =>
      dumpFastring(db.getRecordByUserObject(x, false))
  }
}