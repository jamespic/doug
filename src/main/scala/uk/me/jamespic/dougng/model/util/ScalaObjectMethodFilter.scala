package uk.me.jamespic.dougng.model.util
import java.lang.reflect.Method
import com.orientechnologies.orient.`object`.enhancement.{OObjectMethodFilter, OObjectEntitySerializer}

object ScalaObjectMethodFilter extends OObjectMethodFilter {
  private val SetterRegex = "(.*)_\\$eq".r

  override def getFieldName(m: Method) = {
    m.getName() match {
      case SetterRegex(fieldName) => fieldName
      case fieldName => fieldName
    }
  }

  override def isSetterMethod(methodName: String, m: Method) = {
    val pTypes = m.getParameterTypes()
    (methodName == m.getName()) &&
    !SetterRegex.findAllIn(methodName).isEmpty &&
    (pTypes != null) &&
      (pTypes.length == 1) &&
      (!OObjectEntitySerializer.isTransientField(m.getDeclaringClass(), methodName))
  }

  override def isGetterMethod(methodName: String, m: Method) = {
    val pTypes = m.getParameterTypes()
    (methodName == m.getName()) &&
    (pTypes == null) ||
      (pTypes.length == 0) &&
      (!OObjectEntitySerializer.isTransientField(m.getDeclaringClass(), methodName))
  }
}