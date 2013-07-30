object TypeClasses {
  import shapeless._

  class TypeClass[X]
  implicit object TNilTypeClass extends TypeClass[HNil];import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(213); 
  implicit def combinedTypeClass[X: TypeClass, Y <: HList: TypeClass] = new TypeClass[X :: Y];System.out.println("""combinedTypeClass: [X, Y <: shapeless.HList](implicit evidence$3: TypeClasses.TypeClass[X], implicit evidence$4: TypeClasses.TypeClass[Y])TypeClasses.TypeClass[shapeless.::[X,Y]]""");$skip(72); 
  implicit def optionTypeClass[X: TypeClass] = new TypeClass[Option[X]]
  implicit object IntTypeClass extends TypeClass[Int];System.out.println("""optionTypeClass: [X](implicit evidence$5: TypeClasses.TypeClass[X])TypeClasses.TypeClass[Option[X]]""");$skip(123); 

  def f[X: TypeClass] = {
    println(implicitly[TypeClass[X]])
  }

  class F[X: TypeClass] {
    println(implicitly[TypeClass[X]])
  };System.out.println("""f: [X](implicit evidence$6: TypeClasses.TypeClass[X])Unit""");$skip(87); val res$0 = 

  f[Int :: HNil];System.out.println("""res0: <error> = """ + $show(res$0));$skip(21); val res$1 = 
  new F[Int :: HNil];System.out.println("""res1: <error> = """ + $show(res$1))}
}
