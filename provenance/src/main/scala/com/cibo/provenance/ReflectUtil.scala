package com.cibo.provenance

import com.typesafe.scalalogging.LazyLogging

import scala.reflect.ClassTag
import scala.util.{Failure, Success}


class ReflectionException(msg: String) extends RuntimeException(msg)

object ReflectUtil extends LazyLogging {
  import scala.reflect.runtime.universe._
  import scala.reflect.runtime.currentMirror
  import scala.tools.reflect.ToolBox
  import scala.language.existentials
  import scala.language.reflectiveCalls
  import scala.util.Try

  def classToName[T](implicit ct: ClassTag[T]) = {
    val name1 = ct.toString
    Try(Class.forName(name1)) match {
      case Success(clazz1) => name1
      case Failure(_) =>
        val name2 = "scala." + name1
        Try(Class.forName(name2)) match {
          case Success(clazz2) => name2
          case Failure(_) =>
            throw new RuntimeException(f"Failed to resolve a class name for $ct")
        }
    }
  }

  //import Implicits._
  import io.circe._

  def getSymbolName[T: TypeTag](tt: TypeTag[T]) = typeOf[T].typeSymbol.fullName

  def functionWithProvenanceToName[O, T <: FunctionWithProvenance[O] : TypeTag : ClassTag](obj: T) = {
    val cz = obj.getClass
    val name = Try { cz.getName.stripSuffix("$") }.getOrElse {
      throw new ReflectionException(
        "A FunctionWithProvenance should either be a singleton object, "+
        " or have a companion object that can determine the type-specific name of any instance!"
      )
    }
    val ct: ClassTag[T] = implicitly[ClassTag[T]]
    val tt: TypeTag[T] = implicitly[TypeTag[T]]
    val sn = getSymbolName[T](tt)
    val tnd = cz.getTypeName
    val tp = cz.getTypeParameters
    if (tp.isEmpty)
      name
    else
      logger.info("A FunctionWithProvenance that is not a singleton should override def name to include type parameters!")
      name
  }

  def functionWithProvenanceBaseName[O, T <: FunctionWithProvenance[O] : TypeTag](obj: T) = {
    val cz = obj.getClass
    cz.getName.stripSuffix("$")
  }

  def getFunctionByName(
    name: String,
    typeParamTypes: List[String] = List.empty
  ): FunctionWithProvenance[_] = {
    try {
      // This will work if the function is a singleton.
      val clazz = Class.forName(name + "$")
      val obj = clazz.getField("MODULE$").get(clazz)
      obj.asInstanceOf[FunctionWithProvenance[_]]
    } catch {
      case e: Exception if e.isInstanceOf[java.lang.ClassCastException] | e.isInstanceOf[java.lang.ClassNotFoundException] =>
        // Fall back to reflection only for parameterized FunctionWithProvenance instances
        if (!name.contains("[")) {
          throw new RuntimeException(
            f"The `name` method for FunctionWithProvenance $name must be overridden when it is not a singleton!" +
            "See MapWithProvenance for an example."
          )
        }
        val src = "new " + name
        val tree = toolbox.parse(src)
        val fwp = toolbox.compile(tree).apply()
        fwp.asInstanceOf[FunctionWithProvenance[_]]
    }
  }

  val toolbox = currentMirror.mkToolBox()

  /*

  def createTypeTag(tp: String): TypeTag[_] = {
    val ttagCall = s"scala.reflect.runtime.universe.typeTag[$tp]"
    val tpe = toolbox.typecheck(toolbox.parse(ttagCall), toolbox.TYPEmode).tpe.resultType.typeArgs.head
    val mirror = currentMirror

    TypeTag(currentMirror, new reflect.api.TypeCreator {
      def apply[U <: reflect.api.Universe with Singleton](m: reflect.api.Mirror[U]) = {
        assert(m eq mirror, s"TypeTag[$tpe] defined in $mirror cannot be migrated to $m.")
        tpe.asInstanceOf[U#Type]
      }
    })
  }

  def getClassSymbolForName(name: String): ClassSymbol = {
    getClassSymbol(Class.forName(name))
  }

  //def getTypeTagForName(name: String): TypeTag[_] = {
  //  val sym = getClassSymbolForName(name)
  //}

  def getType[T : TypeTag]: Type = {
    val tt = implicitly[TypeTag[T]]
    tt.tpe
  }

  def getClassSymbol[T : TypeTag](clazz: Class[T]): ClassSymbol =
    getClassSymbol[T]

  def getClassSymbol[T : TypeTag]: ClassSymbol = {
    val classRuntime: Class[_] = getClassRuntime[T]
    getTypeMirror[T].classSymbol(classRuntime)
  }

  def getTypeMirror[T : TypeTag]: Mirror =
    typeTag[T].mirror

  def getClassRuntime[T : TypeTag]: Class[T] =
    getTypeMirror.runtimeClass(typeOf[T]).asInstanceOf[Class[T]]

  def getClassTag[T : TypeTag]: ClassTag[T] =
    ClassTag(getClassRuntime[T])

  def getClassMirror[T : TypeTag]: Mirror = {
    val runtimeClass = getClassRuntime[T]
    val classMirror = runtimeMirror(runtimeClass.getClassLoader)
    classMirror
  }

  def getInstanceMirror[T : TypeTag](obj: T): InstanceMirror = {
    val runtimeClass = getClassRuntime[T]
    val classTag = ClassTag[T](runtimeClass)
    val classMirror = runtimeMirror(runtimeClass.getClassLoader)
    classMirror.reflect(obj)(classTag)
  }

  def getTypeMethodSymbol[T : TypeTag](methodName: String): MethodSymbol = {
    getType[T].decl(TermName(methodName)).asMethod
  }

  def getObjectMethodMirror[T : TypeTag](obj: T, methodName: String): MethodMirror = {
    implicit val classTag = getClassTag[T]
    getInstanceMirror(obj).reflectMethod(getTypeMethodSymbol[T](methodName))
  }

  def getObjectMethodMirror[T : TypeTag](obj: T, methodSymbol: MethodSymbol): MethodMirror = {
    getInstanceMirror(obj).reflectMethod(methodSymbol)
  }

  def callMethod[T : TypeTag](obj: T, methodSymbol: MethodSymbol): Any =
    getObjectMethodMirror(obj, methodSymbol).apply(Seq.empty)

  def callMethod[T : TypeTag](obj: T, methodSymbol: MethodSymbol, args: Seq[Any]): Any = {
    getObjectMethodMirror(obj, methodSymbol).apply(args)
  }

  def findEncoderMethod[T : TypeTag]: Option[Symbol] =
    findCompanionMethodByImplicitReturnType[T, Encoder[_]]

  def findDecoderMethod[T : TypeTag]: Option[Symbol] =
    findCompanionMethodByImplicitReturnType[T, Encoder[_]]

  def findCompanionMethodByImplicitReturnType[T : TypeTag, R : TypeTag]: Option[MethodSymbol] =
    findCompanionMethodsByReturnType[T, R].filter(_.isImplicit)
      .onlyOption(f"Multiple implicit ${implicitly[TypeTag[R]]} methods found on ${implicitly[TypeTag[T]]}")
      .map(_.asInstanceOf[MethodSymbol])

  def findCompanionMethodByName[T : TypeTag](methodName: String): Option[MethodSymbol] =
    findCompanionMethodsByName[T](methodName)
      .onlyOption(f"Multiple methods named $methodName found on ${implicitly[TypeTag[T]]}")
      .map(_.asInstanceOf[MethodSymbol])

  def findCompanionMethodsByReturnType[T : TypeTag, R : TypeTag] = {
    val checkClass = getClassSymbol[T]
    val companion = checkClass.companion
    val returnType = getClassSymbol[R]
    findMembersByReturnType(companion, returnType)
  }

  def findCompanionMethodsByName[T : TypeTag](methodName: String) = {
    val checkClass = getClassSymbol[T]
    val companion = checkClass.companion
    companion.typeSignature.members.filter {
      _.name.toString == methodName
    }.toList
  }

  def findMembersByReturnType(obj: Symbol, returnType: Symbol): List[Symbol] =
    obj.typeSignature.members.filter {
      _.typeSignature.resultType.baseClasses.contains(returnType)
    }.toList

  */

  /*
  def getEncoder[T : TypeTag]: Encoder[T] = {
    val comp: Symbol = getClassSymbol[T].companion
    val method: MethodSymbol = findCompanionImplicit[T, Encoder[T]].asInstanceOf[MethodSymbol]
    callMethod(comp, method).asInstanceOf[Encoder[T]]
  }

  def getDecoder[T : TypeTag]: Decoder[T] = {
    val method: MethodSymbol = findCompanionImplicit[T, Decoder[T]].asInstanceOf[MethodSymbol]
    callMethod(getClassSymbol[T], method).asInstanceOf[Decoder[T]]
  }
  */
}

/*
object Implicits {
  import scala.reflect.runtime.universe._
  import scala.language.implicitConversions

  implicit class RichReflectable[T](ct: TypeTag[T]) {

  }

  implicit class LonelySeq[T : TypeTag](seq: Seq[T]) {
    def onlyOption(msg: String = "Multiple values found!"): Option[T] =
      if (seq.tail.isEmpty)
        Some(seq.head)
      else
        None

    def onlyOrError(msg: String = "Multiple values found!"): T =
      if (seq.tail.isEmpty)
        seq.head
      else
        throw new RuntimeException(f"$msg: $seq")
  }

  implicit class WithFilterType(obj: Any) {
    def asType[T](obj: Any): T = obj.asInstanceOf[T]

    def ifType[U : ClassTag : Codec]: Option[U] = obj match {
      case obj2: U => Some(obj2)
      case _ => None
    }
  }

  implicit def methodFromName[T  : TypeTag](methodName: String): MethodSymbol =
    ReflectUtil.getTypeMethodSymbol[T](methodName)
}
*/