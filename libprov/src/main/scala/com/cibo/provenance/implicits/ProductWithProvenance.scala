package com.cibo.provenance.implicits

import com.cibo.provenance._

/**
  * This implicit wrapper tracks decomposition of a case class.
  * It allows for select parts of a Product to be passed individually to other components,
  * while tracking which parts were used.
  *
  * @param product    Some ValueWithProvenance of a Product
  * @tparam T         The type of value the ValueWithProvenance wraps.
  */
class ProductWithProvenance[T <: Product : Codec](product: ValueWithProvenance[T]) {
  private val productClassTag = implicitly[Codec[T]].classTag
  private val fields = productClassTag.runtimeClass.getDeclaredFields
  private val fieldNames = fields.map(_.getName)
  private val fieldTypes = fields.map(_.getType)

  def productElement[O : Codec](elementName: String): ProductElementByNameWithProvenance[T, O]#Call = {
    val i = fieldNames.indexOf(elementName)
    require(i != -1, f"Failed to find element $elementName in $product: found $fieldNames")
    val clazz = fieldTypes(i)
    val expectedClazz = implicitly[Codec[O]].classTag.runtimeClass
    require(clazz.getName == expectedClazz.getName, f"Element $elementName in $productClassTag has type $clazz, but we expected $expectedClazz!")
    val f = new ProductElementByNameWithProvenance[T, O]
    f(product, elementName)
  }
}

/**
  * This built-in is used to decompose case classes.  It is similar to `ApplyWithProvenance`,
  * except the parameters are accessed by name.
  *
  * @tparam T       The type of Product to which the function is being applied.
  * @tparam O:      The type of the element being accessed.
  */
class ProductElementByNameWithProvenance[T <: Product : Codec, O : Codec]() extends Function2WithProvenance[T, String, O]  {
  self =>

  val currentVersion: Version = NoVersion

  private val productClassTag = implicitly[Codec[T]].classTag
  //private val outputClassTag = implicitly[Codec[O]].classTag
  private val fields = productClassTag.runtimeClass.getDeclaredFields
  private val fieldNames = fields.map(_.getName)
  private val fieldTypes = fields.map(_.getType)

  def impl(product: T, elementName: String): O = {
    val i = fieldNames.indexOf(elementName)
    require(i != -1, f"Failed to find element $elementName in $product: found $fieldNames")
    require(fieldTypes(i) == outputClassTag.runtimeClass,
      f"Incorrect class type for $productClassTag $elementName.  Class has ${fieldTypes(i)} but expected $outputClassTag")
    product.productElement(i).asInstanceOf[O]
  }

  override lazy val typeParameterTypeNames: Seq[String] =
    Seq(
      Codec.classTagToSerializableName(implicitly[Codec[T]].classTag),
      Codec.classTagToSerializableName(implicitly[Codec[O]].classTag)
    )

  def wrap(obj: ValueWithProvenance[_ <: T]): (ValueWithProvenance[_ <: String]) => Call = {
    val f: (ValueWithProvenance[_ <: String]) => Call = apply(obj, _)
    f
  }
}


