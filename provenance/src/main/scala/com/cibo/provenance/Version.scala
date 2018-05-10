package com.cibo.provenance

import java.time.Instant

/**
  * Created by ssmith on 5/11/17.
  *
  * This represents the "declared version" of a FunctionWithProvenance.  The system expects that output will
  * be consistent for the same inputs at a given declared version, and will "shortcut" past executing a call
  * when a result exists for the same input at the same version.
  *
  * The _full_ version of data is a combination of the declared version, and the BuildInfo associated with each
  * result.  When a given declared version is demonstrated to be inconsistent at some commit/build, the
  * function in question is considered tainted at that version.
  *
  */


sealed trait Version extends Serializable {
  val id: String
  val isDev: Boolean
  override def toString: String = f"v$id"
}

case class ProdVersion(id: String) extends Version {
  val isDev: Boolean = false
  require(!id.contains("-dev-"), "The text '-dev-' can only appear in non-production Version IDs.")
}

case class DevVersion(id: String) extends Version {
  val isDev: Boolean = true
  require(id.contains("-dev-"), "The text '-dev-' must appear ini dev version IDs.")
}

object NoVersion extends ProdVersion("-") with Serializable

object NoVersionProvenance extends UnknownProvenance[Version](NoVersion) with Serializable


object Version {
  import io.circe._

  lazy val devSuffix =
    "-dev-" + Instant.now.toString + "-" + sys.env.getOrElse("USER", "anonymous")

  def apply(id: String) = new ProdVersion(id)

  def apply(base: String, dev: Boolean) = if (dev) new DevVersion(base + devSuffix) else new ProdVersion(base)

  def unapply(obj: Version): String =
    obj.id

  implicit val encoder: Encoder[Version] = Encoder.instance {
    (obj: Version) =>
      val found = obj.id.contains("-dev-")
      obj match {
        case _: ProdVersion =>
          if (found)
            throw new RuntimeException(f"Expected no -dev- in the id for dev=false: ${obj.id}")
        case _: DevVersion =>
          if (!found)
            throw new RuntimeException(f"Expected -dev- in the version for dev=true: ${obj.id}")
      }
      Encoder.encodeString.apply(obj.id)
  }

  implicit val decoder: Decoder[Version] = Decoder.instance {
    (c: HCursor) =>
      val id: String = c.value.asString.get
      val dev: Boolean = id.contains("-dev-")
      val obj: Version =
        if (id == "-")
          NoVersion
        else if (id.contains("-dev-"))
          DevVersion(id)
        else
          ProdVersion(id)
      Right(obj).asInstanceOf[Either[DecodingFailure, Version]]
  }
}


