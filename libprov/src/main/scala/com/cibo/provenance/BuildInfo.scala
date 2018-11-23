package com.cibo.provenance

import io.circe._

import scala.reflect.ClassTag

/**
  * Created by ssmith on 10/22/17.
  *
  * A `BuildInfo` object captures the `commitId` and `buildId` for a given software build, along with other related
  * values.
  *
  * The `BuildInfo` is subclassed by VCS platform (git, currently).  When serialized, the type is captured in
  * the "vcs" key in the JSON.
  *
  * - `commitId`: Depends on the VCS platform.  A unique identifier for an exact set of source code.
  * - `buildId`: A millisecond-level timestamp as a string.
  *
  * NOTE: The `BuildInfo` in use is the build of the app or library that is _using_ the data provenance library,
  * NOT the build of the provenance library itself.  The provenance library's `BuildInfo` is also available
  * as `com.cibo.provenance.internal.BuildInfo`.
  *
  * There are three additional special case  of sub-classes:
  * - `BuildInfoBrief`: a stub for a real `BuildInfo` that retains just the commitId and buildId needed to load it
  * - `NoBuildInfo`: Used for values with `UnknownProvenance`
  * - `DummyBuildInfo`: A class used by the test suite that uses a constant dummy commitId and constant buildId.
  *
  */
trait BuildInfo extends Serializable {
  // SbtBuildInfo Defaults
  def name: String
  def version: String
  def scalaVersion: String
  def sbtVersion: String
  def builtAtString: String
  def builtAtMillis: Long

  // Provenance Library Core Data
  def commitId: String
  def buildId: String = builtAtString.replace(":",".").replace(" ",".").replace("-",".")

  // Common Methods
  def abbreviate: BuildInfoBrief = BuildInfoBrief(commitId, buildId)
  def debrief(implicit rt: ResultTracker): BuildInfo = this

  override def toString: String = f"BuildInfo($commitId@$buildId)"

  def codec: Codec[_ <: BuildInfo]

  def toEncoded: Json

  @transient
  lazy val toBytesAndDigest: (Array[Byte], Digest) =
    Codec.serializeAndDigest(this)(BuildInfo.codec)

  def toBytes = toBytesAndDigest._1

  def toDigest = toBytesAndDigest._2
}


object BuildInfo {
  import scala.language.implicitConversions

  // A custom encoding uses the "vcs" field to differentiate subclasses.
  // This is not a sealed trait so it cannot happen automatically w/ circe macros.

  private val decoder: Decoder[BuildInfo] = Decoder.instance(
    c => {
      c.downField("vcs").as[String] match {
        case Right(value) =>
          // NOTE: We could add reflection rather than hard-coding the list of VCS types.
          value match {
            case "git" => c.as[BuildInfoGit](BuildInfoGit.codec.decoder)
            case "none" => c.as[BuildInfoNone](BuildInfoNone.codec.decoder)
            case "brief" => c.as[BuildInfoBrief](BuildInfoBrief.codec.decoder)
            case other => throw new RuntimeException(f"Unrecognized VCS: $other")
          }
        case Left(err) =>
          throw err
      }
    }
  )

  private val encoder: Encoder[BuildInfo] =
    Encoder.instance { _.toEncoded }

  implicit def codec: CirceJsonCodec[BuildInfo] =
    Codec(encoder, decoder)
}



