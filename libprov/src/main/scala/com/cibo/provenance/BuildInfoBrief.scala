package com.cibo.provenance

import io.circe.{Decoder, Encoder}

case class BuildInfoBrief(commitId: String, override val buildId: String) extends BuildInfo {
  def impl: BuildInfoNone = BuildInfoNone

  def name: String = impl.name
  def version: String = impl.version
  def scalaVersion: String = impl.scalaVersion
  def sbtVersion: String = impl.sbtVersion

  def builtAtString: String = impl.builtAtString
  def builtAtMillis: Long = impl.builtAtMillis

  override def abbreviate: BuildInfoBrief = this
  override def debrief(implicit rt: ResultTracker):  BuildInfo = rt.loadBuildInfoOption(commitId, buildId).get

  def codec: CirceJsonCodec[BuildInfoBrief] = BuildInfoBrief.codec
  def toEncoded = codec.encoder.apply(this)
}


object BuildInfoBrief {

  private val encoder: Encoder[BuildInfoBrief] =
    Encoder.forProduct3("vcs", "commitId", "buildId") {
      bi => Tuple3("brief", bi.commitId, bi.buildId)
    }

  private val decoder: Decoder[BuildInfoBrief] =
    Decoder.forProduct3("vcs", "commitId", "buildId") {
      (vcs: String, commitId: String, buildId: String) => BuildInfoBrief(commitId, buildId)
    }

  implicit val codec: CirceJsonCodec[BuildInfoBrief] = Codec(encoder, decoder)


}
