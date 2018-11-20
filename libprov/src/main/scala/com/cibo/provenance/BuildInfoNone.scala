package com.cibo.provenance

import io.circe.{Decoder, Encoder}

trait BuildInfoNone extends BuildInfo with Serializable {
  def name: String = "-"
  def version: String = "-"
  def scalaVersion: String = "-"
  def sbtVersion: String = "-"
  def builtAtString: String = "-"
  def builtAtMillis: Long = 0L

  def commitId: String = "-"
  override def buildId: String = "-"
}


object BuildInfoNone extends BuildInfoNone {
  private val encoder: Encoder[BuildInfoNone] = Encoder.forProduct1("vcs")(_ => Tuple1("none"))
  private val decoder: Decoder[BuildInfoNone] = Decoder.forProduct1("vcs")((vcs: String) => BuildInfoNone)
  implicit val codec: CodecUsingJson[BuildInfoNone] = Codec(encoder, decoder)
  def toEncoded = codec.encoder.apply(this)
}


