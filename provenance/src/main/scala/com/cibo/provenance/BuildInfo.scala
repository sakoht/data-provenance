package com.cibo.provenance

import java.time.Instant

import io.circe.{Decoder, DecodingFailure, Encoder, Json}


/**
  * Created by ssmith on 10/22/17.
  *
  * Apps using this library generate a BuildInfo object at compile time that extends this trait.
  */

/**
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

  // Provenance Library Core
  def commitId: String
  def buildId: String = builtAtString.replace(":",".").replace(" ",".").replace("-",".")

  def abbreviate: BuildInfoBrief = BuildInfoBrief(commitId, buildId)
  def debrief(implicit rt: ResultTracker): BuildInfo = this

  override def toString: String = f"BuildInfo($commitId@$buildId)"

  def mkEncoder: Encoder[BuildInfo] = BuildInfo.createEncoder[BuildInfo]
  def mkDecoder: Decoder[BuildInfo] = BuildInfo.createDecoder[BuildInfo]

  lazy val toBytesAndDigest: (Array[Byte], Digest) =
    Util.getBytesAndDigest(this)(
      this.mkEncoder,
      this.mkDecoder
    )

  def toBytes = toBytesAndDigest._1
  def toDigest = toBytesAndDigest._2
}



object BuildInfo {
  // A custom encoding uses the "vcs" field to differentiate subclasses.
  // This is not a sealed trait so it cannot happen automatically w/ circe macros.

  implicit def createDecoder[A <:  BuildInfo]: Decoder[A] = Decoder.instance(
    c => {
      (
        c.downField("vcs").as[String] match {
          case Right(value) => value match {
            case "git" => c.as[GitBuildInfo](GitBuildInfo.decoder)
            case "none" => c.as[NoBuildInfo](NoBuildInfo.decoder)
            case "brief" => c.as[BuildInfoBrief](BuildInfoBrief.decoder)
            case other => throw new RuntimeException(f"Unrecognized VCS: $other")
          }
          case Left(err) =>
            throw err
        }
      ).asInstanceOf[Either[io.circe.DecodingFailure, A]]
    }
  )

  implicit def createEncoder[A <:  BuildInfo]: Encoder[A] =
    new Encoder[A] {
      def apply(obj: A): Json =
        obj match {
          case o: GitBuildInfo => GitBuildInfo.encoder(o)
          case o: NoBuildInfo => NoBuildInfo.mkEncoder(o)
          case o: BuildInfoBrief => BuildInfoBrief.encoder(o)
          case other =>
            throw new RuntimeException(f"Unrecognized type of BuildInfo: $other")
        }
    }
}

/**
  * SbtBuildInfo uses this as the base trait for projects using `git`.
  * A custom object extending this trait will be created and inserted into the build.
  *
  * When that object is serialized and later re-loaded, it comes back a a GitBuildInfoSaved (below).
  */
trait GitBuildInfo extends BuildInfo with Serializable {
  def gitBranch: String
  def gitRepoClean: String
  def gitHeadRev: String
  def gitCommitAuthor: String
  def gitCommitDate: String
  def gitDescribe: String

  def commitId: String = gitHeadRev
}

/**
  * A re-loaded instance of a saved GitBuildInfo.
  *
  * @param name
  * @param version
  * @param scalaVersion
  * @param sbtVersion
  * @param builtAtString
  * @param builtAtMillis
  * @param gitBranch
  * @param gitRepoClean
  * @param gitHeadRev
  * @param gitCommitAuthor
  * @param gitCommitDate
  * @param gitDescribe
  */
case class GitBuildInfoSaved(
  name: String,
  version: String,
  scalaVersion: String,
  sbtVersion: String,
  builtAtString: String,
  builtAtMillis: Long,
  gitBranch: String,
  gitRepoClean: String,
  gitHeadRev: String,
  gitCommitAuthor: String,
  gitCommitDate: String,
  gitDescribe: String
) extends GitBuildInfo


object GitBuildInfo {
  implicit val encoder: Encoder[GitBuildInfo] =
    Encoder.forProduct15(
      "vcs", "name", "version", "scalaVersion", "sbtVersion", "builtAtString", "builtAtMillis", "commitId", "buildId",
      "gitBranch", "gitRepoClean", "gitHeadRev", "gitCommitAuthor", "gitCommitDate", "gitDescribe"
    ) {
      bi =>
        Tuple15(
          "git", bi.name, bi.version, bi.scalaVersion, bi.sbtVersion, bi.builtAtString, bi.builtAtMillis,
          bi.commitId, bi.buildId, bi.gitBranch, bi.gitRepoClean, bi.gitHeadRev, bi.gitCommitAuthor,
          bi.gitCommitDate, bi.gitDescribe
        )
    }

  implicit val decoder: Decoder[GitBuildInfo] =
    Decoder.forProduct15(
      "vcs", "name", "version", "scalaVersion", "sbtVersion", "builtAtString", "builtAtMillis", "commitId", "buildId",
      "gitBranch", "gitRepoClean", "gitHeadRev", "gitCommitAuthor", "gitCommitDate", "gitDescribe"
    ) {
      (
        vcs: String, name: String, version: String, scalaVersion: String, sbtVersion: String,
        builtAtString: String, builtAtMillis: Long, commitId: String, buildId: String,
        gitBranch: String, gitRepoClean: String, gitHeadRev: String,
        gitCommitAuthor: String, gitCommitDate: String, gitDescribe: String
      ) =>
        GitBuildInfoSaved(
          name, version, scalaVersion, sbtVersion, builtAtString, builtAtMillis,
          gitBranch, gitRepoClean, gitHeadRev, gitCommitAuthor, gitCommitDate, gitDescribe
        )
    }
}


trait NoBuildInfo extends BuildInfo with Serializable {
  def name: String = "-"
  def version: String = "-"
  def scalaVersion: String = "-"
  def sbtVersion: String = "-"
  def builtAtString: String = "-"
  def builtAtMillis: Long = 0L

  def commitId: String = "-"
  override def buildId: String = "-"
}


object NoBuildInfo extends NoBuildInfo {
  implicit val encoder: Encoder[NoBuildInfo] = Encoder.forProduct1("vcs")(_ => Tuple1("none"))
  implicit val decoder: Decoder[NoBuildInfo] = Decoder.forProduct1("vcs")((vcs: String) => NoBuildInfo)
}


case class BuildInfoBrief(commitId: String, override val buildId: String) extends BuildInfo {
  def impl: NoBuildInfo = NoBuildInfo

  def name: String = impl.name
  def version: String = impl.version
  def scalaVersion: String = impl.scalaVersion
  def sbtVersion: String = impl.sbtVersion

  def builtAtString: String = impl.builtAtString
  def builtAtMillis: Long = impl.builtAtMillis

  override def abbreviate: BuildInfoBrief = this
  override def debrief(implicit rt: ResultTracker):  BuildInfo = rt.loadBuildInfoOption(commitId, buildId).get
}


object BuildInfoBrief {
  implicit val encoder: Encoder[BuildInfoBrief] =
    Encoder.forProduct3("vcs", "commitId", "buildId") {
      bi => Tuple3("brief", bi.commitId, bi.buildId)
    }
  implicit val decoder: Decoder[BuildInfoBrief] =
    Decoder.forProduct3("vcs", "commitId", "buildId") {
      (vcs: String, commitId: String, buildId: String) => BuildInfoBrief(commitId, buildId)
    }
}

