package com.cibo.provenance.testsupport

import com.cibo.io.s3.SyncablePath
import com.cibo.provenance._

/**
  * This ResultTracker is used by applications to regression test.
  * It re-checks results, and helps maintain the test reference data.
  *
  * Usage:
  *
  *   implicit val bi: BuildInfo = com.mycompany.myapp.BuildInfo
  *
  *   describe("mything") {
  *     it("works") {
  *       implicit val rt = ResultTrackerForTest(outputPath = SyncablePath("/tmp/mything-works-rt.out"),
  *                                              referencePath = SyncablePath("src/test/resources/mything-works-rt.ref")
  *       rt.clean()
  *       MyThing(p1, p2).resolve
  *       MyThing(p3, p4).resolve
  *       MyThing(p5, p6).resolve
  *       rt.check()
  *     }
  *   }
  *
  * This ResultTracker is a two-layer ResultTrackerSimple.  New data, including conflicts, will land in the top-level
  * directory.  The reference data is in the bottom directory (which might be on S3 for integration tests).
  *
  * It uses the "Rechecking" trait, which overrides resolve() and resolveAsync() to re-verify results that are already saved.
  * On the first test run with tests or software changes, it will automatically stage the new data.
  *
  * @param outputPath           A path to a local empty tmp directory used just by this test.
  * @param referencePath        A path to reference data.  For UT: src/main/resources/.  For IT s3://....
  * @param bi                   The current app build information should be implicitly available.
  */
case class ResultTrackerForTest(outputPath: SyncablePath, referencePath: SyncablePath)(implicit val bi: BuildInfo)
  extends ResultTrackerSimple(
    outputPath,
    writable = true,
    Some(ResultTrackerSimple(referencePath, writable = false))
  ) with Rechecking with Cleanable with Pushable {

  /**
    * This should be called at the end of a test.  If there is new reference data, it will cancel
    * the test, and notify the developer where the data is to stage.
    *
    * The intent is to make it as simple as possible to upgrade
    * @param pos
    */
  def check()(implicit pos: org.scalactic.source.Position): Unit = {
    if (basePath.toFile.exists() && basePath.toFile.list().toList.nonEmpty) {
      val msg =
        if (outputPath.isLocal && referencePath.isLocal)
          f"# New reference data! To stage it:\nrsync -av --progress ${outputPath.path}/ ${referencePath.path}"
        else
          f"# New reference data! To stage it:\naws s3 sync ${outputPath.path} ${referencePath.path}"
      throw new ResultTrackerForTest.UnstagedReferenceDataException(msg)
    }
  }
}


object ResultTrackerForTest {
  import io.circe._
  import com.cibo.provenance.BuildInfo.codec
  implicit private val buildInfoEncoder = codec.encoder
  implicit private val buildInfoDecoder = codec.decoder
  implicit private val e2 = new BinaryEncoder[ResultTrackerSimple]
  implicit private val d2 = new BinaryDecoder[ResultTrackerSimple]

  implicit lazy val encoder: Encoder[ResultTrackerForTest] =
    Encoder.forProduct3[SyncablePath, SyncablePath, BuildInfo, ResultTrackerForTest](
      "localPath", "canonicalPath", "buildInfo")(
        r => (r.outputPath, r.referencePath, r.currentAppBuildInfo))

  implicit lazy val decoder: Decoder[ResultTrackerForTest] =
    Decoder.forProduct3[SyncablePath, SyncablePath, BuildInfo, ResultTrackerForTest](
      "localPath", "canonicalPath", "buildInfo")(
        (localPath,canonicalPath,buildInfo) => new ResultTrackerForTest(localPath,canonicalPath)(buildInfo))

  class UnstagedReferenceDataException(msg: String) extends RuntimeException(msg)
}



