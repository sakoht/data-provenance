package com.cibo.provenance.testsupport

import com.cibo.io.s3.SyncablePath
import com.cibo.provenance._
import org.apache.commons.io.FileUtils

/**
  * This ResultTracker is used by applications to regression test.
  * It re-checks results of all attempts to resolve a call versus reference data that is auto-maintained.
  *
  * Usage:
  *
  *   implicit val bi: BuildInfo = com.mycompany.myapp.BuildInfo
  *
  *   describe("mything") {
  *     it("works") {
  *       implicit val rt = ResultTrackerForTest(outputPath = SyncablePath("/tmp/mything-works-rt"),
  *                                              referencePath = SyncablePath("src/test/resources/mything-works-rt")
  *       rt.clean()
  *       MyThing(p1, p2).resolve
  *       MyThing(p3, p4).resolve
  *       MyThing(p5, p6).resolve
  *       // add more examples to test corner cases here...
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
  ) with Rechecking {
  require(underlyingTracker.nonEmpty, "Refusing to clean() a ResultTracker that does not have an underlying tracker.")
  require(basePath.isLocal, "Refusing to clean() an S3 path.")

  /**
    * Remove all output data, leaving the reference data in place.
    * This also clears caches.
    *
    * This should be called at the beginning of a test.
    */
  def clean(): Unit = {
    if (basePath.isRemote) {
      sys.env.get("USER") match {
        case Some(user) =>
          if (!basePath.path.contains(user))
            throw new RuntimeException(f"Refusing to delete an S3 test a directory that does not contain the current user's name: $user not in ${basePath.path}")
          com.cibo.io.Shell.run(s"aws s3 rm --recursive ${basePath.path}")

        case None =>
          throw new RuntimeException(
            "Failed to determine the current user." +
              f"Refusing to delete a test a directory that does not contain the current user's name!: ${basePath.path}"
          )
      }
    }

    // For local paths this deletes the primary data.  For remote it deletes the data buffered locally.
    val testDataDir = basePath.toFile
    FileUtils.deleteDirectory(testDataDir)

    clearCaches()
  }

  /**
    * This should be called at the end of a test.  If there is new reference data,
    * it will notify the developer with a special exception, and give instructions on how to stage the data.
    *
    * Note: Calling push() will also stage it automatically.
    */
  def check(): Unit = {
    if (basePath.toFile.exists() && basePath.toFile.list().toList.nonEmpty) {
      val msg =
        if (outputPath.isLocal && referencePath.isLocal)
          f"# New reference data! To stage it:\nrsync -av --progress ${outputPath.path}/ ${referencePath.path}"
        else
          f"# New reference data! To stage it:\naws s3 sync ${outputPath.path} ${referencePath.path}"
      throw new ResultTrackerForTest.UnstagedReferenceDataException(msg)
    }
  }

  /**
    * This method moves data from the outputPath to the referencePath.
    * It is typically run after a test is updated to emit new results, or the classes in question have a new version,
    * which also means new results.
    * It is called before check() when it is known that the test data needs to be regenerated.
    */
  def push(): Unit = {
    val referencePath = underlyingTracker.get.basePath
    assert(basePath.toFile.getName == referencePath.toFile.getName)

    if (!referencePath.toFile.exists())
      referencePath.toFile.mkdirs()
    FileUtils.copyDirectoryToDirectory(basePath.toFile, referencePath.toFile.getParentFile)
    if (referencePath.isRemote)
      referencePath.syncToS3()

    clean()
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



