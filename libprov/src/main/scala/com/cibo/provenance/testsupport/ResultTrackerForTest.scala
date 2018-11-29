package com.cibo.provenance.testsupport

import java.io.File

import com.cibo.provenance._
import com.cibo.provenance.kvstore.{KVStore, LocalStore, S3Store}
import org.apache.commons.io.FileUtils
import com.cibo.provenance.kvstore._

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
  *       implicit val rt = ResultTrackerForTest(outputPath = "/tmp/mything-works-rt",
  *                                              referencePath = "src/test/resources/mything-works-rt"
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
  * @param outputStorage        A KVStore for to a local empty tmp directory used just by this test.
  * @param referenceStorage     A KVStore for reference data.  For UT: src/main/resources/.  For IT s3://....
  * @param bi                   The current app build information should be implicitly available.
  */
case class ResultTrackerForTest(outputStorage: KVStore, referenceStorage: KVStore)(implicit val bi: BuildInfo)
  extends ResultTrackerSimple(
    outputStorage,
    writable = true,
    Some(ResultTrackerSimple(referenceStorage, writable = false))
  ) with Rechecking {

  def outputPath = outputStorage.basePath

  def referencePath = referenceStorage.basePath

  require(underlyingTrackerOption.nonEmpty, "Refusing to clean() a ResultTracker that does not have an underlying tracker.")
  require(storage.isLocal, "Refusing to clean() an S3 path.")

  /**
    * Get the underlying tracker that holds just the reference data.
    *
    * @return a ResultTrackerSimple
    */
  def referenceTracker: ResultTrackerSimple = underlyingTrackerOption.get

  /**
    * Remove all output data, leaving the reference data in place.
    * This also clears caches.
    *
    * This should be called at the beginning of a test.
    */
  def clean(): Unit = {
    if (storage.isRemote) {
      sys.env.get("USER") match {
        case Some(user) =>
          if (!basePath.contains(user))
            throw new RuntimeException(f"Refusing to delete an S3 test a directory that does not contain the current user's name: $user not in ${basePath}")
          import scala.sys.process._
          s"aws s3 rm --recursive ${basePath}".!

        case None =>
          throw new RuntimeException(
            "Failed to determine the current user." +
              f"Refusing to delete a test a directory that does not contain the current user's name!: ${basePath}"
          )
      }
    }

    // For local paths this deletes the primary data.  For remote it deletes the data buffered locally.
    storage match {
      case local: LocalStore =>
        FileUtils.deleteDirectory(new File(local.basePath))
      case _: S3Store =>
    }

    clearCaches()
  }

  /**
    * This should be called at the end of a test.  If some things were done that did not have reference data,
    * it will notify the developer with a special exception, and give instructions on how to stage the data.
    *
    * Note: Calling push() will also stage it automatically.
    */
  def checkForUnstagedResults(): Unit = {
    if (storage.getKeySuffixes().toList.nonEmpty) {
      val msg =
        if (storage.isLocal && underlyingTrackerOption.map(_.isLocal).getOrElse(throw new RuntimeException("Missing underlying tracker.")))
          f"# New reference data! To stage it:\nrsync -av --progress ${basePath}/ ${referenceTracker.basePath}"
        else
          f"# New reference data! To stage it:\naws s3 sync ${basePath} ${referenceTracker.basePath}"
      throw new ResultTrackerForTest.UnstagedReferenceDataException(msg)
    }
  }

  @deprecated("call checkForUnstagedResults() instead", "v0.10.5")
  def check(): Unit = checkForUnstagedResults()

  /**
    * This method moves data from the outputPath to the referencePath.
    * It is typically run after a test is updated to emit new results, or the classes in question have a new version,
    * which also means new results.
    * It is called before check() when it is known that the test data needs to be regenerated.
    */
  def push(): Unit = {
    val referenceDir = underlyingTrackerOption.get.basePath
    if (isLocal && referenceTracker.isLocal) {
      if (new File(basePath).exists) {
        new File(referenceDir).getParentFile.mkdirs()
        FileUtils.copyDirectoryToDirectory(new File(basePath), new File(referenceDir))
      } else {
        logger.warn(f"No data in $basePath to transfer to $referencePath...")
      }
    } else {
      val cmd = Seq("aws", "s3", "sync", basePath, referenceDir)
      import scala.sys.process._
      logger.info(f"RUNNING: $cmd")
      cmd.!
    }
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
    Encoder.forProduct3[String, String, BuildInfo, ResultTrackerForTest](
      "localPath", "canonicalPath", "buildInfo")(
        r => (r.outputPath, r.referencePath, r.currentAppBuildInfo))

  implicit lazy val decoder: Decoder[ResultTrackerForTest] =
    Decoder.forProduct3[String, String, BuildInfo, ResultTrackerForTest](
      "localPath", "canonicalPath", "buildInfo")(
        (localPath,canonicalPath,buildInfo) => ResultTrackerForTest(localPath, canonicalPath)(buildInfo))

  def apply(outputPath: String, referencePath: String)(implicit buildInfo: BuildInfo): ResultTrackerForTest =
    ResultTrackerForTest(KVStore(outputPath), KVStore(referencePath))(buildInfo)

  class UnstagedReferenceDataException(msg: String) extends RuntimeException(msg)
}



