package com.cibo.provenance

import java.io.File
import scala.concurrent.{Await, ExecutionContext}


/**
  * This ResultTracker is used by the provenance test suite to test both sync and async
  * resolution across all tests.
  *
  * @param rootPath   The base path, under which the sync/ and async/ sub-paths have tracking.
  * @param ec         An execution context used to do async resolution.
  *
  */
case class ResultTrackerForSelfTest(rootPath: String)(implicit bi: BuildInfo, ec: ExecutionContext = ExecutionContext.global)
  extends ResultTrackerDuplex(
    new ResultTrackerSimple(rootPath + "/sync")(bi) with TestTrackingOverrides,
    new ResultTrackerSimple(rootPath + "/async")(bi) with TestTrackingOverrides
  ) {

  import org.apache.commons.io.FileUtils

  /**
    * The pair of underlying result trackers are used identically, except when calling resolve():
    * - the first calls resolve() normally
    * - the second calls resolveAsync() and awaits the result
    *
    * Note: This is only to be used by the test suite, since it embeds an Await.
    * It should
    *
    * @param call:  A `FunctionCallWithProvenance[O]` to resolve (get or create a result)
    * @tparam O     The output type of the call.
    *
    * @return       A `FunctionCallResultWithProvenance[O]` that wraps the call output.
    */
  override def resolve[O](call: FunctionCallWithProvenance[O]): FunctionCallResultWithProvenance[O] = {
    import scala.concurrent.duration._
    val aa = a.resolve(call)
    val bbFuture = b.resolveAsync(call)
    val bb = Await.result(bbFuture, 5.minutes)
    require(aa == bb, s"resolve and resolveAsync for $call returned different values: $aa vs $bb")
    aa
  }

  /**
    * Delete all data in storage.  This is typically called at the beginning of a test.
    */
  def wipe(): Unit = {
    if (rootPath.startsWith("s3://")) {
      sys.env.get("USER") match {
        case Some(user) =>
          if (!rootPath.contains(user))
            throw new RuntimeException(f"Refusing to delete a test a directory that does not contain the current user's name: $user not in ${rootPath}")
          val bothStores: S3Store = new S3Store(rootPath)(a.storage.asInstanceOf[S3Store].amazonS3)
          bothStores.getSuffixes().foreach(bothStores.remove)
        case None =>
          throw new RuntimeException(
            "Failed to determine the current user." +
              f"Refusing to delete a test a directory that does not contain the current user's name!: $rootPath"
          )
      }
    }

    // For local paths this deletes the primary data.  For remote it deletes the data buffered locally.
    val testDataDir = new File(rootPath)
    FileUtils.deleteDirectory(testDataDir)
  }
}


/**
  * The TestTracking trait adds methods to a ResultTrackerSimple that are only applicable for testing.
  */
trait TestTrackingOverrides extends ResultTrackerSimple {
  override protected def checkForInconsistentSerialization[O](obj: O): Boolean = true
  override protected def blockSavingConflicts(newResult: FunctionCallResultWithKnownProvenanceSerializable): Boolean = false
  override protected def checkForConflictedOutputBeforeSave(newResult: FunctionCallResultWithKnownProvenanceSerializable): Boolean = true
  override protected def checkForResultAfterSave(newResult: FunctionCallResultWithKnownProvenanceSerializable): Boolean = true
}

