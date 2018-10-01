package com.cibo.provenance

import com.cibo.io.s3.SyncablePath
import scala.concurrent.{Await, ExecutionContext}

/**
  * This ResultTracker is used by the provenance test suite to test both sync and async
  * resolution across all tests.
  *
  * @param rootPath   The base path, under which the sync/ and async/ sub-paths have tracking.
  * @param ec         An execution context used to do async resolution.
  *
  */
case class ResultTrackerForTest(rootPath: SyncablePath)(implicit bi: BuildInfo, ec: ExecutionContext = ExecutionContext.global)
  extends ResultTrackerDuplex(
    new ResultTrackerSimple(rootPath / "sync")(bi, ec) with TestTrackingOverrides,
    new ResultTrackerSimple(rootPath / "async")(bi, ec) with TestTrackingOverrides
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
    if (rootPath.isRemote) {
      sys.env.get("USER") match {
        case Some(user) =>
          if (!rootPath.path.contains(user))
            throw new RuntimeException(f"Refusing to delete a test a directory that does not contain the current user's name: $user not in ${rootPath.path}")
          com.cibo.io.Shell.run(s"aws s3 rm --recursive ${rootPath.path}")

        case None =>
          throw new RuntimeException(
            "Failed to determine the current user." +
              f"Refusing to delete a test a directory that does not contain the current user's name!: ${rootPath.path}"
          )
      }
    }

    // For local paths this deletes the primary data.  For remote it deletes the data buffered locally.
    val testDataDir = rootPath.toFile
    FileUtils.deleteDirectory(testDataDir)
  }
}


object ResultTrackerForTest {
  def apply(pathString: String)(implicit  bi: BuildInfo): ResultTrackerForTest =
    ResultTrackerForTest(SyncablePath(pathString))
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


