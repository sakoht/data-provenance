package com.cibo.provenance

import com.cibo.io.s3.{LocalPath, S3SyncablePath}

import scala.concurrent.Await
import scala.util.Try

/**
  * The TestTracking trait adds methods to a ResultTrackerSimple that are only applicable for testing.
  */
trait TestTracking extends ResultTrackerSimple {
  import org.apache.commons.io.FileUtils

  override protected def checkForInconsistentSerialization[O](obj: O): Boolean = true
  override protected def blockSavingConflicts(newResult: FunctionCallResultWithKnownProvenanceSerializable): Boolean = false
  override protected def checkForConflictedOutputBeforeSave(newResult: FunctionCallResultWithKnownProvenanceSerializable): Boolean = true
  override protected def checkForResultAfterSave(newResult: FunctionCallResultWithKnownProvenanceSerializable): Boolean = true

  /**
    * Delete all data in storage.  This is typically called at the beginning of a test.
    */
  def wipe: Unit = {
    if (basePath.isRemote) {
      sys.env.get("USER") match {
        case Some(user) =>
          if (!basePath.path.contains(user))
            throw new RuntimeException(f"Refusing to delete a test a directory that does not contain the current user's name: $user not in ${basePath.path}")
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
  }

  // During testing, force the synchronous interface to also exercise the async interface.
  // This ensures automatic testing of both routes to data everywhere.
  // We turn off GCache caching to ensure one load/save doesn't affect the other.

  @transient
  override lazy val lightCacheSize: Long = 0L

  @transient
  override lazy val heavyCacheSize: Long = 0L

  override def saveBytes(path: String, bytes: Array[Byte]): Unit = {
    // Save asynchronously
    Await.result(saveBytesAsync(path, bytes), ioTimeout)
    val v1 = loadBytes(path)
    require(v1.deep == bytes.deep, s"Reloaded data from async call does not match at $path")

    // Delete it.
    storage.remove(path)
    require(Try(loadBytes(path)).isFailure, s"Deletion failed for path at $path")

    // Save synchronously.
    super.saveBytes(path, bytes)
    val v2 = loadBytes(path)
    require(v2.deep == bytes.deep, "")
  }

  override def loadBytes(path: String): Array[Byte] = {
    // Load async
    val v1 = Await.result(loadBytesAsync(path), ioTimeout)

    // Load sync
    val v2 = super.loadBytes(path)

    // Compare
    require(v1.deep == v2.deep, s"Synchronous load and async load do not match for $path!")
    
    // Return either
    v1
  }
}
