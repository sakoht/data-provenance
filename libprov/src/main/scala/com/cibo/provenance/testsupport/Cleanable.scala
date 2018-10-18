package com.cibo.provenance.testsupport

import com.cibo.provenance.ResultTrackerSimple
import org.apache.commons.io.FileUtils


/**
  * Allow a ResultTrackerSimple to wipe the top-level tracker in a multi-layer tracker.
  * Typically happens at the beginning of a test case.
  * This requires that the top-level tracker be on local disk.
  */
trait Cleanable extends ResultTrackerSimple {
  def clean(): Unit = {
    require(underlyingTracker.nonEmpty, "Refusing to clean() a ResultTracker that does not have an underlying tracker.")
    require(basePath.isLocal, "Refusing to clean() an S3 path.")

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
  }
}
