package com.cibo.provenance

/**
  * The TestTracking trait adds methods to a ResultTrackerSimple that are only applicable for testing.
  */
trait TestTracking extends ResultTrackerSimple {
  import org.apache.commons.io.FileUtils

  /**
    * Delete all data in storage.  This is typically called at the beginning of a test.
    */
  def wipe: Unit = {
    if (basePath.isRemote) {
      sys.env.get("USER") match {
        case Some(user) =>
          if (!basePath.path.contains("-" + user))
            throw new RuntimeException(f"Refusing to delete a test a directory that does not contain the current user's name: $user")
          com.cibo.io.Shell.run(s"aws s3 rm --recursive ${basePath.path}")
        case None =>
          throw new RuntimeException(
            "Failed to determine the current user." +
            f"Refusing to delete a test a directory that does not contain the current user's name!"
          )
      }
    } else {
      val testDataDir = basePath.getFile
      FileUtils.deleteDirectory(testDataDir)
    }
  }
}