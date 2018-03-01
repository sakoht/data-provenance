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
      val paths = s3db.getSuffixesForPrefix("")
      paths.foreach {
        path =>
          println("DELETE: " + path)
      }
    } else {
      val testDataDir = basePath.getFile
      FileUtils.deleteDirectory(testDataDir)
    }
  }
}