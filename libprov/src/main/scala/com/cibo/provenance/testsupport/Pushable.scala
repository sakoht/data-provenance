package com.cibo.provenance.testsupport

import com.cibo.provenance.ResultTrackerSimple
import org.apache.commons.io.FileUtils

/**
  * Allow a ResultTrackerSimple to push data from the top level tracker to the underlying tracker.
  *
  * When a test case interacts with new versions of components, this pushes the new data into
  * position to be reference data.
  */
trait Pushable extends ResultTrackerSimple {
  require(underlyingTracker.nonEmpty, "The Pushable trait only works on a multi-layer ResultTrackerSimple.")
  def push(): Unit = {
    val referencePath = underlyingTracker.get.basePath
    if (!referencePath.toFile.exists())
      referencePath.toFile.mkdirs()
    FileUtils.copyDirectoryToDirectory(basePath.toFile, referencePath.toFile)
    if (referencePath.isRemote)
      referencePath.syncToS3()
  }
}
