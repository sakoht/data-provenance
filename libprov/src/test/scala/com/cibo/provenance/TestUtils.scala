package com.cibo.provenance

import java.time.Instant

import com.cibo.io.s3.SyncablePath
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.scalatest.Matchers
import com.cibo.aws.AWSClient.Implicits.s3SyncClient
import com.cibo.io.s3.SyncablePathBaseDir.Implicits.default

object TestUtils extends LazyLogging with Matchers {
  import java.io.File
  import java.nio.file.{Files, Paths}

  import com.cibo.io.Shell.getOutputAsBytes

  // The this is the BuildInfo _object_ for this library.
  // Not to be confused with the BuildInfo base trait use for all apps with provenance tracking.
  val libBuildInfo: BuildInfo = com.cibo.provenance.internal.BuildInfo

  // Use the scala version for the library in this test, cross-compiled tests can run in parallel.
  val subdir =
    sys.env.getOrElse("USER", "anonymous-" + com.cibo.provenance.internal.BuildInfo.buildId.toString) +
      f"/data-provenance-test-output-${libBuildInfo.scalaVersion}"

  val localTestOutputBaseDir: String =
    f"/tmp/" + subdir

  val remoteTestOutputBaseDir: String =
    f"s3://com-cibo-user/" + subdir

  // Set this env var
  val testOutputBaseDir =
    sys.env.get("PROVENANCE_TEST_REMOTE") match {
      case Some(value) => remoteTestOutputBaseDir
      case None => localTestOutputBaseDir
    }

  def diffOutputSubdir(subdir: String) = {
    diffOutputSubdirX(subdir + "/sync")
    diffOutputSubdirX(subdir + "/async")
  }

  def diffOutputSubdirX(subdir: String) = {
    val version =
      if (libBuildInfo.scalaVersion.startsWith("2.11"))
        "2.11"
      else if (libBuildInfo.scalaVersion.startsWith("2.12"))
        "2.12"
      else
        throw new RuntimeException(f"Unexpected scala version $libBuildInfo.scalaVersion")

    val actualOutputDirPath = SyncablePath(f"$testOutputBaseDir/$subdir")
    if (actualOutputDirPath.isRemote) {
      val f = actualOutputDirPath.toFile
      logger.info(s"Syncing $actualOutputDirPath to local disk...")
      FileUtils.deleteDirectory(f)
      val t1 = Instant.now
      actualOutputDirPath.syncFromS3()
      val t2 = Instant.now
      val d = t2.toEpochMilli - t1.toEpochMilli
      logger.warn(s"Sync of $actualOutputDirPath completed in ${d}ms.")

    }

    val actualOutputLocalPath = actualOutputDirPath.localPath

    def normalize(in: String): String = {
      val lines = in.split("\n")
      val leftMarginSize: Int = lines.foldLeft[Int](lines.head.length) {
        case (prevMargin, nextLine) =>
          val margin = nextLine.length - nextLine.replaceAll("^\\s+", "").length
          if (margin < prevMargin) margin else prevMargin
      }
      val leftMargin = " " * leftMarginSize
      def rightColumn(line: String): String = line.split("\\s+").last
      lines.map(_.stripPrefix(leftMargin)).sortBy(rightColumn).mkString("\n") + "\n"
    }

    val newManifestBytes =
      getOutputAsBytes(s"cd $actualOutputLocalPath && (wc -c `find . -type f | grep -v codecs`)")
    val newManifestString = normalize(new String(newManifestBytes))

    val rootSubdir = "src/test/resources/expected-output"

    val expectedDataRoot =
      if (new File(rootSubdir).exists)
        rootSubdir
      else if (new File(f"libprov/$rootSubdir").exists)
        f"libprov/$rootSubdir"
      else
        throw new RuntimeException(f"Failed to find $rootSubdir under the current directory or provenance subdir!")

    val manifestBaseName =
      if (subdir endsWith "/sync")
        subdir.stripSuffix("/sync")
      else if (subdir endsWith "/async")
        subdir.stripSuffix("/async")
      else
        subdir

    val expectedManifestFile = new File(f"$expectedDataRoot/scala-$version/$manifestBaseName.manifest")

    val expectedManifestString =
      if (!expectedManifestFile.exists) {
        logger.warn(s"Failed to find $expectedManifestFile!")
        ""
      } else {
        val expectedManifestBytes = Files.readAllBytes(Paths.get(expectedManifestFile.getAbsolutePath))
        new String(expectedManifestBytes)
      }

    try {
      if (!new File(actualOutputLocalPath).exists)
        throw new RuntimeException(s"Failed to find $actualOutputLocalPath!")

      newManifestString shouldEqual expectedManifestString

    } catch {
      case e: org.scalatest.exceptions.TestFailedException =>
        // For any failure, replace the test content.  This will show up in git status, and it can be committed or not.
        expectedManifestFile.getParentFile.mkdirs()
        logger.error(f"Writing $expectedManifestFile to put in source control.  Reverse this if the change is not intentional.")
        logger.error(f"Previous value was $expectedManifestString")
        expectedManifestFile.delete()
        Files.write(Paths.get(expectedManifestFile.getAbsolutePath), newManifestString.getBytes("UTF-8"))
        throw e
      case ee: Exception =>
        println(ee)
        throw ee
    }
  }
}

