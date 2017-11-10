package com.cibo.provenance

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.Matchers

object TestUtils extends LazyLogging with Matchers {
  import java.io.File
  import java.nio.file.{Files, Paths}

  import com.cibo.io.Shell.getOutputAsBytes

  // The this is the BuildInfo _object_ for this library.
  // Not to be confused with the BuildInfo base trait use for all apps with provenance tracking.
  val libBuildInfo: BuildInfo = com.cibo.provenance.internal.BuildInfo

  // Use the scala version for the library in this test, cross-compiled tests can run in parallel.
  val testOutputBaseDir: String =
    f"/tmp/" + sys.env.getOrElse("USER", "anonymous") + f"/data-provenance-test-output-${libBuildInfo.scalaVersion}"

  def diffOutputSubdir(subdir: String) = {
    val version =
      if (libBuildInfo.scalaVersion.startsWith("2.11"))
        "2.11"
      else if (libBuildInfo.scalaVersion.startsWith("2.12"))
        "2.12"
      else
        throw new RuntimeException(f"Unexpected scala version $libBuildInfo.scalaVersion")

    val actualOutputDir = f"$testOutputBaseDir/$subdir"
    val newManifestBytes = getOutputAsBytes(s"cd $actualOutputDir && wc -c `find . -type file | sort`")
    val newManifestString = new String(newManifestBytes)

    val rootSubdir = "src/test/resources/expected-output"

    val expectedDataRoot =
      if (new File(rootSubdir).exists)
        rootSubdir
      else if (new File(f"provenance/$rootSubdir").exists)
        f"provenance/$rootSubdir"
      else
        throw new RuntimeException(f"Failed to find $rootSubdir under the current directory or provenance subdir!")

    val expectedManifestFile = new File(f"$expectedDataRoot/scala-$version/$subdir.manifest")

    try {

      if (!new File(actualOutputDir).exists)
        throw new RuntimeException(s"Failed to find $actualOutputDir!")

      val expectedManifestString =
        if (!expectedManifestFile.exists) {
          logger.warn(s"Failed to find $expectedManifestFile!")
          ""
        } else {
          val expectedManifestBytes = Files.readAllBytes(Paths.get(expectedManifestFile.getAbsolutePath))
          new String(expectedManifestBytes)
        }

      newManifestString shouldBe expectedManifestString

    } catch {
      case e: Exception =>
        // For any failure, replae the test content.  This will show up in git status, and it can be committed or not.
        expectedManifestFile.getParentFile.mkdirs()
        logger.error(f"Writing $expectedManifestFile to put in source control.  Rever this if the change is not intentional.")
        Files.write(Paths.get(expectedManifestFile.getAbsolutePath), newManifestBytes)
        throw e
    }
  }
}

