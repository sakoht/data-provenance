package com.cibo.provenance.testsupport

import com.cibo.io.s3.SyncablePath
import com.cibo.provenance.BuildInfo
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils

/**
  * Generates ResultTrackerForTest() for all tests in a repo using a consistent directory tree.
  *
  * Create these in your app's test/ and it/ trees:
  *
  *   object MyAppResultTrackerUT extends ResultTrackerForTestFactory(
  *       outputRoot = SyncablePath(s"/tmp/" + sys.env.getOrElse("USER","anonymous") + "/result-trackers-for-unit-tests/myapp"),
  *       referenceRoot = SyncablePath("src/test/resources/provenance-data-by-test")
  *     )(com.mycompany.myapp.BuildInfo)
  *
  *   object MyAppResultTrackerIT extends ResultTrackerForTestFactory(
  *       outputRoot = SyncablePath(s"/tmp/" + sys.env.getOrElse("USER","anonymous") + "/result-trackers-for-integration-tests/myapp"),
  *       referenceRoot = SyncablePath("s3://mybucket/provenance-data-by-test/myapp")
  *     )(com.mycompany.myapp.BuildInfo)
  *
  *
  * Then write tests and integration tests that just specify the test name:
  *
  *   describe("mything") {
  *     it("works") {
  *       implicit val rt = MyAppResultTrackerUT.mk("mything-works")
  *       rt.clean()
  *       MyFunctionWithProvenanceA(...).resolve
  *       MyFunctionWithProvenanceB(...).resolve
  *       MyFunctionWithProvenanceC(...).resolve
  *       rt.check()
  *     }
  *   }
  *
  * @param outputRoot       A local temp directory under which tests for this repo can have subdirectories.
  * @param referenceRoot    For unit tests, "src/test/resources/result-trackers-for-tests"
  *                         For integration tests "s3://somebucket/somedir-for-this-repo".
  */

case class ResultTrackerForTestFactory(outputRoot: String, referenceRoot: String)(implicit bi: BuildInfo) extends LazyLogging {
  def apply(testName: String): ResultTrackerForTest =
    new ResultTrackerForTest(outputRoot + "/" + testName, referenceRoot + "/" + testName)
}

