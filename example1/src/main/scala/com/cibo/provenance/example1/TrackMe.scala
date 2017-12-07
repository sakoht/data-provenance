/* Copyright (c) 2017 CiBO Technologies - All Rights Reserved
 * You may use, distribute, and modify this code under the
 * terms of the BSD 3-Clause license.
 *
 * A copy of the license can be found on the root of this repository,
 * at https://github.com/cibotech/data-provenance/LICENSE.md,
 * or at https://opensource.org/licenses/BSD-3-Clause
 */

package com.cibo.provenance.examples

import com.cibo.provenance._
import com.cibo.provenance.tracker.ResultTrackerSimple
import com.typesafe.scalalogging.LazyLogging
import org.slf4j.LoggerFactory

case class TrackMeParams(
  dbPath: String = f"/tmp/rtx/" + sys.env.getOrElse("USER","anonymous"),
  a: Int = 5,
  b: Int = 5
)

object TrackMe  {

  def main(args: Array[String]): Unit =
    paramsFromArgs(args, "trackme") match {
      case Some(params) =>
        run(params)
        sys.exit(0)
      case None =>
        sys.exit(1)
    }
  
  def paramsFromArgs(args: Array[String], cliName: String): Option[TrackMeParams] = {
    import scopt.OptionParser
    val parser = new OptionParser[TrackMeParams](cliName) {
      arg[String]("<db-path>") optional() action { (v, c) => c.copy(dbPath = v) } text "A local or s3 path to use for storage."
      opt[Int]("a") optional() action { (v, c) => c.copy(a = v) } text "integer to be used as input"
      opt[Int]("b") optional() action { (v, c) => c.copy(b = v) } text "integer to be used as input"
    }

    parser.parse(args, TrackMeParams())
  }

  def run(params : TrackMeParams) : Unit = {

    val logger = LoggerFactory.getLogger(getClass)

    // A BuildInfo is required for the ResultTrackers.
    // The SbtBuildInfo plugin writes an object with build metadata before compiling.
    // This is configured in buildinfo.sbt in this repo.  The object lives here:
    import com.cibo.provenance.examples.BuildInfo
    implicit val bi: BuildInfo = BuildInfo
    logger.info(f"BUILD INFO: $bi")

    // The ResultTrackerSimple can be redirected to S3 if given an s3:// path via the CLI.
    implicit val rt: ResultTrackerSimple = ResultTrackerSimple(params.dbPath)
    logger.info(f"RESULT TRACKER: $rt")

    // Make some calls and see the results.

    val c1 = multMe(10, addMe(5, addMe(5, multMe(2, 2))))
    println(c1)
    println("CALL: " + c1.toString)
    val r1 = c1.resolve
    println("RESULT: " + r1.toString)

    
    val c2 = multMe(addMe(params.a, params.b), addMe(2, addMe(2, 5)))
    println("CALL: " + c2.toString)
    val r2 = c2.resolve
    println("RESULT: " + r2.toString)
  }
}

object addMe extends Function2WithProvenance[Int, Int, Int] {
  val currentVersion = Version("0.1")
  def impl(a: Int, b: Int) = a + b
}

object multMe extends Function2WithProvenance[Int, Int, Int] {
  val currentVersion = Version("0.1")
  def impl(a: Int, b: Int) = a * b
}

