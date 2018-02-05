package com.cibo.provenance

import com.cibo.provenance.exceptions.{InvalidVersionException, UnknownVersionException, UnrunnableVersionException}
import org.scalatest.{FunSpec, Matchers}

/**
  * Created by ssmith on 9/20/17.
  *
  * This tests the non-default cases of
  */

class MultipleVersionsAtATimeSpec extends FunSpec with Matchers {

  implicit val buildInfo: BuildInfo = DummyBuildInfo
  implicit val storage: ResultTrackerNone = ResultTrackerNone()

  describe("Functions with provenance support one 'currentVersion' by default") {

    object myFuncWithOneVersion extends Function2WithProvenance[String, Double, Int] {
      val currentVersion: Version = Version("1.0")
      def impl(d: Double, n: Int): String = d.toString + "," + n.toString
    }

    it("should use the current version by default, track it, and run correctly.") {
      val s1 = myFuncWithOneVersion(1.23, 3)
      s1.getVersionValue shouldEqual Version("1.0")

      val r1 = s1.run
      r1.output shouldEqual "1.23,3"
      r1.provenance.getVersionValue shouldEqual Version("1.0")
    }

    it("should construct with other version values, but throw an exception if something besides the currentVersion is run.") {
      val s1 = myFuncWithOneVersion(1.23, 3, Version("9.99"))
      s1.getVersionValue shouldEqual Version("9.99")

      intercept[InvalidVersionException[_]] { s1.run }
    }
  }

  describe("Functions with provenance should support loading older versions that do not run any longer.") {

    object myFuncWithOldVersions extends Function2WithProvenance[String, Double, Int] {
      val currentVersion: Version = Version("1.0")
      override lazy val loadableVersions: Seq[Version] = Seq("1.0", "0.9", "0.8").map(v => Version(v))

      def impl(d: Double, n: Int): String = d.toString + "," + n.toString
    }

    it("should construct with other all loadable version values") {
      myFuncWithOldVersions.loadableVersions.foreach {
        version =>
          val s1 = myFuncWithOldVersions(1.23, 3, version)
          s1.getVersionValue shouldEqual version
      }
    }

    myFuncWithOldVersions.loadableVersions.foreach {
      version =>
        val s1 = myFuncWithOldVersions(1.23, 3, version)

        if (version == myFuncWithOldVersions.currentVersion) {
          it(f"should run on the current version $version") {
            s1.run
          }
        } else {
          it(f"should fail to run on other version $version") {
            intercept[UnrunnableVersionException[_]] {
              s1.run
            }
          }
        }
    }
  }

  describe("Functions with provenance should let the developer explicitly support running alternate in the same code base.") {

    object myFuncWithOldVersions extends Function2WithProvenance[String, Double, Int] {
      val currentVersion: Version = Version("1.3")
      override lazy val loadableVersions: Seq[Version] = Seq("1.0", "1.1", "1.2", "1.3").map(n => Version(n))
      override lazy val runnableVersions: Seq[Version] = Seq("1.1", "1.2", "1.3").map(n => Version(n))

      def impl(d: Double, n: Int): String = d.toString + "," + n.toString

      override def implVersion(d: Double, n: Int, v: Version): String = {
        v.id match {
          case "1.3" => impl(d, n)
          case "1.2" => impl1_2(d, n)
          case "1.1" => impl1_1(d, n)
          case "1.0" => throw UnrunnableVersionException(v, this)
          case _ => throw UnknownVersionException(v, this)
        }
      }

      // How to organize old versions is up to the developer.

      def impl1_2(d: Double, n: Int): String = d.toString + "/" + n.toString

      def impl1_1(d: Double, n: Int): String = d.toString + ":" + n.toString
    }

    it("works at the default version") {
      myFuncWithOldVersions(1.1, 3).run.output shouldEqual "1.1,3"
    }

    it("works at the current version explicitly") {
      myFuncWithOldVersions(1.1, 3, Version("1.3")).run.output shouldEqual "1.1,3"
    }

    it("works at old versions") {
      myFuncWithOldVersions(1.1, 3, Version("1.2")).run.output shouldEqual "1.1/3"
      myFuncWithOldVersions(1.1, 3, Version("1.1")).run.output shouldEqual "1.1:3"
    }

    it("Fails at old versions that are not supported") {
      intercept[UnrunnableVersionException[_]] {
        myFuncWithOldVersions(1.1, 3, Version("1.0")).run
      }
    }

    it("Fails at unrecognized versions") {
      intercept[UnknownVersionException[_]] {
        myFuncWithOldVersions(1.1, 3, Version("9.99")).run
      }
    }
  }
}

