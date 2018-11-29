organization := "com.cibo"
name         := "provenance-all"
licenses     += ("BSD Simplified", url("https://opensource.org/licenses/BSD-3-Clause"))

crossScalaVersions := Seq("2.12.7", "2.11.12")
scalaVersion := crossScalaVersions.value.head

// NOTE: Each sub-project should be independently buildable w/o this parent build.sbt file.
// This top-level project is for convenience in IntelliJ.
// The apps must build independently and have distinct SbtBuildInfo objects for a complete test.

lazy val provenance_all = (project in file(".")).aggregate(provenance, example1)

lazy val provenance = project.in(file("libprov"))
lazy val example1 = project.in(file("example1")).dependsOn(provenance)


