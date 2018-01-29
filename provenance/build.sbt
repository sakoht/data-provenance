organization := "com.cibo"
name         := "provenance"
licenses     += ("BSD Simplified", url("https://opensource.org/licenses/BSD-3-Clause"))

crossScalaVersions := Seq("2.12.4", "2.11.11")
scalaVersion := crossScalaVersions.value.head

lazy val provenance = project.in(file(".")).configs(IntegrationTest)

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Xfatal-warnings")

// Test config
fork in Test := true
testOptions in Test ++= Seq(Tests.Argument("-oDF"), Tests.Argument("-h", "target/unit-test-reports"))

// Integration test config
Defaults.itSettings
fork in IntegrationTest := true
testOptions in IntegrationTest ++= Seq(Tests.Argument("-oDF"))

