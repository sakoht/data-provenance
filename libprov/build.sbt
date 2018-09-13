organization := "com.cibo"
name         := "provenance"
licenses     += ("BSD Simplified", url("https://opensource.org/licenses/BSD-3-Clause"))

crossScalaVersions := Seq("2.12.6")
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

// Some source templates under src/main/boilerplate.
// The output goes to target/scala-2.12/src_managed.
// NOTE: This means this project requires sbt to compile initially.
// You can re-compile in IntelliJ afterward if none of the templates change.
enablePlugins(spray.boilerplate.BoilerplatePlugin)

