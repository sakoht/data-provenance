
organization := "com.cibo"
name         := "provenance-example"

crossScalaVersions := Seq("2.11.11", "2.12.4")
scalaVersion := crossScalaVersions.value.head

scalacOptions ++= Seq("-unchecked", "-deprecation", "-Xfatal-warnings")

fork in run := false
mainClass in Compile := Some("com.cibo.provenance.examples.TrackMe")

// Test config
fork in Test := true
testOptions in Test ++= Seq(Tests.Argument("-oDF"), Tests.Argument("-h", "target/unit-test-reports"))

// Integration test config
Defaults.itSettings
fork in IntegrationTest := true
testOptions in IntegrationTest ++= Seq(Tests.Argument("-oDF"))
