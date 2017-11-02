import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runTest,
  releaseStepCommand("createHeaders"),
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  publishArtifacts,
  setNextVersion,
  commitNextVersion,
  pushChanges
)

releaseCrossBuild := true

bintrayOrganization  := Some("cibotech")
bintrayRepository    := "public"
bintrayPackageLabels := Seq("scala", "aws")