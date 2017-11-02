
publishTo := {
  val host = "https://cibotech.jfrog.io/cibotech/"
  if (isSnapshot.value)
    Some("snapshots" at host + "libs-snapshot-local")
  else
    Some("releases" at host + "libs-release-local")
}

