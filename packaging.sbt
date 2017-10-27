publishArtifact in Test := false
publishArtifact in IntegrationTest := false

enablePlugins(JavaAppPackaging)
enablePlugins(DockerPlugin)

dockerRepository := Some("473168459077.dkr.ecr.us-east-1.amazonaws.com")
dockerBaseImage := "473168459077.dkr.ecr.us-east-1.amazonaws.com/ubuntu-java:16.04_8"
mainClass in Docker in Compile := Some("com.cibo.provenance.ExampleMain")
dockerUpdateLatest := true
