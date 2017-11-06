import com.typesafe.sbt.packager.docker.Cmd

import scala.util.Properties

enablePlugins(JavaAppPackaging)
enablePlugins(DockerPlugin)

mainClass in Docker in Compile := Some("com.cibo.provenance.examples.TrackMe")

// Run this to get the account ID and region:
//  aws iam get-user" | grep "Arn\":" | sed s/^.*arn:aws:iam::// | sed "s/:.*//" 
//  aws configure get region

val awsAccountId = "AWS_ACCOUNT_ID"
val awsRegion = "AWS_REGION"

dockerRepository := Some(f"$awsAccountId.dkr.ecr.${awsRegion}.amazonaws.com")
dockerBaseImage := f"${awsAccountId}.dkr.ecr.${awsRegion}.amazonaws.com/ubuntu-java:16.04_8"
version in Docker := Properties.envOrElse("TRAVIS_BUILD_NUMBER", version.value)
mainClass in Docker in Compile := Some("com.cibo.queueable.ShellCli")
dockerUpdateLatest := true

dockerCommands := {
  val commands = dockerCommands.value
  val installationCommand =
    Cmd("RUN apt-get update && apt-get install -y curl && apt-get install -y awscli")
  (commands.head +: Seq(installationCommand)) ++ commands.tail
}

// Note: this is root _inside_ the container.  Change to an unprivileged user after any installs/updates.
daemonUser in Docker := "root"

