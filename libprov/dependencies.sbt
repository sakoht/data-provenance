val circeVersion = "0.9.1"
val awsSdkVersion = "1.11.428"

libraryDependencies ++= Seq(
  "com.amazonaws"              %  "aws-java-sdk-s3"             % awsSdkVersion,
  "commons-io"                 %  "commons-io"                  % "2.6",
  "com.typesafe.scala-logging" %% "scala-logging"               % "3.9.0",
  "com.github.dwhjames"        %% "aws-wrap"                    % "0.12.1",
  "com.google.guava"           %  "guava"                       % "24.0-jre",
  "org.scala-lang"             %  "scala-compiler"              % scalaVersion.value,

  "io.circe"                   %% "circe-core"                  % circeVersion,
  "io.circe"                   %% "circe-generic"               % circeVersion,
  "io.circe"                   %% "circe-parser"                % circeVersion,
  "io.circe"                   %% "circe-generic-extras"        % circeVersion
)

libraryDependencies ++= Seq(
  "ch.qos.logback"             %  "logback-classic"             % "1.1.8",
  "org.scalatest"              %% "scalatest"                   % "3.0.3",
  "org.pegdown"                %  "pegdown"                     % "1.6.0"
).map(_ % "test, it")
