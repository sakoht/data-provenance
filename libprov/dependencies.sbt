val circeVersion = "0.9.1"

libraryDependencies ++= Seq(
  "com.cibo"                   %% "shared"                      % "0.17",
  "org.scala-lang"             % "scala-compiler"               % scalaVersion.value,

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
