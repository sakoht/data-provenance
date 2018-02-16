libraryDependencies ++= Seq(
  "com.cibo"                          %% "provenance"               % "0.2-SNAPSHOT",

  "com.github.scopt"                  %% "scopt"                    % "3.5.0",
  "com.typesafe"                      %  "config"                   % "1.3.1",
  "com.typesafe.scala-logging"        %% "scala-logging"            % "3.4.0",
  "ch.qos.logback"                    %  "logback-classic"          % "1.1.8"
)

libraryDependencies ++= Seq(
  "ch.qos.logback"             %  "logback-classic"             % "1.1.8",
  "org.scalatest"              %% "scalatest"                   % "3.0.3",
  "org.pegdown"                %  "pegdown"                     % "1.6.0"
).map(_ % "test")

