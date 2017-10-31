libraryDependencies ++= Seq(
  "com.cibo"                   %% "shared"                      % "0.4-SNAPSHOT"
)

libraryDependencies ++= Seq(
  "ch.qos.logback"             %  "logback-classic"             % "1.1.8",
  "org.scalatest"              %% "scalatest"                   % "3.0.3",
  "org.pegdown"                %  "pegdown"                     % "1.6.0"
).map(_ % "test, it")
