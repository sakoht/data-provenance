resolvers += Resolver.url("hmrc-sbt-plugin-releases", url("https://dl.bintray.com/hmrc/sbt-plugin-releases"))(Resolver.ivyStylePatterns)
addSbtPlugin("org.foundweekends" % "sbt-bintray"          % "0.5.1")
addSbtPlugin("com.eed3si9n"      % "sbt-buildinfo"        % "0.7.0")
addSbtPlugin("uk.gov.hmrc"       % "sbt-git-stamp"        % "5.3.0")
addSbtPlugin("de.heikoseeberger" % "sbt-header"           % "3.0.2")
addSbtPlugin("com.typesafe.sbt"  % "sbt-native-packager"  % "1.3.2")
addSbtPlugin("com.github.gseitz" % "sbt-release"          % "1.0.6")

