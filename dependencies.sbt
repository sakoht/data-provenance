val extraResolvers = List(
   "Cibo Libs"          at "https://cibotech.jfrog.io/cibotech/libs-local",
   "Cibo Libs Release"  at "https://cibotech.jfrog.io/cibotech/libs-release-local",
   "Cibo Ext Snapshots" at "https://cibotech.jfrog.io/cibotech/ext-snapshot-local",
   "Cibo Ext Releases"  at "https://cibotech.jfrog.io/cibotech/ext-release-local",
   "Geotools"           at "http://download.osgeo.org/webdav/geotools/",
   "GeoSolutions"       at "http://maven.geo-solutions.it",
   Resolver.jcenterRepo,
   Resolver.bintrayRepo("cibotech", "public"),
   Resolver.bintrayRepo("stanch", "maven"),
   Resolver.bintrayRepo("drdozer", "maven"),
   Resolver.bintrayRepo("monsanto", "maven"),
   Resolver.bintrayRepo("scalaz", "releases"),
   Resolver.bintrayRepo("dwhjames", "maven"),
   Resolver.bintrayRepo("timeoutdigital", "releases"),
   Resolver.sonatypeRepo("releases"),
   Resolver.bintrayRepo("mingchuno", "maven")
 )

resolvers ++= List(
  Classpaths.typesafeReleases
) ++ extraResolvers

libraryDependencies ++= Seq(
  "com.cibo"                   %% "shared"                      % "0.3"
)

libraryDependencies ++= Seq(
  "ch.qos.logback"             %  "logback-classic"             % "1.1.8",
  "org.scalatest"              %% "scalatest"                   % "3.0.3",
  "org.pegdown"                %  "pegdown"                     % "1.6.0"
).map(_ % "test, it")
