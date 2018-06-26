val extraResolvers = List(
   Resolver.defaultLocal,
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

