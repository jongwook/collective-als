
resolvers ++= Seq("Sonatype OSS Releases" at "https://oss.sonatype.org/service/local/staging/deploy/maven2",
  "Typesafe Repository" at "https://repo.typesafe.com/typesafe/releases/")

addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.6.0")

addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.1")

addSbtPlugin("org.tpolecat" % "tut-plugin" % "0.4.8")

addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.0")
