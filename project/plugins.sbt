logLevel := Level.Warn

resolvers += Resolver.url("joprice maven", url("http://dl.bintray.com/content/joprice/maven"))(Resolver.ivyStylePatterns)

addSbtPlugin("com.github.joprice" % "sbt-jni" % "SNAPSHOT-CARY")
