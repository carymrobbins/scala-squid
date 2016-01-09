import sbt._
import Keys._

object build extends Build {
  val paradiseVersion = "2.1.0"

  val buildSettings = Defaults.coreDefaultSettings ++ Seq(
    version := "0.1.0",
    scalaVersion := "2.11.7",
    scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Xfatal-warnings"),
    scalacOptions in Test ++= Seq("-Yrangepos"),
    crossScalaVersions := Seq("2.10.2", "2.10.3", "2.10.4", "2.10.5", "2.10.6", "2.11.0", "2.11.1", "2.11.2", "2.11.3", "2.11.4", "2.11.5", "2.11.6", "2.11.7"),
    resolvers += Resolver.sonatypeRepo("snapshots"),
    resolvers += Resolver.sonatypeRepo("releases"),
    addCompilerPlugin("org.scalamacros" % "paradise" % paradiseVersion cross CrossVersion.full),
    libraryDependencies <+= scalaVersion("org.scala-lang" % "scala-reflect" % _),
    libraryDependencies ++= Seq(
      "com.typesafe" % "config" % "1.3.0",
      "org.specs2" %% "specs2-core" % "3.6.6" % "test"
    )
  )

  lazy val squid = project.in(file(".")).settings(buildSettings: _*)
    .aggregate(protocol, macros)

  lazy val protocol = project.settings(buildSettings: _*).settings(
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4"
    )
  )

  lazy val macros = project.settings(buildSettings: _*).settings(
    libraryDependencies ++= Seq(
      "org.postgresql" % "postgresql" % "9.4.1207"
    )
  ).dependsOn(protocol)
}
