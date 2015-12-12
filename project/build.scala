// import com.simplytyped.Antlr4Plugin._
import sbt._
import Keys._

object build extends Build {
  val paradiseVersion = "2.1.0"

  val buildSettings = Defaults.coreDefaultSettings ++ Seq(
    version := "0.1.0",
    scalaVersion := "2.11.7",
    scalacOptions ++= Seq(),
    scalacOptions in Test ++= Seq("-Yrangepos"),
    crossScalaVersions := Seq("2.10.2", "2.10.3", "2.10.4", "2.10.5", "2.10.6", "2.11.0", "2.11.1", "2.11.2", "2.11.3", "2.11.4", "2.11.5", "2.11.6", "2.11.7"),
    resolvers += Resolver.sonatypeRepo("snapshots"),
    resolvers += Resolver.sonatypeRepo("releases"),
    addCompilerPlugin("org.scalamacros" % "paradise" % paradiseVersion cross CrossVersion.full),
    libraryDependencies ++= Seq(
      "com.typesafe" % "config" % "1.3.0",
      "org.specs2" %% "specs2-core" % "3.6.6" % "test"
    )
  )

  lazy val squid = project.in(file("."))
    .settings(buildSettings: _*)
    .aggregate(parser, meta, macros, core)

  lazy val parser = project
    // .settings(antlr4Settings: _*)
    .settings(buildSettings: _*)
    .settings(
      // antlr4PackageName in Antlr4 := Some("squid.parser.gen"),
      libraryDependencies ++= Seq(
        // Manual parsing
        // "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4"

        // Consuming JSON parse trees
        "io.argonaut" %% "argonaut" % "6.2-SNAPSHOT"
    )
  )

  lazy val meta = project.settings(buildSettings: _*).settings(
    libraryDependencies ++= Seq(
      postgres
    )
  ).dependsOn(parser)

  lazy val macros = project.settings(buildSettings: _*)
    .dependsOn(parser, meta, metaTest)

  lazy val core = project.settings(buildSettings: _*).dependsOn(macros)

  // Common dependencies

  val metaTest = meta % "test->test"

  val postgres = "org.postgresql" % "postgresql" % "9.4-1201-jdbc41"
}
