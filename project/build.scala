import sbt._
import Keys._

import com.github.joprice.Jni
import Jni.Keys._

object build extends Build {
  val paradiseVersion = "2.1.0"

  lazy val LIBPG_QUERY_DIR = Option(System.getProperty("LIBPG_QUERY_DIR")).getOrElse {
    throw new MessageOnlyException("The 'LIBPG_QUERY_DIR' property is required.")
  }

  // NOTE: javaHome.value seems to always be None, regardless of -java-home setting.
  lazy val JAVA_HOME = Option(System.getProperty("JAVA_HOME")).getOrElse {
    throw new MessageOnlyException("The 'JAVA_HOME' property is required.")
  }

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
      "org.specs2" %% "specs2-core" % "3.6.6" % "test"
    )
  )

  lazy val squid = project.in(file("."))
    .settings(buildSettings: _*)
    .aggregate(parser, meta, macros, core)

  lazy val parser = project
    .settings(buildSettings: _*)
    .settings(Jni.settings: _*)
    .settings(
      jniClasses := Seq("squid.parser.jni.PGParserJNI"),
      libraryName := "libPGParserJNI",
      gccFlags := Seq(
        "-shared",
        "-fpic",
        "-O3",
        s"-L$LIBPG_QUERY_DIR",
        s"-I$LIBPG_QUERY_DIR",
        s"-I$JAVA_HOME/include",
        s"-I$JAVA_HOME/include/linux",
        s"-I${headersPath.value}"
      ),
      jniSourceFiles ++= Seq(
        file(s"$LIBPG_QUERY_DIR/libpg_query.a")
      ),
      libraryDependencies ++= Seq(
        "io.argonaut" %% "argonaut" % "6.2-SNAPSHOT"
      )
  )

  lazy val meta = project.settings(buildSettings: _*).settings(
    libraryDependencies ++= Seq(
      "org.postgresql" % "postgresql" % "9.4-1201-jdbc41",
      "com.typesafe" % "config" % "1.3.0"
    )
  ).dependsOn(parser)

  lazy val macros = project.settings(buildSettings: _*)
    .dependsOn(parser, meta, meta % "test->test")

  lazy val core = project.settings(buildSettings: _*).dependsOn(macros)
}
