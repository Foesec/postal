val scala3Version = "3.2.0"

lazy val root = project
  .in(file("."))
  .settings(
    name         := "postal",
    version      := "0.1.0-SNAPSHOT",
    organization := "li.flxkbr",
    scalaVersion := scala3Version,
    libraryDependencies ++= Dependencies.all,
  )
