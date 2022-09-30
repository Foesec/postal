import sbt.*

object Dependencies {

  private lazy val main = Seq(
    "org.typelevel"   %% "cats-core"   % "2.8.0",
    "co.fs2"          %% "fs2-core"    % "3.3.0",
    "com.github.fd4s" %% "fs2-kafka"   % "3.0.0-M8",
    "org.tpolecat"    %% "doobie-core" % "1.0.0-RC1",
    "org.legogroup"   %% "woof-core"   % "0.4.6",
  )

  private lazy val test = Seq(
    "org.scalameta" %% "munit"               % "0.7.29",
    "org.typelevel" %% "munit-cats-effect-3" % "1.0.7",
  ).map(_ % Test)

  val all = main ++ test
}
