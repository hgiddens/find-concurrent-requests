name := "conc"
libraryDependencies ++= Seq(
  "com.github.julien-truffaut" %% "monocle-core" % "1.1.1",
  "io.argonaut" %% "argonaut" % "6.1",
  "org.scalaz" %% "scalaz-concurrent" % "7.1.5"
)
scalaVersion := "2.11.7"
scalacOptions ++= Seq(
  "-feature",
  "-language:higherKinds",
  "-Xlint"
)
