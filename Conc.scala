import argonaut.{ACursor, DecodeJson, DecodeResult, Json, JsonObject, Parse}
import argonaut.Argonaut._
import java.nio.file.{Files, Paths}
import java.time.{Duration, Instant}
import monocle.{Lens, Traversal}
import monocle.Monocle._
import scalaz.{\/, ApplicativePlus, Monad, OptionT}
import scalaz.concurrent.{Task, TaskApp}
import scalaz.Scalaz.{some => _, _}

trait LiftMT[M[_], MT[_[_], _]] {
  def liftMT[F[_]: Monad, A](ma: M[A]): MT[F, A]
}
object LiftMT {
  implicit def OptionTLiftMT: LiftMT[Option, OptionT] =
    new LiftMT[Option, OptionT] {
      def liftMT[F[_], A](ma: Option[A])(implicit F: Monad[F]) =
        OptionT(F.point(ma))
    }

  // Hacky but the type inference works this way.
  implicit final class syntax[M[_], MT[_[_], _], A](v: M[A])(implicit LMT: LiftMT[M, MT]) {
    def liftMT[F[_]: Monad]: MT[F, A] =
      LMT.liftMT[F, A](v)
  }
}

final case class Hit(end: Instant, duration: Duration) {
  def start: Instant =
    end.minus(duration)
  def events[M[_]: ApplicativePlus]: M[Event] =
    (Start(start): Event).point[M] <+> (End(end): Event).point[M]
}
object Hit {
  implicit def decodeJson: DecodeJson[Hit] = {
    def tryParse[A: DecodeJson, B](c: ACursor, fn: A => B, msg: => String): DecodeResult[B] =
      c.as[A].flatMap { a =>
        \/.fromTryCatchNonFatal(fn(a)).fold(
          _ => DecodeResult.fail(msg, c.history),
          DecodeResult.ok
        )
      }
    def decodeInstant(c: ACursor): DecodeResult[Instant] =
      tryParse(c, Instant.parse(_: String), "Invalid instant")
    def decodeDuration(c: ACursor): DecodeResult[Duration] =
      tryParse(c, (_: String).toDouble, "Invalid double").map { fSeconds =>
        val nanos = (fSeconds * 1000 * 1000 * 1000).toLong
        Duration.ofNanos(nanos)
      }

    DecodeJson { c =>
      ^(
        decodeInstant(c --\ "_source" --\ "@timestamp"),
        decodeDuration(c --\ "_source" --\ "@fields" --\ "request_time")
      )(apply)
    }
  }
}

sealed trait Event {
  def at: Instant
}
final case class Start(at: Instant) extends Event
final case class End(at: Instant) extends Event

object App extends TaskApp {
  import LiftMT.syntax
  type Filename = String

  @inline def jsonAt(field: JsonField): Lens[JsonObject, Option[Json]] =
    at[JsonObject, JsonField, Json](field)

  def puts(s: String): Task[Unit] =
    Task(println(s))

  def hits: Traversal[Json, Json] =
    jObjectPrism ^|-> jsonAt("hits") ^<-? some ^<-?
    jObjectPrism ^|-> jsonAt("hits") ^<-? some ^<-?
    jArrayPrism ^|->> each

  // Argonaut requires all input to be loaded into memory
  def content(file: Filename): Task[String] =
    Task {
      val bytes = Files.readAllBytes(Paths.get(file))
      new String(bytes, "UTF-8")
    }

  def usage: Task[Unit] =
    puts("Pass single filename as argument")

  def singleton[A](as: List[A]): Option[A] =
    as match {
      case List(a) => Some(a)
      case _ => None
    }

  def process(args: List[String]): OptionT[Task, Unit] =
    for {
      file <- singleton(args).liftMT[Task]
      string <- content(file).liftM[OptionT]
      json <- Parse.parseOption(string).liftMT[Task]
      allHits = hits.getAll(json).collapse[Vector]
      parsedHits <- allHits.traverse(_.as[Hit].toOption).liftMT[Task]
      events = parsedHits.flatMap(_.events[Vector]).sortBy(_.at)
      peak = events.foldLeft((0, 0)) { (state, event) =>
        val (max, curr) = state
        event match {
          case Start(_) => (if (max === curr) max+1 else max, curr+1)
          case End(_) => (max, curr-1)
        }
      }._1
      _ <- puts(s"${parsedHits.length} hits, peak = $peak").liftM[OptionT]
    } yield ()

  override def runl(args: List[String]) =
    process(args).getOrElseF(usage)
}
