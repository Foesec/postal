package li.flxkbr.postal.testkit

import scala.reflect.ClassTag
import munit.Location

trait ExtendedAssertions {
  this: munit.CatsEffectSuite =>

  def assertEqualType[A, L <: A, R <: A](l: L, r: R)(using
      loc: Location,
  ): Unit = {
    assert(l.getClass() == r.getClass())
  }

  def assertIsInstance[A](
      other: AnyRef,
  )(using ev: ClassTag[A], loc: Location): Unit = {
    val expRtc = ev.runtimeClass
    assert(expRtc.isAssignableFrom(other.getClass))
  }
}
