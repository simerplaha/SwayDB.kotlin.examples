package stream

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import swaydb.java.Stream

internal class StreamTest {

  @Test
  fun stream() {
    val sum =
      Stream
        .range(1, 1000)
        .map { it - 1 }
        .filter { it % 2 == 0 }
        .foldLeft(0, { a: Int?, b: Int? -> Integer.sum(a!!, b!!) })

    assertEquals(249500, sum)
  }
}