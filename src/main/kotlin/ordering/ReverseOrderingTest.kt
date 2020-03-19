package ordering

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import swaydb.java.memory.MapConfig
import swaydb.java.serializers.Default

internal class ReverseOrderingTest {

  @Test
  fun reverse() {
    val map =
      MapConfig.functionsOff(Default.intSerializer(), Default.intSerializer()) //provide a typed comparator that reverses ordering
        .setTypedComparator { key1: Int, key2: Int? -> key1.compareTo(key2!!) * -1 }
        .get()

    //insert in natural ordering from 1 to 100
    (1..100).forEach { map.put(it, it) }

    val actual =
      map
        .keys()
        .stream()
        .materialize()

    //print out the stream. Since ordering is in reverse this will print from 100 to 1.
    actual.forEach(::println)

    val expected = (1..100).map { 100 - it + 1 }

    assertEquals(expected, actual)
  }
}