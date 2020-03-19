package queue

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import swaydb.java.memory.QueueConfig
import swaydb.java.serializers.Default
import java.time.Duration

internal class QueueTest {

  @Test
  fun quickStart() {
    val queue =
      QueueConfig.config(Default.intSerializer())
        .get()

    queue.push(1)
    queue.push(2, Duration.ofSeconds(0))
    queue.push(3)

    assertEquals(1, queue.popOrNull()) //1
    assertEquals(3, queue.popOrNull()) //returns 3 because 2 is expired.
  }
}