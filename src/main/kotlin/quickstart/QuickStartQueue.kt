package quickstart

import swaydb.java.memory.QueueConfig
import swaydb.java.serializers.Default
import java.time.Duration

object QuickStartQueue {

  @JvmStatic
  fun main(args: Array<String>) {
    val queue = QueueConfig.config(Default.intSerializer())
      .get()

    queue.push(1)
    queue.push(2, Duration.ofSeconds(0))
    queue.push(3)

    queue.pop() //returns Optional(1)
    queue.pop() //returns Optional(3) because 2 is expired.
  }
}