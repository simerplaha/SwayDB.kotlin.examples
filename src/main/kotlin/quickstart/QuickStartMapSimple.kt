package quickstart

import swaydb.KeyVal
import swaydb.java.Stream
import swaydb.java.memory.MemoryMap
import swaydb.java.serializers.Default.intSerializer
import java.time.Duration

internal object QuickStartMapSimple {

  @JvmStatic
  fun main(args: Array<String>) {
    val map =
      MemoryMap
        .functionsOff(intSerializer(), intSerializer())
        .get()

    map.put(1, 1) //basic put
    map[1].get() //basic get
    map.expire(1, Duration.ofSeconds(1)) //basic expire
    map.remove(1) //basic remove

    //atomic write a Stream of key-value
    map.put(Stream.range(1, 100).map { KeyVal.create(it) })

    //Create a stream that updates all values within range 10 to 90.
    val updatedKeyValues =
      map
        .stream()
        .from(10)
        .takeWhile { it.key() <= 90 }
        .map { KeyVal.create(it.key(), it.value() + 5000000) }

    //submit the stream to update the key-values as a single transaction.
    map.put(updatedKeyValues)

    //print all key-values to view the update.
    map
      .stream()
      .forEach(::println)
  }
}