package quickstart

import swaydb.KeyVal
import swaydb.java.Deadline
import swaydb.java.PureFunction
import swaydb.java.Return
import swaydb.java.Stream
import swaydb.java.memory.MemoryMap
import swaydb.java.serializers.Default.intSerializer
import java.time.Duration
import java.util.*

internal object QuickStartMapFunctions {

  @JvmStatic
  fun main(args: Array<String>) {

    //create a function that reads key & value and applies modifications
    val function =
      PureFunction.OnKeyValue<Int, Int, Return.Map<Int>> { key: Int, value: Int, _: Optional<Deadline?>? ->
        when {
          key < 25 -> Return.remove() //remove if key is less than 25
          key < 50 -> Return.expire(Duration.ofSeconds(2)) //expire after 2 seconds if key is less than 50
          key < 75 -> Return.update(value + 10000000) //update if key is < 75.
          else -> Return.nothing() //else do nothing
        }
      }

    val map =
      MemoryMap
        .functionsOn(intSerializer(), intSerializer())
        .registerFunction(function)
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
    map.applyFunction(1, 100, function) //apply the function to all key-values ranging 1 to 100.

    //print all key-values to view the update.
    map
      .stream()
      .forEach(::println)
  }
}