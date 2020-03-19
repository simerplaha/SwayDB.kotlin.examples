package quickstart

import swaydb.KeyVal
import swaydb.java.*
import swaydb.java.memory.MapConfig
import swaydb.java.serializers.Default.intSerializer
import java.time.Duration

object QuickStart {

  //some function with random logic that we want to use in our map
  private val function =
    PureFunction.OnKeyValue<Int, Int, Return.Map<Int>> { key, value, _ ->
      when {
        key < 25 -> //remove if key is less than 25
          Return.remove()
        key < 50 -> //expire after 2 seconds if key is less than 50
          Return.expire(Duration.ofSeconds(2))
        key < 75 -> //update if key is < 75.
          Return.update(value!! + 10000000)
        else -> //else do nothing
          Return.nothing()
      }
    }

  @JvmStatic
  fun main(args: Array<String>) {
    //create a memory database.
    val map =
      MapConfig
        .functionsOn(intSerializer(), intSerializer())
        .registerFunction(function)
        .get()

    map.put(1, 1) //basic put
    map.get(1).get() //basic get
    map.expire(1, Duration.ofSeconds(1)) //basic expire
    map.remove(1) //basic remove

    //atomic write a Stream of key-value
    map.put(Stream.range(1, 100).map { KeyVal.create(it) })

    //create a read stream from 10th key-value to 90th, increment values by 1000000 and insert.
    val updatedKeyValuesStream =
      map
        .from(10)
        .stream()
        .takeWhile { it.key() <= 90 }
        .map { KeyVal.create(it.key(), it.value() + 5000000) }

    map.put(updatedKeyValuesStream)

    map.applyFunction(1, 100, function) //apply the function to all key-values ranging 1 to 100.

    //print all key-values to view the update.
    map
      .stream()
      .forEach(::println)
  }
}
