package quickstart

import swaydb.java.KeyVal
import swaydb.java.PureFunction
import swaydb.java.Return
import swaydb.java.Stream
import swaydb.java.serializers.Default.intSerializer
import java.time.Duration
import kotlin.system.exitProcess

object QuickStart {

    @JvmStatic
    fun main(args: Array<String>) {
        //create a memory database.
        val map = swaydb.java.memory.Map
            .configWithFunctions(intSerializer(), intSerializer())
            .init()
            .get()

        map.put(1, 1).get() //basic put
        map.get(1).get() //basic get
        map.expire(1, Duration.ofSeconds(1)).get() //basic expire
        map.remove(1).get() //basic remove

        //atomic write a Stream of key-value
        map.put(Stream.range(1, 100).map { item -> KeyVal.create(item) }).get()

        //create a read stream from 10th key-value to 90th, increment values by 1000000 and insert.
        map
            .from(10)
            .takeWhile { keyVal -> keyVal.key() <= 90 }
            .map { keyVal -> KeyVal.create(keyVal.key(), keyVal.value() + 5000000) }
            .materialize()
            .flatMap { item -> map.put(item) }
            .get()

        val function = PureFunction.OnKeyValue<Int, Int, Return.Map<Int>> { key, value, _ ->
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

        map.registerFunction(function) //register the function.

        map.applyFunction(1, 100, function) //apply the function to all key-values ranging 1 to 100.

        //print all key-values to view the update.
        map
            .forEach { item -> println(item) }
            .materialize()
            .get()

        //stop app.
        exitProcess(0)
    }
}
