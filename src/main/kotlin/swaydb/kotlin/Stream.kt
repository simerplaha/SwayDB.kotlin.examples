/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */
package swaydb.kotlin

import scala.Tuple2
import scala.collection.mutable.ListBuffer
import scala.runtime.AbstractFunction1
import swaydb.data.IO
import java.util.*
import java.util.function.Consumer
import java.util.function.UnaryOperator

/**
 * The Stream of data.
 *
 * @param <K> the type of the key element
 * @param <V> the type of the value element
 */
class Stream<K, V> {
    private val streamObject: swaydb.Stream<K, V>?
    private val success: IO.Success<Tuple2<K, V>>?

    /**
     * Constructs the Stream object.
     * @param streamObject the streamObject
     */
    constructor(streamObject: swaydb.Stream<K, V>) {
        this.streamObject = streamObject
        this.success = null
    }

    /**
     * Constructs the Stream object.
     * @param success the success
     */
    private constructor(success: IO.Success<Tuple2<K, V>>) {
        this.streamObject = null
        this.success = success
    }

    /**
     * Starts the map function for this map.
     * @param function the function
     *
     * @return the stream object for this map
     */
    @Suppress("UNCHECKED_CAST")
    fun map(function: UnaryOperator<MutableMap.MutableEntry<K, V>>): Stream<K, V> {
        return Stream(streamObject?.map(object : AbstractFunction1<K, Any?>() {
            override fun apply(tuple2: K): Any? {
                val result = function.apply(
                        AbstractMap.SimpleEntry<K, V>((tuple2 as Tuple2<K, V>)._1(), (tuple2 as Tuple2<K, V>)._2()))
                return IO.`Success$`.`MODULE$`.apply(Tuple2.apply(result.key, result.value))
            }
        }) as swaydb.Stream<K, V>)
    }

    /**
     * Starts the materialize function for this map.
     *
     * @return the stream object for this map
     */
    @Suppress("UNCHECKED_CAST")
    fun materialize(): Stream<K, V> {
        return Stream(streamObject?.materialize() as IO.Success<Tuple2<K, V>>)
    }

    /**
     * Starts the foreach function for this map.
     * @param consumer the consumer
     *
     * @return the stream object for this map
     */
    @Suppress("UNCHECKED_CAST")
    fun foreach(consumer: Consumer<MutableMap.MutableEntry<K, V>>): Stream<K, V> {
        success?.foreach(object : AbstractFunction1<Tuple2<K, V>, Any?>() {
            override fun apply(t1: Tuple2<K, V>): Any? {
                val entries = (t1 as ListBuffer<*>).seq()
                var index = 0
                while (index < entries.size()) {
                    if (entries.apply(index) is Tuple2<*, *>) {
                        val tuple2 = entries.apply(index) as Tuple2<K, V>
                        consumer.accept(AbstractMap.SimpleEntry<K, V>(tuple2._1(), tuple2._2()))
                    } else {
                        val tuple2 = entries.apply(index) as IO.Success<Tuple2<K, V>>
                        consumer.accept(AbstractMap.SimpleEntry<K, V>(tuple2.get()._1(), tuple2.get()._2()))
                    }
                    index += 1
                }
                return null
            }
        })
        return this
    }
}
