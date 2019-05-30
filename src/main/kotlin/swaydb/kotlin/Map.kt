/*
 * Copyright (C) 2019 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */
package swaydb.kotlin

import swaydb.Apply
import swaydb.data.IO
import swaydb.data.accelerate.Level0Meter
import java.time.LocalDateTime
import java.util.concurrent.TimeUnit
import java.util.Optional
import swaydb.data.compaction.LevelMeter

interface Map<K, V> {
    /**
     * Returns the size of elements in this map.
     *
     * @return the size of elements in this map
     */
    fun size(): Int

    /**
     * Checks the map is empty.
     *
     * @return `true` if a map is empty, `false` otherwise
     */
    fun isEmpty(): Boolean

    /**
     * Checks the map is not empty.
     *
     * @return `true` if a map is not empty, `false` otherwise
     */
    fun nonEmpty(): Boolean

    /**
     * Returns the expiration date for key in this map.
     * @param key the key
     *
     * @return the expiration date for key in this map
     */
    fun expiration(key: K): LocalDateTime?

    /**
     * Returns the time left for key in this map.
     * @param key the key
     *
     * @return the time left for key in this map
     */
    fun timeLeft(key: K): java.time.Duration?

    /**
     * Returns the key size in bytes for this map.
     * @param key the key
     *
     * @return the key size in bytes for this map
     */
    fun keySize(key: K): Int

    /**
     * Returns the value size in bytes for this map.
     * @param value the value
     *
     * @return the value size in bytes for this map
     */
    fun valueSize(value: V): Int

    /**
     * Returns the size for segments for this map.
     *
     * @return the size for segments for this map
     */
    fun sizeOfSegments(): Long

    /**
     * Returns the level of meter for zerro level.
     *
     * @return the level of meter for zerro level
     */
    fun level0Meter(): Level0Meter

    /**
     * Returns the level of meter for level.
     * @param levelNumber the level number
     *
     * @return the level of meter for first level
     */
    fun levelMeter(levelNumber: Int): Optional<LevelMeter>

    /**
     * Checks if a map contains key.
     * @param key the key
     *
     * @return `true` if a map contains key, `false` otherwise
     */
    fun containsKey(key: K): Boolean

    /**
     * Checks if a map might contain key.
     * @param key the key
     *
     * @return `true` if a map might contains key, `false` otherwise
     */
    fun mightContain(key: K): Boolean

    /**
     * Returns the head key for this map.
     *
     * @return the head key for this map
     */
    fun head(): MutableMap.MutableEntry<K, V>?

    /**
     * Returns the optional head key for this map.
     *
     * @return the optional head key for this map
     */
    fun headOption(): Optional<MutableMap.MutableEntry<K, V>>

    /**
     * Returns the last key for this map.
     *
     * @return the last key for this map
     */
    fun last(): MutableMap.MutableEntry<K, V>?

    /**
     * Returns the optional last key for this map.
     *
     * @return the optional last key for this map
     */
    fun lastOption(): Optional<MutableMap.MutableEntry<K, V>>

    /**
     * Checks if a map contains value.
     * @param value the value
     *
     * @return `true` if a map contains value, `false` otherwise
     */
    fun containsValue(value: V): Boolean

    /**
     * Puts a map object to this map.
     * @param map the map
     */
    fun put(map: MutableMap<K, V>)

    /**
     * Puts an entry object to this map.
     * @param entry the entry
     */
    fun put(entry: MutableMap.MutableEntry<K, V>)

    /**
     * Updates map entries for this map.
     * @param map the map
     */
    fun update(map: MutableMap<K, V>)

    /**
     * Clears this map.
     */
    fun clear()

    /**
     * Returns the key set for this map.
     *
     * @return the key set for this map
     */
    fun keySet(): MutableSet<K>

    /**
     * Returns the head key for this map.
     *
     * @return the head key for this map
     */
    fun keysHead(): K?

    /**
     * Returns the optional head key for this map.
     *
     * @return the optional head key for this map
     */
    fun keysHeadOption(): Optional<K>

    /**
     * Returns the last key for this map.
     *
     * @return the last key for this map
     */
    fun keysLast(): K?

    /**
     * Returns the optional last key for this map.
     *
     * @return the optional last key for this map
     */
    fun keysLastOption(): Optional<K>

    /**
     * Returns the values for this map.
     *
     * @return the values last key for this map
     */
    fun values(): List<V>

    /**
     * Returns the entrues for this map.
     *
     * @return the entrues last key for this map
     */
    fun entrySet(): MutableSet<MutableMap.MutableEntry<K, V>>

    /**
     * Puts the key/value pair for this map.
     * @param key the key
     * @param value the value
     *
     * @return the old value for this key or null
     */
    fun put(key: K, value: V): V?

    /**
     * Puts the key/value pair for this map with expiration after data.
     * @param key the key
     * @param value the value
     * @param expireAfter the expireAfter
     * @param timeUnit the timeUnit
     *
     * @return the old value for this key or null
     */
    fun put(key: K, value: V, expireAfter: Long, timeUnit: TimeUnit): V?

    /**
     * Puts the key/value pair for this map with expiration at data.
     * @param key the key
     * @param value the value
     * @param expireAt the expireAt
     *
     * @return the old value for this key or null
     */
    fun put(key: K, value: V, expireAt: LocalDateTime): V?

    /**
     * Setups the expiration after for key to this map.
     * @param key the key
     * @param after the after
     * @param timeUnit the timeUnit
     *
     * @return the old value for this key or null
     */
    fun expire(key: K, after: Long, timeUnit: TimeUnit): V?

    /**
     * Setups the expiration at for key to this map.
     * @param key the key
     * @param expireAt the expireAt
     *
     * @return the old value for this key or null
     */
    fun expire(key: K, expireAt: LocalDateTime): V?

    /**
     * Updates the key/value for this map.
     * @param key the key
     * @param value the value
     *
     * @return the old value for this key or null
     */
    fun update(key: K, value: V): V?

    /**
     * Returns the value or null for key of this map.
     * @param key the key
     *
     * @return the value or null for key of this map
     */
    operator fun get(key: K): V?

    /**
     * Removes the value for key of this map.
     * @param key the key
     *
     * @return the old value or null for key of this map
     */
    fun remove(key: K): V?

    /**
     * Removes the values for key set of this map.
     * @param keys the keys
     */
    fun remove(keys: MutableSet<K>)

    /**
     * Removes the values for keys of this map.
     * @param from the from
     * @param to the to
     */
    fun remove(from: K, to: K)

    /**
     * Returns the java map of this map.
     *
     * @return the java map of this map
     */
    fun asJava(): MutableMap<K, V>

    /**
     * Registers the function for this map.
     * @param functionId the functionId
     * @param function the function
     *
     * @return the function id
     */
    fun registerFunction(functionId: K, function: (V) -> Apply.Map<V>): K

    /**
     * Executes the registered function for this map.
     * @param key the key
     * @param functionId the functionId
     */
    fun applyFunction(key: K, functionId: K)

    /**
     * Returns the map object which starts from key for this map.
     * @param key the key
     *
     * @return the map object
     */
    fun from(key: K): Map<K, V>

    /**
     * Returns the map object which starts or after key for this map.
     * @param key the key
     *
     * @return the map object
     */
    fun fromOrAfter(key: K): Map<K, V>

    /**
     * Returns the map object which starts or before key for this map.
     * @param key the key
     *
     * @return the map object
     */
    fun fromOrBefore(key: K): Map<K, V>

    /**
     * Returns the key objects for this map.
     *
     * @return the key objects for this map
     */
    fun keys(): swaydb.Set<K, IO<*>>

    /**
     * Returns the reversed map object for this map.
     *
     * @return the reversed map object for this map
     */
    fun reverse(): Map<K, V>

    /**
     * Starts the map function for this map.
     * @param function the function
     *
     * @return the stream object for this map
     */
    fun map(function: (MutableMap.MutableEntry<K, V>) -> MutableMap.MutableEntry<K, V>): Stream<K, V>

    /**
     * Starts the drop function for this map.
     * @param count the count
     *
     * @return the stream object for this map
     */
    fun drop(count: Int): Stream<K, V>

    /**
     * Starts the dropWhile function for this map.
     * @param predicate the predicate
     *
     * @return the stream object for this map
     */
    fun dropWhile(predicate: (MutableMap.MutableEntry<K, V>) -> Boolean): Stream<K, V>

    /**
     * Starts the take function for this map.
     * @param count the count
     *
     * @return the stream object for this map
     */
    fun take(count: Int): Stream<K, V>

    /**
     * Starts the takeWhile function for this map.
     * @param predicate the predicate
     *
     * @return the stream object for this map
     */
    fun takeWhile(predicate: (MutableMap.MutableEntry<K, V>) -> Boolean): Stream<K, V>

    /**
     * Starts the foreach function for this map.
     * @param consumer the consumer
     *
     * @return the stream object for this map
     */
    fun foreach(consumer: (MutableMap.MutableEntry<K, V>) -> Unit): Stream<K, V>

    /**
     * Starts the filter function for this map.
     * @param predicate the predicate
     *
     * @return the stream object for this map
     */
    fun filter(predicate: (MutableMap.MutableEntry<K, V>) -> Boolean): Stream<K, V>

    /**
     * Starts the commit function for this map.
     * @param prepares the prepares
     *
     * @return the level zerro for this map
     */
    fun commit(vararg prepares: swaydb.Prepare<K, V>): Level0Meter

}