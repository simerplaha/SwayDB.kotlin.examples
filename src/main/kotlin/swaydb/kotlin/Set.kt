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

import java.time.Duration
import java.time.LocalDateTime
import java.util.Optional
import java.util.concurrent.TimeUnit
import swaydb.Prepare
import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter

/**
 * The Set of data.
 *
 * @param <K> the type of the key element
 */
interface Set<K> {

    /**
     * Checks if a set is empty.
     *
     * @return `true` if a set is empty, `false` otherwise
     */
    val isEmpty: Boolean

    /**
     * Checks if a set contains key.
     * @param key the key
     *
     * @return `true` if a set contains key, `false` otherwise
     */
    operator fun contains(key: K): Boolean

    /**
     * Checks if a set might contain key.
     * @param key the key
     *
     * @return `true` if a set might contain key, `false` otherwise
     */
    fun mightContain(key: K): Boolean

    /**
     * Returns the iterator of elements in this set.
     *
     * @return the iterator of elements in this set
     */
    operator fun iterator(): Iterator<K>

    /**
     * Returns the array of elements in this set.
     *
     * @return the array of elements in this set
     */
    fun toArray(): Array<K>

    /**
     * Adds the key to this set.
     * @param key the key
     *
     * @return `true` if a set contained key, `false` otherwise
     */
    fun add(key: K): Boolean

    /**
     * Adds the key with expire after to this set.
     * @param key the key
     * @param expireAfter the expireAfter
     * @param timeUnit the timeUnit
     *
     * @return `true` if a set contained key, `false` otherwise
     */
    fun add(key: K, expireAfter: Long, timeUnit: TimeUnit): Boolean

    /**
     * Adds the key with expire at to this set.
     * @param key the key
     * @param expireAt the expireAt
     *
     * @return `true` if a set contained key, `false` otherwise
     */
    fun add(key: K, expireAt: LocalDateTime): Boolean

    /**
     * Setups the expiration after for key to this set.
     * @param key the key
     * @param after the after
     * @param timeUnit the timeUnit
     *
     * @return the old value for this key or null
     */
    fun expire(key: K, after: Long, timeUnit: TimeUnit): Boolean

    /**
     * Setups the expiration at for key to this set.
     * @param key the key
     * @param expireAt the expireAt
     *
     * @return the old value for this key or null
     */
    fun expire(key: K, expireAt: LocalDateTime): Boolean

    /**
     * Checks if a set contains key collection.
     * @param collection the collection
     *
     * @return `true` if a set contains key, `false` otherwise
     */
    fun containsAll(collection: Collection<K>): Boolean

    /**
     * Adds the keys to this set.
     * @param list the list
     *
     * @return `true` if a set contained keys, `false` otherwise
     */
    fun add(list: List<K>): Boolean

    /**
     * Retains the keys to this set.
     * @param collection the collection
     *
     * @return `true` if a set contained keys, `false` otherwise
     */
    fun retainAll(collection: Collection<K>): Boolean

    /**
     * Removes the keys of this set.
     * @param keys the keys
     */
    fun remove(keys: MutableSet<K>)

    /**
     * Removes the keys of this set.
     * @param from the from
     * @param to the to
     */
    fun remove(from: K, to: K)

    /**
     * Returns the size of elements in this set.
     *
     * @return the size of elements in this set
     */
    fun size(): Int

    /**
     * Checks if a set is not empty.
     *
     * @return `true` if a set is not empty, `false` otherwise
     */
    fun nonEmpty(): Boolean

    /**
     * Returns the expiration date for key in this set.
     * @param key the key
     *
     * @return the expiration date for key in this set
     */
    fun expiration(key: K): LocalDateTime?

    /**
     * Returns the time left for key in this set.
     * @param key the key
     *
     * @return the time left for key in this set
     */
    fun timeLeft(key: K): Duration?

    /**
     * Returns the size for segments for this set.
     *
     * @return the size for segments for this set
     */
    fun sizeOfSegments(): Long

    /**
     * Returns the level of meter for zerro level.
     *
     * @return the level of meter for zerro level
     */
    fun level0Meter(): LevelZeroMeter

    /**
     * Returns the level of meter for level.
     * @param levelNumber the level number
     *
     * @return the level of meter for first level
     */
    fun levelMeter(levelNumber: Int): Optional<LevelMeter>

    /**
     * Clears this set.
     */
    fun clear()

    /**
     * Removes the key of this set.
     * @param key the key
     *
     * @return `true` if old key was present, `false` otherwise
     */
    fun remove(key: K): Boolean

    /**
     * Returns the java set of this set.
     *
     * @return the java set of this set
     */
    fun asJava(): MutableSet<K>

    /**
     * Starts the commit function for this set.
     * @param prepares the prepares
     *
     * @return the level zerro for this set
     */
    fun commit(vararg prepares: Prepare<K, scala.runtime.`Nothing$`>): swaydb.data.IO.OK
}
