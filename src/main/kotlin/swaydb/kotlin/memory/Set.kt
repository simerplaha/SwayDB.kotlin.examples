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
package swaydb.java.memory

import scala.Function1
import scala.Option
import scala.collection.JavaConverters
import scala.concurrent.duration.Deadline
import scala.concurrent.duration.FiniteDuration
import swaydb.Prepare
import swaydb.data.IO
import swaydb.data.accelerate.Accelerator
import swaydb.data.accelerate.Level0Meter
import swaydb.data.api.grouping.KeyValueGroupingStrategy
import swaydb.data.compaction.LevelMeter
import swaydb.kotlin.Serializer
import swaydb.memory.`Map$`
import swaydb.memory.`Set$`
import java.io.Closeable
import java.time.Duration
import java.time.LocalDateTime
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.function.Consumer

/**
 * The memory Set of data.
 *
 * @param <K> the type of the key element
 */
class Set<K>(private val database: swaydb.Set<K, IO<*>>) : Closeable {
    /**
     * Checks if a set is empty.
     *
     * @return {@code true} if a set is empty, {@code false} otherwise
     */
    val isEmpty: Boolean
        get() {
            return database.isEmpty().get() as Boolean
        }

    /**
     * Checks if a set contains key.
     * @param key the key
     *
     * @return {@code true} if a set contains key, {@code false} otherwise
     */
    fun contains(key: K): Boolean {
        return database.contains(key).get() as Boolean
    }

    /**
     * Checks if a set might contains key.
     * @param key the key
     *
     * @return {@code true} if a set might contains key, {@code false} otherwise
     */
    fun mightContain(key: K): Boolean {
        return database.mightContain(key).get() as Boolean
    }

    /**
     * Returns the iterator of elements in this set.
     *
     * @return the iterator of elements in this set
     */
    fun iterator(): Iterator<K> {
        val entries = database.asScala().toSeq()
        val result = ArrayList<K>()
        var index = 0
        while (index < entries.size()) {
            result.add(entries.apply(index))
            index += 1
        }
        return result.iterator()
    }

    /**
     * Returns the array of elements in this set.
     *
     * @return the array of elements in this set
     */
    @Suppress("UNCHECKED_CAST")
    fun toArray(): Array<K> {
        val entries = database.asScala().toSeq()
        val result = ArrayList<K>()
        var index = 0
        while (index < entries.size()) {
            result.add(entries.apply(index))
            index += 1
        }
        return result.toArray() as Array<K>
    }

    /**
     * Adds the key to this set.
     * @param key the key
     *
     * @return {@code true} if a set contained key, {@code false} otherwise
     */
    fun add(key: K): Boolean {
        val result = database.add(key).get()
        return result is scala.Some<*>
    }

    /**
     * Adds the key with expire after to this set.
     * @param key the key
     * @param expireAfter the expireAfter
     * @param timeUnit the timeUnit
     *
     * @return {@code true} if a set contained key, {@code false} otherwise
     */
    fun add(key: K, expireAfter: Long, timeUnit: TimeUnit): Boolean {
        val result = contains(key)
        database.add(key, FiniteDuration.create(expireAfter, timeUnit)).get()
        return result
    }

    /**
     * Adds the key with expire at to this set.
     * @param key the key
     * @param expireAt the expireAt
     *
     * @return {@code true} if a set contained key, {@code false} otherwise
     */
    fun add(key: K, expireAt: LocalDateTime): Boolean {
        val result = contains(key)
        val expireAtNano = Duration.between(LocalDateTime.now(), expireAt).nano
        database.add(key, FiniteDuration.create(expireAtNano.toLong(), TimeUnit.NANOSECONDS).fromNow()).get()
        return result
    }

    /**
     * Setups the expiration after for key to this set.
     * @param key the key
     * @param after the after
     * @param timeUnit the timeUnit
     *
     * @return the old value for this key or null
     */
    fun expire(key: K, after: Long, timeUnit: TimeUnit): Boolean {
        val result = contains(key)
        database.expire(key, FiniteDuration.create(after, timeUnit)).get()
        return result
    }

    /**
     * Setups the expiration at for key to this set.
     * @param key the key
     * @param expireAt the expireAt
     *
     * @return the old value for this key or null
     */
    fun expire(key: K, expireAt: LocalDateTime): Boolean {
        val result = contains(key)
        val expireAtNano = Duration.between(LocalDateTime.now(), expireAt).nano
        database.expire(key, FiniteDuration.create(expireAtNano.toLong(), TimeUnit.NANOSECONDS).fromNow()).get()
        return result
    }

    /**
     * Checks if a set contains key collection.
     * @param collection the collection
     *
     * @return {@code true} if a set contains key, {@code false} otherwise
     */
    fun containsAll(collection: Collection<K>): Boolean {
        return collection.stream()
                .allMatch({ elem -> database.contains(elem).get() as Boolean })
    }

    /**
     * Adds the keys to this set.
     * @param list the list
     *
     * @return {@code true} if a set contained keys, {@code false} otherwise
     */
    fun add(list: List<K>): Boolean {
        val entries = scala.collection.JavaConverters.asScalaBufferConverter(list).asScala()
        database.add(entries.toSet()).get()
        return true
    }

    /**
     * Retains the keys to this set.
     * @param collection the collection
     *
     * @return {@code true} if a set contained keys, {@code false} otherwise
     */
    fun retainAll(collection: Collection<K>): Boolean {
        val entries = database.asScala().toSeq()
        val result = ArrayList<K>()
        var index = 0
        while (index < entries.size()) {
            result.add(entries.apply(index))
            index += 1
        }
        result.stream()
                .filter({ elem -> !collection.contains(elem) })
                .forEach(Consumer<K>({ database.remove(it) }))
        return true
    }

    /**
     * Removes the keys of this set.
     * @param keys the keys
     */
    fun remove(keys: MutableSet<K>) {
        database.remove(scala.collection.JavaConverters.asScalaSetConverter(keys).asScala()).get()
    }

    /**
     * Removes the keys of this set.
     * @param from the from
     * @param to the to
     */
    fun remove(from: K, to: K) {
        database.remove(from, to).get()
    }

    /**
     * Returns the size of elements in this set.
     *
     * @return the size of elements in this set
     */
    fun size(): Int {
        return database.asScala().size()
    }

    /**
     * Checks if a set is not empty.
     *
     * @return {@code true} if a set is not empty, {@code false} otherwise
     */
    fun nonEmpty(): Boolean {
        return database.nonEmpty().get() as Boolean
    }

    /**
     * Returns the expiration date for key in this set.
     * @param key the key
     *
     * @return the expiration date for key in this set
     */
    fun expiration(key: K): LocalDateTime? {
        val result = database.expiration(key).get()
        if (result is scala.Some<*>) {
            val expiration = result.get() as Deadline
            return LocalDateTime.now().plusNanos(expiration.timeLeft().toNanos())
        }
        return null
    }

    /**
     * Returns the time left for key in this set.
     * @param key the key
     *
     * @return the time left for key in this set
     */
    fun timeLeft(key: K): Duration? {
        val result = database.timeLeft(key).get()
        if (result is scala.Some<*>) {
            val duration = result.get() as FiniteDuration
            return Duration.ofNanos(duration.toNanos())
        }
        return null
    }

    /**
     * Returns the size for segments for this set.
     *
     * @return the size for segments for this set
     */
    fun sizeOfSegments(): Long {
        return database.sizeOfSegments()
    }

    /**
     * Returns the level of meter for zerro level.
     *
     * @return the level of meter for zerro level
     */
    fun level0Meter(): Level0Meter {
        return database.level0Meter()
    }

    /**
     * Returns the level of meter for first level.
     *
     * @return the level of meter for first level
     */
    fun level1Meter(): Optional<LevelMeter> {
        return levelMeter(1)
    }

    /**
     * Returns the level of meter for level.
     * @param levelNumber the level number
     *
     * @return the level of meter for first level
     */
    fun levelMeter(levelNumber: Int): Optional<LevelMeter> {
        val levelMeter = database.levelMeter(levelNumber)
        return if (levelMeter.isEmpty()) Optional.empty<LevelMeter>() else Optional.ofNullable(levelMeter.get())
    }

    /**
     * Clears this set.
     */
    fun clear() {
        database.asScala().clear()
    }

    /**
     * Removes the key of this set.
     * @param key the key
     *
     * @return {@code true} if old key was present, {@code false} otherwise
     */
    fun remove(key: K): Boolean {
        val result = database.remove(key).get()
        return result is scala.Some<*>
    }

    /**
     * Returns the java set of this set.
     *
     * @return the java set of this set
     */
    fun asJava(): MutableSet<K>? {
        return JavaConverters.setAsJavaSetConverter(database.asScala()).asJava()
    }

    /**
     * Closes the database.
     */
    public override fun close() {
        database.closeDatabase().get()
    }

    /**
     * Starts the commit function for this set.
     * @param prepares the prepares
     *
     * @return the level zerro for this set
     */
    fun commit(vararg prepares: Prepare<K, scala.runtime.`Nothing$`>): Level0Meter {
        val preparesList = Arrays.asList(*prepares)
        val prepareIterator = JavaConverters.iterableAsScalaIterableConverter(preparesList).asScala()
        return database.commit(prepareIterator).get() as Level0Meter
    }

    class Builder<K> {
        private var mapSize = `Map$`.`MODULE$`.`apply$default$1`<K, scala.runtime.`Nothing$`>()
        private var segmentSize = `Map$`.`MODULE$`.`apply$default$2`<K, scala.runtime.`Nothing$`>()
        private var cacheSize = `Map$`.`MODULE$`.`apply$default$3`<K, scala.runtime.`Nothing$`>()
        private var cacheCheckDelay = `Map$`.`MODULE$`.`apply$default$4`<K, scala.runtime.`Nothing$`>()
        private var bloomFilterFalsePositiveRate = `Map$`.`MODULE$`.`apply$default$5`<K, scala.runtime.`Nothing$`>()
        private var compressDuplicateValues = `Map$`.`MODULE$`.`apply$default$6`<K, scala.runtime.`Nothing$`>()
        private var deleteSegmentsEventually = `Map$`.`MODULE$`.`apply$default$7`<K, scala.runtime.`Nothing$`>()
        private var groupingStrategy = `Map$`.`MODULE$`.`apply$default$8`<K, scala.runtime.`Nothing$`>()
        private var acceleration = `Map$`.`MODULE$`.`apply$default$9`<K, scala.runtime.`Nothing$`>()
        private var keySerializer: Any? = null
        fun withMapSize(mapSize: Int): Builder<K> {
            this.mapSize = mapSize
            return this
        }

        fun withSegmentSize(segmentSize: Int): Builder<K> {
            this.segmentSize = segmentSize
            return this
        }

        fun withCacheSize(cacheSize: Int): Builder<K> {
            this.cacheSize = cacheSize
            return this
        }

        fun withCacheCheckDelay(cacheCheckDelay: FiniteDuration): Builder<K> {
            this.cacheCheckDelay = cacheCheckDelay
            return this
        }

        fun withBloomFilterFalsePositiveRate(bloomFilterFalsePositiveRate: Double): Builder<K> {
            this.bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate
            return this
        }

        fun withCompressDuplicateValues(compressDuplicateValues: Boolean): Builder<K> {
            this.compressDuplicateValues = compressDuplicateValues
            return this
        }

        fun withDeleteSegmentsEventually(deleteSegmentsEventually: Boolean): Builder<K> {
            this.deleteSegmentsEventually = deleteSegmentsEventually
            return this
        }

        fun withGroupingStrategy(groupingStrategy: Option<KeyValueGroupingStrategy>): Builder<K> {
            this.groupingStrategy = groupingStrategy
            return this
        }

        fun withAcceleration(acceleration: Function1<Level0Meter, Accelerator>): Builder<K> {
            this.acceleration = acceleration
            return this
        }

        fun withKeySerializer(keySerializer: Any): Builder<K> {
            this.keySerializer = keySerializer
            return this
        }

        @Suppress("UNCHECKED_CAST")
        fun build(): Set<K> {
            val keyOrder = `Map$`.`MODULE$`.`apply$default$12`<K, scala.runtime.`Nothing$`>(mapSize, segmentSize,
                    cacheSize, cacheCheckDelay, bloomFilterFalsePositiveRate, compressDuplicateValues,
                    deleteSegmentsEventually, groupingStrategy, acceleration)
            val ec = `Map$`.`MODULE$`.`apply$default$13`<K, scala.runtime.`Nothing$`>(mapSize, segmentSize, cacheSize,
                    cacheCheckDelay, bloomFilterFalsePositiveRate,
                    compressDuplicateValues, deleteSegmentsEventually, groupingStrategy, acceleration)
            return Set(
                    `Set$`.`MODULE$`.apply(mapSize, segmentSize, cacheSize,
                            cacheCheckDelay, bloomFilterFalsePositiveRate, compressDuplicateValues,
                            deleteSegmentsEventually, groupingStrategy, acceleration, Serializer.classToType(keySerializer),
                            keyOrder, ec).get() as swaydb.Set<K, IO<*>>)
        }
    }

    companion object {
        /**
         * Creates the set.
         * @param <K> the type of the key element
         * @param keySerializer the keySerializer
         *
         * @return the set
         */
        @Suppress("UNCHECKED_CAST")
        fun <K> create(keySerializer: Any): Set<K> {
            val mapSize = `Map$`.`MODULE$`.`apply$default$1`<K, scala.runtime.`Nothing$`>()
            val segmentSize = `Map$`.`MODULE$`.`apply$default$2`<K, scala.runtime.`Nothing$`>()
            val cacheSize = `Map$`.`MODULE$`.`apply$default$3`<K, scala.runtime.`Nothing$`>()
            val cacheCheckDelay = `Map$`.`MODULE$`.`apply$default$4`<K, scala.runtime.`Nothing$`>()
            val bloomFilterFalsePositiveRate = `Map$`.`MODULE$`.`apply$default$5`<K, scala.runtime.`Nothing$`>()
            val compressDuplicateValues = `Map$`.`MODULE$`.`apply$default$6`<K, scala.runtime.`Nothing$`>()
            val deleteSegmentsEventually = `Map$`.`MODULE$`.`apply$default$7`<K, scala.runtime.`Nothing$`>()
            val groupingStrategy = `Map$`.`MODULE$`.`apply$default$8`<K, scala.runtime.`Nothing$`>()
            val acceleration = `Map$`.`MODULE$`.`apply$default$9`<K, scala.runtime.`Nothing$`>()
            val keyOrder = `Map$`.`MODULE$`.`apply$default$12`<K, scala.runtime.`Nothing$`>(mapSize, segmentSize,
                    cacheSize, cacheCheckDelay, bloomFilterFalsePositiveRate, compressDuplicateValues,
                    deleteSegmentsEventually, groupingStrategy, acceleration)
            val ec = `Map$`.`MODULE$`.`apply$default$13`<K, scala.runtime.`Nothing$`>(mapSize, segmentSize, cacheSize,
                    cacheCheckDelay, bloomFilterFalsePositiveRate,
                    compressDuplicateValues, deleteSegmentsEventually, groupingStrategy, acceleration)
            return Set(
                    `Set$`.`MODULE$`.apply(mapSize, segmentSize, cacheSize,
                            cacheCheckDelay, bloomFilterFalsePositiveRate, compressDuplicateValues,
                            deleteSegmentsEventually, groupingStrategy, acceleration, Serializer.classToType(keySerializer),
                            keyOrder, ec).get() as swaydb.Set<K, IO<*>>)
        }

        /**
         * Creates the builder.
         * @param <K> the type of the key element
         *
         * @return the builder
         */
        fun <K> builder(): Builder<K> {
            return Builder()
        }
    }
}
