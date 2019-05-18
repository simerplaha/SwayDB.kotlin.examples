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
package swaydb.kotlin.memory

import scala.Function1
import scala.Option
import scala.Tuple2
import scala.collection.JavaConverters
import scala.concurrent.duration.Deadline
import scala.concurrent.duration.FiniteDuration
import scala.runtime.AbstractFunction1
import scala.runtime.BoxedUnit
import swaydb.Apply
import swaydb.Prepare
import swaydb.Stream
import swaydb.data.IO
import swaydb.data.accelerate.Accelerator
import swaydb.data.accelerate.Level0Meter
import swaydb.data.api.grouping.KeyValueGroupingStrategy
import swaydb.data.compaction.LevelMeter
import swaydb.kotlin.Serializer
import swaydb.memory.`Map$`
import java.io.Closeable
import java.time.Duration
import java.time.LocalDateTime
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.function.Consumer
import java.util.function.Function
import java.util.function.Predicate
import java.util.function.UnaryOperator

/**
 * The memory Map of data.
 *
 * @param <K> the type of the key element
 * @param <V> the type of the value element
 */
class Map<K, V> (private val database: swaydb.Map<K, V, IO<*>>) : Closeable {

    /**
     * Checks the map is empty.
     *
     * @return `true` if a map is empty, `false` otherwise
     */
    fun isEmpty(): Boolean {
        return database.isEmpty().get() as Boolean
    }

    /**
     * Returns the size of elements in this map.
     *
     * @return the size of elements in this map
     */
    fun size(): Int {
        return database.asScala().size()
    }

    /**
     * Checks the map is not empty.
     *
     * @return `true` if a map is not empty, `false` otherwise
     */
    fun nonEmpty(): Boolean {
        return database.nonEmpty().get() as Boolean
    }

    /**
     * Returns the expiration date for key in this map.
     * @param key the key
     *
     * @return the expiration date for key in this map
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
     * Returns the time left for key in this map.
     * @param key the key
     *
     * @return the time left for key in this map
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
     * Returns the key size in bytes for this map.
     * @param key the key
     *
     * @return the key size in bytes for this map
     */
    fun keySize(key: K): Int {
        return database.keySize(key)
    }

    /**
     * Returns the value size in bytes for this map.
     * @param value the value
     *
     * @return the value size in bytes for this map
     */
    fun valueSize(value: V): Int {
        return database.valueSize(value)
    }

    /**
     * Returns the size for segments for this map.
     *
     * @return the size for segments for this map
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
        return if (levelMeter.isEmpty) Optional.empty() else Optional.ofNullable(levelMeter.get())
    }

    /**
     * Checks if a map contains key.
     * @param key the key
     *
     * @return `true` if a map contains key, `false` otherwise
     */
    fun containsKey(key: K): Boolean {
        return database.contains(key).get() as Boolean
    }

    /**
     * Checks if a map might contain key.
     * @param key the key
     *
     * @return `true` if a map might contains key, `false` otherwise
     */
    fun mightContain(key: K): Boolean {
        return database.mightContain(key).get() as Boolean
    }

    /**
     * Returns the head key for this map.
     *
     * @return the head key for this map
     */
    @Suppress("UNCHECKED_CAST")
    fun head(): MutableMap.MutableEntry<K, V>? {
        val result = database.headOption().get()
        if (result is scala.Some<*>) {
            val tuple2 = result.get() as Tuple2<K, V>
            return AbstractMap.SimpleEntry(tuple2._1(), tuple2._2())
        }
        return null
    }

    /**
     * Returns the optional head key for this map.
     *
     * @return the optional head key for this map
     */
    fun headOption(): Optional<MutableMap.MutableEntry<K, V>> {
        return Optional.ofNullable(head())
    }

    /**
     * Returns the last key for this map.
     *
     * @return the last key for this map
     */
    @Suppress("UNCHECKED_CAST")
    fun last(): MutableMap.MutableEntry<K, V>? {
        val result = database.lastOption().get()
        if (result is scala.Some<*>) {
            val tuple2 = result.get() as Tuple2<K, V>
            return AbstractMap.SimpleEntry(tuple2._1(), tuple2._2())
        }
        return null
    }

    /**
     * Returns the optional last key for this map.
     *
     * @return the optional last key for this map
     */
    fun lastOption(): Optional<MutableMap.MutableEntry<K, V>> {
        return Optional.ofNullable(last())
    }

    /**
     * Checks if a map contains value.
     * @param value the value
     *
     * @return `true` if a map contains value, `false` otherwise
     */
    fun containsValue(value: V): Boolean {
        return values().contains(value)
    }

    /**
     * Puts a map object to this map.
     * @param map the map
     */
    fun put(map: MutableMap<K, V>) {
        val entries = scala.collection.JavaConverters.mapAsScalaMapConverter<K, V>(map).asScala()
        database.put(entries.toSet()).get()
    }

    /**
     * Puts a seq object to this map.
     * @param seq the seq
     */
    @Suppress("UNCHECKED_CAST")
    fun put(seq: scala.collection.Seq<*>) {
        database.put(seq as scala.collection.Seq<Tuple2<K, V>>)
    }

    /**
     * Updates map entries for this map.
     * @param map the map
     */
    fun update(map: MutableMap<K, V>) {
        val entries = scala.collection.JavaConverters.mapAsScalaMapConverter<K, V>(map).asScala()
        database.update(entries.toSet()).get()
    }

    /**
     * Clears this map.
     */
    fun clear() {
        database.asScala().clear()
    }

    /**
     * Returns the key set for this map.
     *
     * @return the key set for this map
     */
    fun keySet(): Set<K> {
        val entries = database.asScala().toSeq()
        val result = LinkedHashSet<K>()
        var index = 0
        while (index < entries.size()) {
            val tuple2 = entries.apply(index)
            result.add(tuple2._1())
            index += 1
        }
        return result
    }

    /**
     * Returns the head key for this map.
     *
     * @return the head key for this map
     */
    @Suppress("UNCHECKED_CAST")
    fun keysHead(): K? {
        val result = database.keys().headOption().get()
        return if (result is scala.Some<*>) {
            result.get() as K
        } else null
    }

    /**
     * Returns the optional head key for this map.
     *
     * @return the optional head key for this map
     */
    fun keysHeadOption(): Optional<K> {
        return Optional.ofNullable(keysHead())
    }

    /**
     * Returns the last key for this map.
     *
     * @return the last key for this map
     */
    @Suppress("UNCHECKED_CAST")
    fun keysLast(): K? {
        val result = database.keys().lastOption().get()
        return if (result is scala.Some<*>) {
            result.get() as K
        } else null
    }

    /**
     * Returns the optional last key for this map.
     *
     * @return the optional last key for this map
     */
    fun keysLastOption(): Optional<K> {
        return Optional.ofNullable(keysLast())
    }

    /**
     * Returns the values for this map.
     *
     * @return the values last key for this map
     */
    fun values(): List<V> {
        val entries = database.asScala().toSeq()
        val result = ArrayList<V>()
        var index = 0
        while (index < entries.size()) {
            val tuple2 = entries.apply(index)
            result.add(tuple2._2())
            index += 1
        }
        return result
    }

    /**
     * Returns the entrues for this map.
     *
     * @return the entrues last key for this map
     */
    fun entrySet(): Set<MutableMap.MutableEntry<K, V>> {
        val entries = database.asScala().toSeq()
        val result = LinkedHashSet<MutableMap.MutableEntry<K, V>>()
        var index = 0
        while (index < entries.size()) {
            val tuple2 = entries.apply(index)
            result.add(AbstractMap.SimpleEntry(tuple2._1(), tuple2._2()))
            index += 1
        }
        return result
    }

    /**
     * Puts the key/value pair for this map.
     * @param key the key
     * @param value the value
     *
     * @return the old value for this key or null
     */
    fun put(key: K, value: V): V? {
        val oldValue = get(key)
        database.put(key, value).get()
        return oldValue
    }

    /**
     * Puts the key/value pair for this map with expiration after data.
     * @param key the key
     * @param value the value
     * @param expireAfter the expireAfter
     * @param timeUnit the timeUnit
     *
     * @return the old value for this key or null
     */
    fun put(key: K, value: V, expireAfter: Long, timeUnit: TimeUnit): V? {
        val oldValue = get(key)
        database.put(key, value, FiniteDuration.create(expireAfter, timeUnit)).get()
        return oldValue
    }

    /**
     * Puts the key/value pair for this map with expiration at data.
     * @param key the key
     * @param value the value
     * @param expireAt the expireAt
     *
     * @return the old value for this key or null
     */
    fun put(key: K, value: V, expireAt: LocalDateTime): V? {
        val oldValue = get(key)
        val expireAtNano = Duration.between(LocalDateTime.now(), expireAt).nano
        database.put(key, value, FiniteDuration.create(expireAtNano.toLong(), TimeUnit.NANOSECONDS).fromNow()).get()
        return oldValue
    }

    /**
     * Setups the expiration after for key to this map.
     * @param key the key
     * @param after the after
     * @param timeUnit the timeUnit
     *
     * @return the old value for this key or null
     */
    fun expire(key: K, after: Long, timeUnit: TimeUnit): V? {
        val oldValue = get(key)
        database.expire(key, FiniteDuration.create(after, timeUnit)).get()
        return oldValue
    }

    /**
     * Setups the expiration at for key to this map.
     * @param key the key
     * @param expireAt the expireAt
     *
     * @return the old value for this key or null
     */
    fun expire(key: K, expireAt: LocalDateTime): V? {
        val oldValue = get(key)
        val expireAtNano = Duration.between(LocalDateTime.now(), expireAt).nano
        database.expire(key, FiniteDuration.create(expireAtNano.toLong(), TimeUnit.NANOSECONDS).fromNow()).get()
        return oldValue
    }

    /**
     * Updates the key/value for this map.
     * @param key the key
     * @param value the value
     *
     * @return the old value for this key or null
     */
    fun update(key: K, value: V): V? {
        val oldValue = get(key)
        database.update(key, value).get()
        return oldValue
    }

    /**
     * Returns the value or null for key of this map.
     * @param key the key
     *
     * @return the value or null for key of this map
     */
    @Suppress("UNCHECKED_CAST")
    operator fun get(key: K): V? {
        val result = database.get(key).get()
        return if (result is scala.Some<*>) {
            result.get() as V
        } else null
    }

    /**
     * Removes the value for key of this map.
     * @param key the key
     *
     * @return the old value or null for key of this map
     */
    fun remove(key: K): V? {
        val oldValue = get(key)
        database.remove(key).get()
        return oldValue
    }

    /**
     * Removes the values for key set of this map.
     * @param keys the keys
     */
    fun remove(keys: Set<K>) {
        database.remove(scala.collection.JavaConverters.asScalaSetConverter(keys).asScala()).get()
    }

    /**
     * Removes the values for keys of this map.
     * @param from the from
     * @param to the to
     */
    fun remove(from: K, to: K) {
        database.remove(from, to).get()
    }

    /**
     * Returns the java map of this map.
     *
     * @return the java map of this map
     */
    fun asJava(): MutableMap<K, V> {
        return JavaConverters.mapAsJavaMapConverter(database.asScala()).asJava()
    }

    /**
     * Registers the function for this map.
     * @param functionId the functionId
     * @param function the function
     *
     * @return the function id
     */
    fun registerFunction(functionId: K, function: (V) -> Apply.Map<V>): K {
        return database.registerFunction(functionId, object : AbstractFunction1<V, Apply.Map<V>>() {
            override fun apply(value: V): Apply.Map<V> {
                return function(value)
            }
        })
    }

    /**
     * Executes the registered function for this map.
     * @param key the key
     * @param functionId the functionId
     */
    fun applyFunction(key: K, functionId: K) {
        database.applyFunction(key, functionId)
    }

    /**
     * Returns the map object which starts from key for this map.
     * @param key the key
     *
     * @return the map object
     */
    fun from(key: K): Map<K, V> {
        return Map(database.from(key))
    }

    /**
     * Returns the map object which starts or after key for this map.
     * @param key the key
     *
     * @return the map object
     */
    fun fromOrAfter(key: K): Map<K, V> {
        return Map(database.fromOrAfter(key))
    }

    /**
     * Returns the map object which starts or before key for this map.
     * @param key the key
     *
     * @return the map object
     */
    fun fromOrBefore(key: K): Map<K, V> {
        return Map(database.fromOrBefore(key))
    }

    /**
     * Returns the key objects for this map.
     *
     * @return the key objects for this map
     */
    fun keys(): swaydb.Set<K, IO<*>> {
        return database.keys()
    }

    /**
     * Returns the reversed map object for this map.
     *
     * @return the reversed map object for this map
     */
    fun reverse(): Map<K, V> {
        return Map(database.reverse())
    }

    /**
     * Starts the map function for this map.
     * @param function the function
     *
     * @return the stream object for this map
     */
    fun map(function: UnaryOperator<MutableMap.MutableEntry<K, V>>): Stream<Any, IO<*>>? {
        return database.map(object : AbstractFunction1<Tuple2<K, V>, Any>() {
            override fun apply(tuple2: Tuple2<K, V>): Any {
                val result = function.apply(
                        AbstractMap.SimpleEntry(tuple2._1(), tuple2._2()))
                return Tuple2.apply<K, V>(result.key, result.value)
            }
        })
    }

    /**
     * Starts the drop function for this map.
     * @param count the count
     *
     * @return the stream object for this map
     */
    fun drop(count: Int): Stream<Tuple2<K, V>, IO<*>>? {
        return database.drop(count)
    }

    /**
     * Starts the dropWhile function for this map.
     * @param predicate the function
     *
     * @return the stream object for this map
     */
    fun dropWhile(predicate: Predicate<MutableMap.MutableEntry<K, V>>): Stream<Tuple2<K, V>, IO<*>>? {
        return database.dropWhile(object : AbstractFunction1<Tuple2<K, V>, Any>() {
            override fun apply(tuple2: Tuple2<K, V>): Any {
                return predicate.test(AbstractMap.SimpleEntry(tuple2._1(), tuple2._2()))
            }
        })
    }

    /**
     * Starts the take function for this map.
     * @param count the count
     *
     * @return the stream object for this map
     */
    fun take(count: Int): Stream<Tuple2<K, V>, IO<*>>? {
        return database.take(count)
    }

    /**
     * Starts the takeWhile function for this map.
     * @param predicate the function
     *
     * @return the stream object for this map
     */
    fun takeWhile(predicate: (MutableMap.MutableEntry<K, V>) -> Boolean): Stream<Tuple2<K, V>, IO<*>>? {
        return database.takeWhile(object : AbstractFunction1<Tuple2<K, V>, Any>() {
            override fun apply(tuple2: Tuple2<K, V>): Any {
                return predicate(AbstractMap.SimpleEntry(tuple2._1(), tuple2._2()))
            }
        })
    }

    /**
     * Starts the foreach function for this map.
     * @param consumer the consumer
     *
     * @return the stream object for this map
     */
    fun foreach(consumer: Consumer<MutableMap.MutableEntry<K, V>>): Stream<BoxedUnit, IO<*>>? {
        return database.foreach(object : AbstractFunction1<Tuple2<K, V>, Any>() {
            override fun apply(tuple2: Tuple2<K, V>): Any? {
                consumer.accept(AbstractMap.SimpleEntry(tuple2._1(), tuple2._2()))
                return null
            }
        })
    }

    /**
     * Starts the filter function for this map.
     * @param predicate the function
     *
     * @return the stream object for this map
     */
    fun filter(predicate: Predicate<MutableMap.MutableEntry<K, V>>): Stream<Tuple2<K, V>, IO<*>>? {
        return database.filter(object : AbstractFunction1<Tuple2<K, V>, Any>() {
            override fun apply(tuple2: Tuple2<K, V>): Any {
                return predicate.test(AbstractMap.SimpleEntry(tuple2._1(), tuple2._2()))
            }
        })
    }

    /**
     * Closes the database.
     */
    override fun close() {
        database.closeDatabase().get()
    }

    /**
     * Starts the commit function for this map.
     * @param prepares the prepares
     *
     * @return the level zerro for this map
     */
    fun commit(vararg prepares: Prepare<K, V>): Level0Meter {
        val preparesList = Arrays.asList(*prepares)
        val prepareIterator = JavaConverters.iterableAsScalaIterableConverter(preparesList).asScala()
        return database.commit(prepareIterator).get() as Level0Meter
    }

    class Builder<K, V> {

        private var mapSize = `Map$`.`MODULE$`.`apply$default$1`<K, V>()
        private var segmentSize = `Map$`.`MODULE$`.`apply$default$2`<K, V>()
        private var cacheSize = `Map$`.`MODULE$`.`apply$default$3`<K, V>()
        private var cacheCheckDelay = `Map$`.`MODULE$`.`apply$default$4`<K, V>()
        private var bloomFilterFalsePositiveRate = `Map$`.`MODULE$`.`apply$default$5`<K, V>()
        private var compressDuplicateValues = `Map$`.`MODULE$`.`apply$default$6`<K, V>()
        private var deleteSegmentsEventually = `Map$`.`MODULE$`.`apply$default$7`<K, V>()
        private var groupingStrategy = `Map$`.`MODULE$`.`apply$default$8`<K, V>()
        private var acceleration = `Map$`.`MODULE$`.`apply$default$9`<K, V>()
        private var keySerializer: Any? = null
        private var valueSerializer: Any? = null

        fun withMapSize(mapSize: Int): Builder<K, V> {
            this.mapSize = mapSize
            return this
        }

        fun withSegmentSize(segmentSize: Int): Builder<K, V> {
            this.segmentSize = segmentSize
            return this
        }

        fun withCacheSize(cacheSize: Int): Builder<K, V> {
            this.cacheSize = cacheSize
            return this
        }

        fun withCacheCheckDelay(cacheCheckDelay: FiniteDuration): Builder<K, V> {
            this.cacheCheckDelay = cacheCheckDelay
            return this
        }

        fun withBloomFilterFalsePositiveRate(bloomFilterFalsePositiveRate: Double): Builder<K, V> {
            this.bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate
            return this
        }

        fun withCompressDuplicateValues(compressDuplicateValues: Boolean): Builder<K, V> {
            this.compressDuplicateValues = compressDuplicateValues
            return this
        }

        fun withDeleteSegmentsEventually(deleteSegmentsEventually: Boolean): Builder<K, V> {
            this.deleteSegmentsEventually = deleteSegmentsEventually
            return this
        }

        fun withGroupingStrategy(groupingStrategy: Option<KeyValueGroupingStrategy>): Builder<K, V> {
            this.groupingStrategy = groupingStrategy
            return this
        }

        fun withAcceleration(acceleration: Function1<Level0Meter, Accelerator>): Builder<K, V> {
            this.acceleration = acceleration
            return this
        }

        fun withKeySerializer(keySerializer: Any): Builder<K, V> {
            this.keySerializer = keySerializer
            return this
        }

        fun withValueSerializer(valueSerializer: Any): Builder<K, V> {
            this.valueSerializer = valueSerializer
            return this
        }

        @Suppress("UNCHECKED_CAST")
        fun build(): Map<K, V> {
            val keyOrder = `Map$`.`MODULE$`.`apply$default$12`<K, V>(mapSize, segmentSize,
                    cacheSize, cacheCheckDelay, bloomFilterFalsePositiveRate, compressDuplicateValues,
                    deleteSegmentsEventually, groupingStrategy, acceleration)
            val ec = `Map$`.`MODULE$`.`apply$default$13`<K, V>(mapSize, segmentSize, cacheSize,
                    cacheCheckDelay, bloomFilterFalsePositiveRate,
                    compressDuplicateValues, deleteSegmentsEventually, groupingStrategy, acceleration)
            return Map(
                    `Map$`.`MODULE$`.apply(mapSize, segmentSize, cacheSize,
                            cacheCheckDelay, bloomFilterFalsePositiveRate, compressDuplicateValues,
                            deleteSegmentsEventually, groupingStrategy, acceleration,
                            Serializer.classToType(keySerializer), Serializer.classToType(valueSerializer),
                            keyOrder, ec).get() as swaydb.Map<K, V, IO<*>>)
        }
    }

    companion object {

        /**
         * Creates the map.
         * @param <K> the type of the key element
         * @param <V> the type of the value element
         * @param keySerializer the keySerializer
         * @param valueSerializer the valueSerializer
         *
         * @return the map
         */
        @Suppress("UNCHECKED_CAST")
        fun <K, V> create(keySerializer: Any, valueSerializer: Any): Map<K, V> {
            val mapSize = `Map$`.`MODULE$`.`apply$default$1`<K, V>()
            val segmentSize = `Map$`.`MODULE$`.`apply$default$2`<K, V>()
            val cacheSize = `Map$`.`MODULE$`.`apply$default$3`<K, V>()
            val cacheCheckDelay = `Map$`.`MODULE$`.`apply$default$4`<K, V>()
            val bloomFilterFalsePositiveRate = `Map$`.`MODULE$`.`apply$default$5`<K, V>()
            val compressDuplicateValues = `Map$`.`MODULE$`.`apply$default$6`<K, V>()
            val deleteSegmentsEventually = `Map$`.`MODULE$`.`apply$default$7`<K, V>()
            val groupingStrategy = `Map$`.`MODULE$`.`apply$default$8`<K, V>()
            val acceleration = `Map$`.`MODULE$`.`apply$default$9`<K, V>()

            val keyOrder = `Map$`.`MODULE$`.`apply$default$12`<K, V>(mapSize, segmentSize,
                    cacheSize, cacheCheckDelay, bloomFilterFalsePositiveRate, compressDuplicateValues,
                    deleteSegmentsEventually, groupingStrategy, acceleration)
            val ec = `Map$`.`MODULE$`.`apply$default$13`<K, V>(mapSize, segmentSize, cacheSize,
                    cacheCheckDelay, bloomFilterFalsePositiveRate,
                    compressDuplicateValues, deleteSegmentsEventually, groupingStrategy, acceleration)

            return Map(
                    `Map$`.`MODULE$`.apply(mapSize, segmentSize, cacheSize,
                            cacheCheckDelay, bloomFilterFalsePositiveRate, compressDuplicateValues,
                            deleteSegmentsEventually, groupingStrategy, acceleration, Serializer.classToType(keySerializer),
                            Serializer.classToType(valueSerializer), keyOrder, ec).get() as swaydb.Map<K, V, IO<*>>)
        }

        /**
         * Creates the builder.
         * @param <K> the type of the key element
         * @param <V> the type of the value element
         *
         * @return the builder
         */
        fun <K, V> builder(): Builder<K, V> {
            return Builder()
        }
    }
}
