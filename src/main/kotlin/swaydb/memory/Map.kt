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
package swaydb.memory

import scala.Option
import scala.Some
import scala.Tuple2
import scala.collection.JavaConverters
import scala.concurrent.duration.Deadline
import scala.concurrent.duration.FiniteDuration
import scala.runtime.AbstractFunction1
import swaydb.Apply
import swaydb.data.IO
import swaydb.data.accelerate.Level0Meter
import swaydb.data.compaction.LevelMeter
import swaydb.kotlin.Serializer
import java.io.Closeable
import java.time.Duration
import java.time.LocalDateTime
import java.util.*
import java.util.concurrent.TimeUnit
import swaydb.data.accelerate.Accelerator
import swaydb.data.api.grouping.KeyValueGroupingStrategy
import swaydb.Prepare
import java.util.Arrays

class Map<K, V> private constructor(private val database: swaydb.Map<K, V, IO<*>>) : Closeable {

    fun size(): Int {
        return database.asScala().size()
    }

    fun isEmpty(): Boolean {
        return database.isEmpty.get() as Boolean
    }

    fun nonEmpty(): Boolean {
        return database.nonEmpty().get() as Boolean
    }

    fun expiration(key: K): LocalDateTime? {
        val result = database.expiration(key).get()
        if (result is Some<*>) {
            val expiration = result.get() as Deadline
            return LocalDateTime.now().plusNanos(expiration.timeLeft().toNanos())
        }
        return null
    }

    fun timeLeft(key: K): Duration? {
        val result = database.timeLeft(key).get()
        if (result is Some<*>) {
            val duration = result.get() as FiniteDuration
            return Duration.ofNanos(duration.toNanos())
        }
        return null
    }

    fun keySize(key: K): Int {
        return database.keySize(key)
    }

    fun valueSize(value: V): Int {
        return database.valueSize(value)
    }

    fun sizeOfSegments(): Long {
        return database.sizeOfSegments()
    }

    fun level0Meter(): Level0Meter {
        return database.level0Meter()
    }

    fun level1Meter(): Optional<LevelMeter> {
        return levelMeter(1)
    }

    fun levelMeter(levelNumber: Int): Optional<LevelMeter> {
        val levelMeter = database.levelMeter(levelNumber)
        return if (levelMeter.isEmpty) Optional.empty() else Optional.ofNullable(levelMeter.get())
    }

    fun containsKey(key: K): Boolean {
        return database.contains(key).get() as Boolean
    }

    fun mightContain(key: K): Boolean {
        return database.mightContain(key).get() as Boolean
    }

    @Suppress("UNCHECKED_CAST")
    fun head(): MutableMap.MutableEntry<K, V>? {
        val result = database.headOption().get()
        if (result is Some<*>) {
            val tuple2 = result.get() as Tuple2<K, V>
            return AbstractMap.SimpleEntry<K, V>(tuple2._1(), tuple2._2())
        }
        return null
    }

    fun headOption(): Optional<MutableMap.MutableEntry<K, V>> {
        return Optional.ofNullable(head())
    }

    @Suppress("UNCHECKED_CAST")
    fun last(): MutableMap.MutableEntry<K, V>? {
        val result = database.lastOption().get()
        if (result is Some<*>) {
            val tuple2 = result.get() as Tuple2<K, V>
            return AbstractMap.SimpleEntry<K, V>(tuple2._1(), tuple2._2())
        }
        return null
    }

    fun lastOption(): Optional<MutableMap.MutableEntry<K, V>> {
        return Optional.ofNullable(last())
    }

    fun containsValue(value: V): Boolean {
        return values().contains(value)
    }

    fun put(map: kotlin.collections.Map<K, V>) {
        val entries = JavaConverters.mapAsScalaMapConverter<K, V>(map).asScala()
        database.put(entries.toSet()).get()
    }

    fun put(seq: scala.collection.Seq<Tuple2<K, V>>) {
        database.put(seq)
    }

    fun update(map: kotlin.collections.Map<K, V>) {
        val entries = JavaConverters.mapAsScalaMapConverter<K, V>(map).asScala()
        database.update(entries.toSet()).get()
    }

    fun clear() {
        database.asScala().clear()
    }

    fun keySet(): MutableSet<K> {
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

    @Suppress("UNCHECKED_CAST")
    fun keysHead(): K? {
        val result = database.keys().headOption().get()
        if (result is Some<*>) {
            return result.get() as K
        }
        return null
    }

    fun keysHeadOption(): Optional<K> {
        return Optional.ofNullable(keysHead())
    }

    @Suppress("UNCHECKED_CAST")
    fun keysLast(): K? {
        val result = database.keys().lastOption().get()
        if (result is Some<*>) {
            return result.get() as K
        }
        return null
    }

    fun keysLastOption(): Optional<K> {
        return Optional.ofNullable(keysLast())
    }

    fun values(): Collection<V> {
        val entries = database.asScala().toSeq()
        val result = LinkedHashSet<V>()
        var index = 0
        while (index < entries.size()) {
            val tuple2 = entries.apply(index)
            result.add(tuple2._2())
            index += 1
        }
        return result
    }

    fun entrySet(): MutableSet<MutableMap.MutableEntry<K, V>> {
        val entries = database.asScala().toSeq()
        val result = LinkedHashSet<MutableMap.MutableEntry<K, V>>()
        var index = 0
        while (index < entries.size()) {
            val tuple2 = entries.apply(index)
            result.add(AbstractMap.SimpleEntry<K, V>(tuple2._1(), tuple2._2()))
            index += 1
        }
        return result
    }

    fun put(key: K, value: V): V? {
        val oldValue = get(key)
        database.put(key, value).get()
        return oldValue
    }

    fun put(key: K, value: V, expireAfter: Long, timeUnit: TimeUnit): V? {
        val oldValue = get(key)
        database.put(key, value, FiniteDuration.create(expireAfter, timeUnit)).get()
        return oldValue
    }

    fun put(key: K, value: V, expireAt: LocalDateTime): V? {
        val oldValue = get(key)
        val expireAtNano = Duration.between(LocalDateTime.now(), expireAt).nano
        database.put(key, value, FiniteDuration.create(expireAtNano.toLong(), TimeUnit.NANOSECONDS).fromNow()).get()
        return oldValue
    }

    fun expire(key: K, after: Long, timeUnit: TimeUnit): V? {
        val oldValue = get(key)
        database.expire(key, FiniteDuration.create(after, timeUnit)).get()
        return oldValue
    }

    fun expire(key: K, expireAt: LocalDateTime): V? {
        val oldValue = get(key)
        val expireAtNano = Duration.between(LocalDateTime.now(), expireAt).nano
        database.expire(key, FiniteDuration.create(expireAtNano.toLong(), TimeUnit.NANOSECONDS).fromNow()).get()
        return oldValue
    }

    fun update(key: K, value: V): V? {
        val oldValue = get(key)
        database.update(key, value).get()
        return oldValue
    }

    @Suppress("UNCHECKED_CAST")
    fun get(key: K): V? {
        val result = database.get(key).get()
        if (result is Some<*>) {
            return result.get() as V
        }
        return null
    }

    fun remove(key: K): V? {
        val oldValue = get(key)
        database.remove(key).get()
        return oldValue
    }

    fun remove(keys: MutableSet<K>) {
        database.remove(JavaConverters.asScalaSetConverter<K>(keys).asScala()).get()
    }

    fun remove(from: K, to: K) {
        database.remove(from, to).get()
    }

    fun asJava(): kotlin.collections.Map<K, V>? {
        return JavaConverters.mapAsJavaMapConverter(database.asScala()).asJava()
    }

    fun registerFunction(functionId: K, function: (V) -> Apply.Map<V>): K {
        return database.registerFunction(functionId, object : AbstractFunction1<V, Apply.Map<V>>() {
            override fun apply(value: V): Apply.Map<V> {
                return function(value)
            }
        }) as K
    }

    fun applyFunction(key: K, functionId: K) {
        database.applyFunction(key, functionId)
    }

    fun from(key: K): swaydb.Map<K, V, IO<*>> {
        return database.from(key)
    }

    fun fromOrAfter(key: K): swaydb.Map<K, V, IO<*>> {
        return database.fromOrAfter(key)
    }

    fun fromOrBefore(key: K): swaydb.Map<K, V, IO<*>> {
        return database.fromOrBefore(key)
    }

    fun keys(): swaydb.Set<K, IO<*>> {
        return database.keys()
    }

    override fun close() {
        database.closeDatabase().get()
    }

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
        private var acceleration = `Map$`.`MODULE$`.`apply$default$9`<K, V>() as scala.Function1<Level0Meter, Accelerator>
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

        fun withAcceleration(acceleration: scala.Function1<Level0Meter, Accelerator>): Builder<K, V> {
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
        fun build(): swaydb.memory.Map<K, V> {
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

        fun <K, V> builder(): Builder<K, V> {
            return Builder()
        }
    }
}
