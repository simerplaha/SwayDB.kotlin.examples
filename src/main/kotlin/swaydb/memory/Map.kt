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
package swaydb.memory

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

class Map<K, V> private constructor(database: swaydb.Map<K, V, IO<*>>) : Closeable {
    private val database: swaydb.Map<K, V, IO<*>>

    init {
        this.database = database
    }

    fun size(): Int {
        return database.asScala().size()
    }

    fun iEmpty(): Boolean {
        return database.isEmpty().get() as Boolean
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
        return if (levelMeter.isEmpty()) Optional.empty<LevelMeter>() else Optional.ofNullable(levelMeter.get())
    }

    fun containsKey(key: K): Boolean {
        return database.contains(key).get() as Boolean
    }

    fun mightContain(key: K): Boolean {
        return database.mightContain(key).get() as Boolean
    }

    fun head(): MutableMap.MutableEntry<K, V>? {
        val result = database.headOption().get()
        if (result is Some<*>) {
            val tuple2 = result.get() as Tuple2<K, V>
            return AbstractMap.SimpleEntry<K, V>(tuple2._1(), tuple2._2())
        }
        return null
    }

    fun headOption(): Optional<MutableMap.MutableEntry<K, V>> {
        return Optional.ofNullable<MutableMap.MutableEntry<K, V>>(head())
    }

    fun last(): MutableMap.MutableEntry<K, V>? {
        val result = database.lastOption().get()
        if (result is Some<*>) {
            val tuple2 = result.get() as Tuple2<K, V>
            return AbstractMap.SimpleEntry<K, V>(tuple2._1(), tuple2._2())
        }
        return null
    }

    fun lastOption(): Optional<MutableMap.MutableEntry<K, V>> {
        return Optional.ofNullable<MutableMap.MutableEntry<K, V>>(last())
    }

    fun containsValue(value: V): Boolean {
        return values().contains(value)
    }

    fun put(map: MutableMap<K, V>) {
        val entries = JavaConverters.mapAsScalaMapConverter<K, V>(map).asScala()
        database.put(entries.toSet()).get()
    }

    fun put(seq: scala.collection.Seq<Tuple2<K, V>>) {
        database.put(seq)
    }

    fun update(map: MutableMap<K, V>) {
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

    fun keysHead(): K? {
        val result = database.keys().headOption().get()
        if (result is Some<*>) {
            return result.get() as K
        }
        return null
    }

    fun keysHeadOption(): Optional<K> {
        return Optional.ofNullable<K>(keysHead())
    }

    fun keysLast(): K? {
        val result = database.keys().lastOption().get()
        if (result is Some<*>) {
            return result.get() as K
        }
        return null
    }

    fun keysLastOption(): Optional<K> {
        return Optional.ofNullable<K>(keysLast())
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
        val expireAtNano = Duration.between(LocalDateTime.now(), expireAt).getNano()
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
        val expireAtNano = Duration.between(LocalDateTime.now(), expireAt).getNano()
        database.expire(key, FiniteDuration.create(expireAtNano.toLong(), TimeUnit.NANOSECONDS).fromNow()).get()
        return oldValue
    }

    fun update(key: K, value: V): V? {
        val oldValue = get(key)
        database.update(key, value).get()
        return oldValue
    }

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

    fun asJava(): MutableMap<K, V>? {
        return JavaConverters.mapAsJavaMapConverter(database.asScala()).asJava()
    }

    fun registerFunction(functionId: K, function: java.util.function.Function<V, Apply.Map<V>>): K {
        return database.registerFunction(functionId, object : AbstractFunction1<V, Apply.Map<V>>() {
            override fun apply(value: V): Apply.Map<V> {
                return function.apply(value)
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

    companion object {
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
            return Map<K, V>(
                    `Map$`.`MODULE$`.apply(mapSize, segmentSize, cacheSize,
                            cacheCheckDelay, bloomFilterFalsePositiveRate, compressDuplicateValues,
                            deleteSegmentsEventually, groupingStrategy, acceleration, Serializer.classToType(keySerializer),
                            Serializer.classToType(valueSerializer), keyOrder, ec).get() as swaydb.Map<K, V, IO<*>>)
        }
    }
}
