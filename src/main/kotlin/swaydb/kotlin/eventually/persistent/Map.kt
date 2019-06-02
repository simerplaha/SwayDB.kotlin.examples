package swaydb.kotlin.eventually.persistent

import scala.Function1
import scala.Option
import scala.Tuple2
import scala.collection.JavaConverters
import scala.collection.Seq
import scala.concurrent.duration.Deadline
import scala.concurrent.duration.FiniteDuration
import scala.runtime.AbstractFunction1
import swaydb.Apply
import swaydb.Prepare
import swaydb.data.IO
import swaydb.data.accelerate.Accelerator
import swaydb.data.accelerate.Level0Meter
import swaydb.data.api.grouping.KeyValueGroupingStrategy
import swaydb.data.compaction.LevelMeter
import swaydb.data.config.Dir
import swaydb.kotlin.Serializer
import java.io.Closeable
import java.nio.file.Path
import java.time.Duration
import java.time.LocalDateTime
import java.util.*
import java.util.concurrent.TimeUnit

/**
 * The persistent Map of data.
 *
 * @param <K> the type of the key element
 * @param <V> the type of the value element
 */
class Map<K, V>(private val database: swaydb.Map<K, V, IO<*>>) : swaydb.kotlin.Map<K, V>, Closeable {

    /**
     * Checks the map is empty.
     *
     * @return `true` if a map is empty, `false` otherwise
     */
    override fun isEmpty(): Boolean {
        return database.isEmpty.get() as Boolean
    }

    /**
     * Returns the size of elements in this map.
     *
     * @return the size of elements in this map
     */
    override fun size(): Int {
        return database.asScala().size()
    }

    /**
     * Checks the map is not empty.
     *
     * @return `true` if a map is not empty, `false` otherwise
     */
    override fun nonEmpty(): Boolean {
        return database.nonEmpty().get() as Boolean
    }

    /**
     * Returns the expiration date for key in this map.
     * @param key the key
     *
     * @return the expiration date for key in this map
     */
    override fun expiration(key: K): LocalDateTime? {
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
    override fun timeLeft(key: K): Duration? {
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
    override fun keySize(key: K): Int {
        return database.keySize(key)
    }

    /**
     * Returns the value size in bytes for this map.
     * @param value the value
     *
     * @return the value size in bytes for this map
     */
    override fun valueSize(value: V): Int {
        return database.valueSize(value)
    }

    /**
     * Returns the size for segments for this map.
     *
     * @return the size for segments for this map
     */
    override fun sizeOfSegments(): Long {
        return database.sizeOfSegments()
    }

    /**
     * Returns the level of meter for zerro level.
     *
     * @return the level of meter for zerro level
     */
    override fun level0Meter(): Level0Meter {
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
    override fun levelMeter(levelNumber: Int): Optional<LevelMeter> {
        val levelMeter = database.levelMeter(levelNumber)
        return if (levelMeter.isEmpty) Optional.empty() else Optional.ofNullable(levelMeter.get())
    }

    /**
     * Checks if a map contains key.
     * @param key the key
     *
     * @return `true` if a map contains key, `false` otherwise
     */
    override fun containsKey(key: K): Boolean {
        return database.contains(key).get() as Boolean
    }

    /**
     * Checks if a map might contain key.
     * @param key the key
     *
     * @return `true` if a map might contains key, `false` otherwise
     */
    override fun mightContain(key: K): Boolean {
        return database.mightContain(key).get() as Boolean
    }

    /**
     * Returns the head key for this map.
     *
     * @return the head key for this map
     */
    @Suppress("UNCHECKED_CAST")
    override fun head(): MutableMap.MutableEntry<K, V>? {
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
    override fun headOption(): Optional<MutableMap.MutableEntry<K, V>> {
        return Optional.ofNullable(head())
    }

    /**
     * Returns the last key for this map.
     *
     * @return the last key for this map
     */
    @Suppress("UNCHECKED_CAST")
    override fun last(): MutableMap.MutableEntry<K, V>? {
        val result = database.lastOption().get()
        if (result is scala.Some<*>) {
            val tuple2 = result.get() as scala.Tuple2<K, V>
            return AbstractMap.SimpleEntry(tuple2._1(), tuple2._2())
        }
        return null
    }

    /**
     * Returns the optional last key for this map.
     *
     * @return the optional last key for this map
     */
    override fun lastOption(): Optional<MutableMap.MutableEntry<K, V>> {
        return Optional.ofNullable(last())
    }

    /**
     * Checks if a map contains value.
     * @param value the value
     *
     * @return `true` if a map contains value, `false` otherwise
     */
    override fun containsValue(value: V): Boolean {
        return values().contains(value)
    }

    /**
     * Puts a map object to this map.
     * @param map the map
     */
    override fun put(map: MutableMap<K, V>) {
        val entries = scala.collection.JavaConverters.mapAsScalaMapConverter<K, V>(map).asScala()
        database.put(entries.toSet()).get()
    }

    /**
     * Puts an entry object to this map.
     * @param entry the entry
     */
    override fun put(entry: MutableMap.MutableEntry<K, V>) {
        database.put(entry.key, entry.value)
    }

    /**
     * Updates map entries for this map.
     * @param map the map
     */
    override fun update(map: MutableMap<K, V>) {
        val entries = scala.collection.JavaConverters.mapAsScalaMapConverter<K, V>(map).asScala()
        database.update(entries.toSet()).get()
    }

    /**
     * Clears this map.
     */
    override fun clear() {
        database.asScala().clear()
    }

    /**
     * Returns the key set for this map.
     *
     * @return the key set for this map
     */
    override fun keySet(): MutableSet<K> {
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
    override fun keysHead(): K? {
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
    override fun keysHeadOption(): Optional<K> {
        return Optional.ofNullable(keysHead())
    }

    /**
     * Returns the last key for this map.
     *
     * @return the last key for this map
     */
    @Suppress("UNCHECKED_CAST")
    override fun keysLast(): K? {
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
    override fun keysLastOption(): Optional<K> {
        return Optional.ofNullable(keysLast())
    }

    /**
     * Returns the values for this map.
     *
     * @return the values last key for this map
     */
    override fun values(): List<V> {
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
    override fun entrySet(): MutableSet<MutableMap.MutableEntry<K, V>> {
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
    override fun put(key: K, value: V): V? {
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
    override fun put(key: K, value: V, expireAfter: Long, timeUnit: TimeUnit): V? {
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
    override fun put(key: K, value: V, expireAt: LocalDateTime): V? {
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
    override fun expire(key: K, after: Long, timeUnit: TimeUnit): V? {
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
    override fun expire(key: K, expireAt: LocalDateTime): V? {
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
    override fun update(key: K, value: V): V? {
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
    override operator fun get(key: K): V? {
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
    override fun remove(key: K): V? {
        val oldValue = get(key)
        database.remove(key).get()
        return oldValue
    }

    /**
     * Removes the values for key set of this map.
     * @param keys the keys
     */
    override fun remove(keys: MutableSet<K>) {
        database.remove(scala.collection.JavaConverters.asScalaSetConverter(keys).asScala()).get()
    }

    /**
     * Removes the values for keys of this map.
     * @param from the from
     * @param to the to
     */
    override fun remove(from: K, to: K) {
        database.remove(from, to).get()
    }

    /**
     * Returns the java map of this map.
     *
     * @return the java map of this map
     */
    override fun asJava(): MutableMap<K, V> {
        return JavaConverters.mapAsJavaMapConverter(database.asScala()).asJava()
    }

    /**
     * Registers the function for this map.
     * @param functionId the functionId
     * @param function the function
     *
     * @return the function id
     */
    override fun registerFunction(functionId: K, function: (V) -> Apply.Map<V>): K {
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
    override fun applyFunction(key: K, functionId: K) {
        database.applyFunction(key, functionId)
    }

    /**
     * Returns the map object which starts from key for this map.
     * @param key the key
     *
     * @return the map object
     */
    override fun from(key: K): Map<K, V> {
        return Map(database.from(key))
    }

    /**
     * Returns the map object which starts or after key for this map.
     * @param key the key
     *
     * @return the map object
     */
    override fun fromOrAfter(key: K): Map<K, V> {
        return Map(database.fromOrAfter(key))
    }

    /**
     * Returns the map object which starts or before key for this map.
     * @param key the key
     *
     * @return the map object
     */
    override fun fromOrBefore(key: K): Map<K, V> {
        return Map(database.fromOrBefore(key))
    }

    /**
     * Returns the key objects for this map.
     *
     * @return the key objects for this map
     */
    override fun keys(): swaydb.Set<K, IO<*>> {
        return database.keys()
    }

    /**
     * Returns the reversed map object for this map.
     *
     * @return the reversed map object for this map
     */
    override fun reverse(): Map<K, V> {
        return Map(database.reverse())
    }

    /**
     * Starts the map function for this map.
     * @param function the function
     *
     * @return the stream object for this map
     */
    @Suppress("UNCHECKED_CAST")
    override fun map(function: (MutableMap.MutableEntry<K, V>) -> MutableMap.MutableEntry<K, V>): swaydb.kotlin.Stream<K, V> {
        return swaydb.kotlin.Stream(database.map(object : AbstractFunction1<Tuple2<K, V>, Any>() {
            override fun apply(tuple2: Tuple2<K, V>): Any {
                val result = function(
                        AbstractMap.SimpleEntry(tuple2._1(), tuple2._2()))
                return Tuple2.apply<K, V>(result.key, result.value)
            }
        }) as swaydb.Stream<K, V>)
    }

    /**
     * Starts the drop function for this map.
     * @param count the count
     *
     * @return the stream object for this map
     */
    @Suppress("UNCHECKED_CAST")
    override fun drop(count: Int): swaydb.kotlin.Stream<K, V> {
        return swaydb.kotlin.Stream(database.drop(count) as swaydb.Stream<K, V>)
    }

    /**
     * Starts the dropWhile function for this map.
     * @param predicate the function
     *
     * @return the stream object for this map
     */
    @Suppress("UNCHECKED_CAST")
    override fun dropWhile(predicate: (MutableMap.MutableEntry<K, V>) -> Boolean): swaydb.kotlin.Stream<K, V> {
        return swaydb.kotlin.Stream(database.dropWhile(object : AbstractFunction1<Tuple2<K, V>, Any>() {
            override fun apply(tuple2: Tuple2<K, V>): Any {
                return predicate(AbstractMap.SimpleEntry(tuple2._1(), tuple2._2()))
            }
        }) as swaydb.Stream<K, V>)
    }

    /**
     * Starts the take function for this map.
     * @param count the count
     *
     * @return the stream object for this map
     */
    @Suppress("UNCHECKED_CAST")
    override fun take(count: Int): swaydb.kotlin.Stream<K, V> {
        return swaydb.kotlin.Stream(database.take(count) as swaydb.Stream<K, V>)
    }

    /**
     * Starts the takeWhile function for this map.
     * @param predicate the function
     *
     * @return the stream object for this map
     */
    @Suppress("UNCHECKED_CAST")
    override fun takeWhile(predicate: (MutableMap.MutableEntry<K, V>) -> Boolean): swaydb.kotlin.Stream<K, V> {
        return swaydb.kotlin.Stream(database.takeWhile(object : AbstractFunction1<Tuple2<K, V>, Any>() {
            override fun apply(tuple2: Tuple2<K, V>): Any {
                return predicate(AbstractMap.SimpleEntry(tuple2._1(), tuple2._2()))
            }
        }) as swaydb.Stream<K, V>)
    }

    /**
     * Starts the foreach function for this map.
     * @param consumer the consumer
     *
     * @return the stream object for this map
     */
    @Suppress("UNCHECKED_CAST")
    override fun foreach(consumer: (MutableMap.MutableEntry<K, V>) -> Unit): swaydb.kotlin.Stream<K, V> {
        return swaydb.kotlin.Stream(database.foreach(object : AbstractFunction1<Tuple2<K, V>, Any>() {
            override fun apply(tuple2: Tuple2<K, V>): Any? {
                consumer(AbstractMap.SimpleEntry(tuple2._1(), tuple2._2()))
                return null
            }
        }) as swaydb.Stream<K, V>)
    }

    /**
     * Starts the filter function for this map.
     * @param predicate the function
     *
     * @return the stream object for this map
     */
    @Suppress("UNCHECKED_CAST")
    override fun filter(predicate: (MutableMap.MutableEntry<K, V>) -> Boolean): swaydb.kotlin.Stream<K, V> {
        return swaydb.kotlin.Stream(database.filter(object : AbstractFunction1<Tuple2<K, V>, Any>() {
            override fun apply(tuple2: Tuple2<K, V>): Any {
                return predicate(AbstractMap.SimpleEntry(tuple2._1(), tuple2._2()))
            }
        }) as swaydb.Stream<K, V>)
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
    override fun commit(vararg prepares: Prepare<K, V>): Level0Meter {
        val preparesList = Arrays.asList(*prepares)
        val prepareIterator = JavaConverters.iterableAsScalaIterableConverter(preparesList).asScala()
        return database.commit(prepareIterator).get() as Level0Meter
    }

    class Builder<K, V> {

        private var dir: Path? = null
        private var maxOpenSegments = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$2`<Any, Any>()
        private var cacheSize = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$3`<Any, Any>()
        private var mapSize = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$4`<Any, Any>()
        private var mmapMaps = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$5`<Any, Any>()
        private var recoveryMode: swaydb.data.config.RecoveryMode = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$6`<Any, Any>()
        private var mmapAppendix = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$7`<Any, Any>()
        private var mmapSegments: swaydb.data.config.MMAP = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$8`<Any, Any>()
        private var segmentSize = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$9`<Any, Any>()
        private var appendixFlushCheckpointSize = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$10`<Any, Any>()
        private var otherDirs: Seq<Dir> = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$11`<Any, Any>()
        private var cacheCheckDelay = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$12`<Any, Any>()
        private var segmentsOpenCheckDelay = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$13`<Any, Any>()
        private var bloomFilterFalsePositiveRate = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$14`<Any, Any>()
        private var compressDuplicateValues = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$15`<Any, Any>()
        private var deleteSegmentsEventually = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$16`<Any, Any>()
        private var lastLevelGroupingStrategy: Option<KeyValueGroupingStrategy> = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$17`<Any, Any>()
        private var acceleration: Function1<Level0Meter, Accelerator> = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$18`<Any, Any>()
        private var keySerializer: Any? = null
        private var valueSerializer: Any? = null

        fun withDir(dir: Path): Builder<K, V> {
            this.dir = dir
            return this
        }

        fun withMaxOpenSegments(maxOpenSegments: Int): Builder<K, V> {
            this.maxOpenSegments = maxOpenSegments
            return this
        }

        fun withCacheSize(cacheSize: Int): Builder<K, V> {
            this.cacheSize = cacheSize
            return this
        }

        fun withMapSize(mapSize: Int): Builder<K, V> {
            this.mapSize = mapSize
            return this
        }

        fun withMmapMaps(mmapMaps: Boolean): Builder<K, V> {
            this.mmapMaps = mmapMaps
            return this
        }

        fun withRecoveryMode(recoveryMode: swaydb.data.config.RecoveryMode): Builder<K, V> {
            this.recoveryMode = recoveryMode
            return this
        }

        fun withMmapAppendix(mmapAppendix: Boolean): Builder<K, V> {
            this.mmapAppendix = mmapAppendix
            return this
        }

        fun withMmapSegments(mmapSegments: swaydb.data.config.MMAP): Builder<K, V> {
            this.mmapSegments = mmapSegments
            return this
        }

        fun withSegmentSize(segmentSize: Int): Builder<K, V> {
            this.segmentSize = segmentSize
            return this
        }

        fun withAppendixFlushCheckpointSize(appendixFlushCheckpointSize: Int): Builder<K, V> {
            this.appendixFlushCheckpointSize = appendixFlushCheckpointSize
            return this
        }

        fun withOtherDirs(otherDirs: Seq<Dir>): Builder<K, V> {
            this.otherDirs = otherDirs
            return this
        }

        fun withCacheCheckDelay(cacheCheckDelay: FiniteDuration): Builder<K, V> {
            this.cacheCheckDelay = cacheCheckDelay
            return this
        }

        fun withSegmentsOpenCheckDelay(segmentsOpenCheckDelay: FiniteDuration): Builder<K, V> {
            this.segmentsOpenCheckDelay = segmentsOpenCheckDelay
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

        fun withLastLevelGroupingStrategy(lastLevelGroupingStrategy: Option<KeyValueGroupingStrategy>): Builder<K, V> {
            this.lastLevelGroupingStrategy = lastLevelGroupingStrategy
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
            val keyOrder = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$21`<Any, Any>(dir,
                    maxOpenSegments, cacheSize, mapSize, mmapMaps, recoveryMode,
                    mmapAppendix, mmapSegments, segmentSize, appendixFlushCheckpointSize, otherDirs,
                    cacheCheckDelay, segmentsOpenCheckDelay,
                    bloomFilterFalsePositiveRate, compressDuplicateValues, deleteSegmentsEventually,
                    lastLevelGroupingStrategy, acceleration)
            val ec = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$22`<Any, Any>(dir,
                    maxOpenSegments, cacheSize, mapSize, mmapMaps,
                    recoveryMode, mmapAppendix, mmapSegments, segmentSize, appendixFlushCheckpointSize,
                    otherDirs, cacheCheckDelay, segmentsOpenCheckDelay,
                    bloomFilterFalsePositiveRate, compressDuplicateValues, deleteSegmentsEventually,
                    lastLevelGroupingStrategy, acceleration)
            return Map(
                    swaydb.persistent.`Map$`.`MODULE$`.apply(dir,
                            maxOpenSegments,
                            cacheSize, mapSize, mmapMaps, recoveryMode, mmapAppendix, mmapSegments, segmentSize,
                            appendixFlushCheckpointSize, otherDirs, cacheCheckDelay, segmentsOpenCheckDelay,
                            bloomFilterFalsePositiveRate, compressDuplicateValues, deleteSegmentsEventually,
                            lastLevelGroupingStrategy, acceleration, Serializer.classToType(keySerializer),
                            Serializer.classToType(valueSerializer), keyOrder, ec).get() as swaydb.Map<K, V, IO<*>>)
        }
    }

    companion object {

        /**
         * Creates the map.
         * @param <K> the type of the key element
         * @param <V> the type of the value element
         * @param keySerializer the keySerializer
         * @param valueSerializer the valueSerializer
         * @param dir the directory
         *
         * @return the map
         */
        @Suppress("UNCHECKED_CAST")
        fun <K, V> create(keySerializer: Any,
                          valueSerializer: Any, dir: Path): Map<K, V> {
        val maxOpenSegments = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$2`<Any, Any>()
        val mapSize = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$3`<Any, Any>()
        val maxMemoryLevelSize = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$4`<Any, Any>()
        val maxSegmentsToPush = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$5`<Any, Any>();
        val memoryLevelSegmentSize = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$6`<Any, Any>();
        val persistentLevelSegmentSize = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$7`<Any, Any>();
        val persistentLevelAppendixFlushCheckpointSize = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$8`<Any, Any>();
        val mmapPersistentSegments = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$9`<Any, Any>();
        val mmapPersistentAppendix = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$10`<Any, Any>();
        val cacheSize = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$11`<Any, Any>();
        val otherDirs = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$12`<Any, Any>();
        val cacheCheckDelay = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$13`<Any, Any>();
        val segmentsOpenCheckDelay = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$14`<Any, Any>();
        val bloomFilterFalsePositiveRate = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$15`<Any, Any>();
        val compressDuplicateValues = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$16`<Any, Any>();
        val deleteSegmentsEventually = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$17`<Any, Any>();
        val groupingStrategy =
                swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$18`<Any, Any>();
        val acceleration = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$19`<Any, Any>();
        val keyOrder = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$22`<Any, Any>(
                dir, maxOpenSegments, mapSize, maxMemoryLevelSize, maxSegmentsToPush, memoryLevelSegmentSize,
                persistentLevelSegmentSize, persistentLevelAppendixFlushCheckpointSize, mmapPersistentSegments,
                mmapPersistentAppendix, cacheSize, otherDirs, cacheCheckDelay, segmentsOpenCheckDelay,
                bloomFilterFalsePositiveRate, compressDuplicateValues, deleteSegmentsEventually,
                groupingStrategy, acceleration);
        val ec = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$23`<Any, Any>(
                dir, maxOpenSegments, mapSize, maxMemoryLevelSize, maxSegmentsToPush, memoryLevelSegmentSize,
                persistentLevelSegmentSize, persistentLevelAppendixFlushCheckpointSize,
                mmapPersistentSegments, mmapPersistentAppendix, cacheSize, otherDirs,
                cacheCheckDelay, segmentsOpenCheckDelay, bloomFilterFalsePositiveRate,
                compressDuplicateValues, deleteSegmentsEventually, groupingStrategy, acceleration);
        return Map(
                swaydb.eventually.persistent.`Map$`.`MODULE$`.apply(dir,
                maxOpenSegments, mapSize, maxMemoryLevelSize, maxSegmentsToPush,
                memoryLevelSegmentSize, persistentLevelSegmentSize, persistentLevelAppendixFlushCheckpointSize,
                mmapPersistentSegments, mmapPersistentAppendix, cacheSize, otherDirs,
                cacheCheckDelay, segmentsOpenCheckDelay, bloomFilterFalsePositiveRate,
                compressDuplicateValues, deleteSegmentsEventually, groupingStrategy, acceleration,
                Serializer.classToType(keySerializer), Serializer.classToType(valueSerializer), keyOrder, ec).get() as swaydb.Map<K, V, IO<*>>);
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
