package swaydb.kotlin.eventually.persistent

import scala.Function1
import scala.Option
import scala.collection.JavaConverters
import scala.collection.Seq
import scala.concurrent.duration.Deadline
import scala.concurrent.duration.FiniteDuration
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
 * The persistent Set of data.
 *
 * @param <K> the type of the key element
 */
class Set<K>(private val database: swaydb.Set<K, IO<*>>) : swaydb.kotlin.Set<K>, Closeable {

    /**
     * Checks if a set is empty.
     *
     * @return `true` if a set is empty, `false` otherwise
     */
    override val isEmpty: Boolean
        get() = database.isEmpty.get() as Boolean

    /**
     * Checks if a set contains key.
     * @param key the key
     *
     * @return `true` if a set contains key, `false` otherwise
     */
    operator override fun contains(key: K): Boolean {
        return database.contains(key).get() as Boolean
    }

    /**
     * Checks if a set might contain key.
     * @param key the key
     *
     * @return `true` if a set might contain key, `false` otherwise
     */
    override fun mightContain(key: K): Boolean {
        return database.mightContain(key).get() as Boolean
    }

    /**
     * Returns the iterator of elements in this set.
     *
     * @return the iterator of elements in this set
     */
    override operator fun iterator(): Iterator<K> {
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
    override fun toArray(): Array<K> {
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
     * @return `true` if a set contained key, `false` otherwise
     */
    override fun add(key: K): Boolean {
        val result = database.add(key).get()
        return result is scala.Some<*>
    }

    /**
     * Adds the key with expire after to this set.
     * @param key the key
     * @param expireAfter the expireAfter
     * @param timeUnit the timeUnit
     *
     * @return `true` if a set contained key, `false` otherwise
     */
    override fun add(key: K, expireAfter: Long, timeUnit: TimeUnit): Boolean {
        val result = contains(key)
        database.add(key, FiniteDuration.create(expireAfter, timeUnit)).get()
        return result
    }

    /**
     * Adds the key with expire at to this set.
     * @param key the key
     * @param expireAt the expireAt
     *
     * @return `true` if a set contained key, `false` otherwise
     */
    override fun add(key: K, expireAt: LocalDateTime): Boolean {
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
    override fun expire(key: K, after: Long, timeUnit: TimeUnit): Boolean {
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
    override fun expire(key: K, expireAt: LocalDateTime): Boolean {
        val result = contains(key)
        val expireAtNano = Duration.between(LocalDateTime.now(), expireAt).nano
        database.expire(key, FiniteDuration.create(expireAtNano.toLong(), TimeUnit.NANOSECONDS).fromNow()).get()
        return result
    }

    /**
     * Checks if a set contains key collection.
     * @param collection the collection
     *
     * @return `true` if a set contains key, `false` otherwise
     */
    override fun containsAll(collection: Collection<K>): Boolean {
        return collection.stream()
                .allMatch { elem -> database.contains(elem).get() as Boolean }
    }

    /**
     * Adds the keys to this set.
     * @param list the list
     *
     * @return `true` if a set contained keys, `false` otherwise
     */
    override fun add(list: List<K>): Boolean {
        val entries = scala.collection.JavaConverters.asScalaBufferConverter(list).asScala()
        database.add(entries.toSet()).get()
        return true
    }

    /**
     * Retains the keys to this set.
     * @param collection the collection
     *
     * @return `true` if a set contained keys, `false` otherwise
     */
    override fun retainAll(collection: Collection<K>): Boolean {
        val entries = database.asScala().toSeq()
        val result = ArrayList<K>()
        var index = 0
        while (index < entries.size()) {
            result.add(entries.apply(index))
            index += 1
        }
        result.stream()
                .filter { elem -> !collection.contains(elem) }
                .forEach({ database.remove(it) })
        return true
    }

    /**
     * Removes the keys of this set.
     * @param keys the keys
     */
    override fun remove(keys: MutableSet<K>) {
        database.remove(scala.collection.JavaConverters.asScalaSetConverter<K>(keys).asScala()).get()
    }

    /**
     * Removes the keys of this set.
     * @param from the from
     * @param to the to
     */
    override fun remove(from: K, to: K) {
        database.remove(from, to).get()
    }

    /**
     * Returns the size of elements in this set.
     *
     * @return the size of elements in this set
     */
    override fun size(): Int {
        return database.asScala().size()
    }

    /**
     * Checks if a set is not empty.
     *
     * @return `true` if a set is not empty, `false` otherwise
     */
    override fun nonEmpty(): Boolean {
        return database.nonEmpty().get() as Boolean
    }

    /**
     * Returns the expiration date for key in this set.
     * @param key the key
     *
     * @return the expiration date for key in this set
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
     * Returns the time left for key in this set.
     * @param key the key
     *
     * @return the time left for key in this set
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
     * Returns the size for segments for this set.
     *
     * @return the size for segments for this set
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
     * Clears this set.
     */
    override fun clear() {
        database.asScala().clear()
    }

    /**
     * Removes the key of this set.
     * @param key the key
     *
     * @return `true` if old key was present, `false` otherwise
     */
    override fun remove(key: K): Boolean {
        val result = database.remove(key).get()
        return result is scala.Some<*>
    }

    /**
     * Returns the java set of this set.
     *
     * @return the java set of this set
     */
    override fun asJava(): MutableSet<K> {
        return JavaConverters.setAsJavaSetConverter(database.asScala()).asJava()
    }

    /**
     * Closes the database.
     */
    override fun close() {
        database.closeDatabase().get()
    }

    /**
     * Starts the commit function for this set.
     * @param prepares the prepares
     *
     * @return the level zerro for this set
     */
    override fun commit(vararg prepares: Prepare<K, scala.runtime.`Nothing$`>):Level0Meter
    {
        val preparesList = Arrays.asList<Prepare<K, scala.runtime.`Nothing$`>>(*prepares)
        val prepareIterator = JavaConverters.iterableAsScalaIterableConverter<Prepare<K, scala.runtime.`Nothing$`>>(preparesList).asScala()
        return database.commit(prepareIterator).get() as Level0Meter
    }

    class Builder<K> {

        private var dir: Path? = null
        private var maxOpenSegments = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$2`<K, scala.runtime.`Nothing$`>()
        private var cacheSize = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$3`<K, scala.runtime.`Nothing$`>()
        private var mapSize = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$4`<K, scala.runtime.`Nothing$`>()
        private var mmapMaps = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$5`<K, scala.runtime.`Nothing$`>()
        private var recoveryMode: swaydb.data.config.RecoveryMode = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$6`<K, scala.runtime.`Nothing$`>()
        private var mmapAppendix = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$7`<K, scala.runtime.`Nothing$`>()
        private var mmapSegments: swaydb.data.config.MMAP = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$8`<K, scala.runtime.`Nothing$`>()
        private var segmentSize = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$9`<K, scala.runtime.`Nothing$`>()
        private var appendixFlushCheckpointSize = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$10`<K, scala.runtime.`Nothing$`>()
        private var otherDirs: Seq<Dir> = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$11`<K, scala.runtime.`Nothing$`>()
        private var cacheCheckDelay = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$12`<K, scala.runtime.`Nothing$`>()
        private var segmentsOpenCheckDelay = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$13`<K, scala.runtime.`Nothing$`>()
        private var bloomFilterFalsePositiveRate = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$14`<K, scala.runtime.`Nothing$`>()
        private var compressDuplicateValues = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$15`<K, scala.runtime.`Nothing$`>()
        private var deleteSegmentsEventually = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$16`<K, scala.runtime.`Nothing$`>()
        private var lastLevelGroupingStrategy: Option<KeyValueGroupingStrategy> = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$17`<K, scala.runtime.`Nothing$`>()
        private var acceleration: Function1<Level0Meter, Accelerator> = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$18`<K, scala.runtime.`Nothing$`>()
        private var keySerializer: Any? = null

        fun withDir(dir: Path): Builder<K> {
            this.dir = dir
            return this
        }

        fun withMaxOpenSegments(maxOpenSegments: Int): Builder<K> {
            this.maxOpenSegments = maxOpenSegments
            return this
        }

        fun withCacheSize(cacheSize: Int): Builder<K> {
            this.cacheSize = cacheSize
            return this
        }

        fun withMapSize(mapSize: Int): Builder<K> {
            this.mapSize = mapSize
            return this
        }

        fun withMmapMaps(mmapMaps: Boolean): Builder<K> {
            this.mmapMaps = mmapMaps
            return this
        }

        fun withRecoveryMode(recoveryMode: swaydb.data.config.RecoveryMode): Builder<K> {
            this.recoveryMode = recoveryMode
            return this
        }

        fun withMmapAppendix(mmapAppendix: Boolean): Builder<K> {
            this.mmapAppendix = mmapAppendix
            return this
        }

        fun withMmapSegments(mmapSegments: swaydb.data.config.MMAP): Builder<K> {
            this.mmapSegments = mmapSegments
            return this
        }

        fun withSegmentSize(segmentSize: Int): Builder<K> {
            this.segmentSize = segmentSize
            return this
        }

        fun withAppendixFlushCheckpointSize(appendixFlushCheckpointSize: Int): Builder<K> {
            this.appendixFlushCheckpointSize = appendixFlushCheckpointSize
            return this
        }

        fun withOtherDirs(otherDirs: Seq<Dir>): Builder<K> {
            this.otherDirs = otherDirs
            return this
        }

        fun withCacheCheckDelay(cacheCheckDelay: FiniteDuration): Builder<K> {
            this.cacheCheckDelay = cacheCheckDelay
            return this
        }

        fun withSegmentsOpenCheckDelay(segmentsOpenCheckDelay: FiniteDuration): Builder<K> {
            this.segmentsOpenCheckDelay = segmentsOpenCheckDelay
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

        fun withLastLevelGroupingStrategy(lastLevelGroupingStrategy: Option<KeyValueGroupingStrategy>): Builder<K> {
            this.lastLevelGroupingStrategy = lastLevelGroupingStrategy
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
            val keyOrder = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$21`<K, scala.runtime.`Nothing$`>(dir,
                    maxOpenSegments, cacheSize, mapSize, mmapMaps, recoveryMode,
                    mmapAppendix, mmapSegments, segmentSize, appendixFlushCheckpointSize, otherDirs,
                    cacheCheckDelay, segmentsOpenCheckDelay,
                    bloomFilterFalsePositiveRate, compressDuplicateValues, deleteSegmentsEventually,
                    lastLevelGroupingStrategy, acceleration)
            val ec = swaydb.persistent.`Map$`.`MODULE$`.`apply$default$22`<K, scala.runtime.`Nothing$`>(dir,
                    maxOpenSegments, cacheSize, mapSize, mmapMaps,
                    recoveryMode, mmapAppendix, mmapSegments, segmentSize, appendixFlushCheckpointSize,
                    otherDirs, cacheCheckDelay, segmentsOpenCheckDelay,
                    bloomFilterFalsePositiveRate, compressDuplicateValues, deleteSegmentsEventually,
                    lastLevelGroupingStrategy, acceleration)
            return Set(
                    swaydb.persistent.`Set$`.`MODULE$`.apply(dir,
                            maxOpenSegments, cacheSize, mapSize, mmapMaps, recoveryMode,
                            mmapAppendix, mmapSegments, segmentSize, appendixFlushCheckpointSize, otherDirs,
                            cacheCheckDelay, segmentsOpenCheckDelay,
                            bloomFilterFalsePositiveRate, compressDuplicateValues, deleteSegmentsEventually,
                            lastLevelGroupingStrategy, acceleration, Serializer.classToType(keySerializer),
                            keyOrder, ec).get() as swaydb.Set<K, IO<*>>)
        }
    }

    companion object {

        /**
         * Creates the set.
         * @param <K> the type of the key element
         * @param keySerializer the keySerializer
         * @param dir the dir
         *
         * @return the set
         */
        @Suppress("UNCHECKED_CAST")
        fun <K> create(keySerializer: Any, dir: Path): Set<K> {
            val maxOpenSegments = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$2`<K, scala.runtime.`Nothing$`>()
            val mapSize = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$3`<K, scala.runtime.`Nothing$`>()
            val maxMemoryLevelSize = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$4`<K, scala.runtime.`Nothing$`>()
            val maxSegmentsToPush = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$5`<K, scala.runtime.`Nothing$`>()
            val memoryLevelSegmentSize = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$6`<K, scala.runtime.`Nothing$`>()
            val persistentLevelSegmentSize = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$7`<K, scala.runtime.`Nothing$`>()
            val persistentLevelAppendixFlushCheckpointSize =
                    swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$8`<K, scala.runtime.`Nothing$`>()
            val mmapPersistentSegments = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$9`<K, scala.runtime.`Nothing$`>();
            val mmapPersistentAppendix = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$10`<K, scala.runtime.`Nothing$`>();
            val cacheSize = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$11`<K, scala.runtime.`Nothing$`>()
            val otherDirs = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$12`<K, scala.runtime.`Nothing$`>()
            val cacheCheckDelay = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$13`<K, scala.runtime.`Nothing$`>()
            val segmentsOpenCheckDelay = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$14`<K, scala.runtime.`Nothing$`>()
            val bloomFilterFalsePositiveRate = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$15`<K, scala.runtime.`Nothing$`>()
            val compressDuplicateValues = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$16`<K, scala.runtime.`Nothing$`>()
            val deleteSegmentsEventually = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$17`<K, scala.runtime.`Nothing$`>()
            val groupingStrategy =
                    swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$18`<K, scala.runtime.`Nothing$`>()
            val acceleration = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$19`<K, scala.runtime.`Nothing$`>()
            val keyOrder = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$22`<K, scala.runtime.`Nothing$`>(
                    dir, maxOpenSegments, mapSize, maxMemoryLevelSize, maxSegmentsToPush, memoryLevelSegmentSize,
                    persistentLevelSegmentSize, persistentLevelAppendixFlushCheckpointSize, mmapPersistentSegments,
                    mmapPersistentAppendix, cacheSize, otherDirs, cacheCheckDelay, segmentsOpenCheckDelay,
                    bloomFilterFalsePositiveRate, compressDuplicateValues, deleteSegmentsEventually,
                    groupingStrategy, acceleration)
            val ec = swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$23`<K, scala.runtime.`Nothing$`>(
                    dir, maxOpenSegments, mapSize, maxMemoryLevelSize, maxSegmentsToPush, memoryLevelSegmentSize,
                    persistentLevelSegmentSize, persistentLevelAppendixFlushCheckpointSize,
                    mmapPersistentSegments, mmapPersistentAppendix, cacheSize, otherDirs,
                    cacheCheckDelay, segmentsOpenCheckDelay, bloomFilterFalsePositiveRate,
                    compressDuplicateValues, deleteSegmentsEventually, groupingStrategy, acceleration)
            return Set(
                    swaydb.eventually.persistent.`Set$`.`MODULE$`.apply(dir,
                            maxOpenSegments, mapSize, maxMemoryLevelSize, maxSegmentsToPush,
                            memoryLevelSegmentSize, persistentLevelSegmentSize, persistentLevelAppendixFlushCheckpointSize,
                            mmapPersistentSegments, mmapPersistentAppendix, cacheSize, otherDirs,
                            cacheCheckDelay, segmentsOpenCheckDelay, bloomFilterFalsePositiveRate,
                            compressDuplicateValues, deleteSegmentsEventually, groupingStrategy, acceleration,
                            Serializer.classToType(keySerializer), keyOrder, ec).get() as swaydb.Set<K, IO<*>>)
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
