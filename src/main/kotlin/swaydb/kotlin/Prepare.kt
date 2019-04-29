package swaydb.kotlin

import scala.concurrent.duration.Deadline

object Prepare {

    fun <K, V> put(key: K, value: V): swaydb.Prepare.Put<K, V> {
        return swaydb.Prepare.Put<K, V>(key, value, scala.Option.empty<Deadline>())
    }

    fun <K> remove(key: K): swaydb.Prepare.Remove<K> {
        return swaydb.Prepare.Remove<K>(key, scala.Option.empty<K>(), scala.Option.empty<Deadline>())
    }
}
