package swaydb.kotlin

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import java.time.Duration
import java.time.LocalDateTime

object Apply {
    fun <V> update(value: V): swaydb.Apply.Map<V> {
        return swaydb.Apply.`Update$`.`MODULE$`.apply(value)
    }

    fun <V> expire(expireAt: LocalDateTime): swaydb.Apply.Map<V> {
        val expireAtNano = Duration.between(LocalDateTime.now(), expireAt).getNano()
        return swaydb.Apply.`Expire$`.`MODULE$`.apply(
                FiniteDuration.create(expireAtNano.toLong(), TimeUnit.NANOSECONDS).fromNow()) as swaydb.Apply.Map<V>
    }

    fun <V> remove(): swaydb.Apply.Map<V> {
        return swaydb.Apply.`Remove$`.`MODULE$` as swaydb.Apply.Map<V>
    }

    fun <V> nothing(): swaydb.Apply.Map<V> {
        return swaydb.Apply.`Nothing$`.`MODULE$` as swaydb.Apply.Map<V>
    }
}