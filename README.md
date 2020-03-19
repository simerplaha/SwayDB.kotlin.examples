# SwayDB.kotlin.examples [![Slack Chat][slack-badge]][slack-link] [![Gitter Chat][gitter-badge]][gitter-link] [![Build status][build-badge]][build-link] [![Maven central][maven-badge]][maven-link]

[gitter-badge]: https://badges.gitter.im/Join%20Chat.svg
[gitter-link]: https://gitter.im/SwayDB-chat/Lobby

[slack-badge]: https://img.shields.io/badge/slack-join%20chat-e01563.svg
[slack-link]: https://join.slack.com/t/swaydb/shared_invite/enQtNzI1NzM1NTA0NzQxLTJiNjRhMDg2NGQ3YzBkNGMxZGRmODlkN2M3MWEwM2U2NWY1ZmU5OWEyYTgyN2ZhYjlhNjdlZTM3YWJjMGZmNzQ

[maven-badge]: https://img.shields.io/maven-central/v/io.swaydb/java_2.12.svg
[maven-link]: https://search.maven.org/search?q=g:io.swaydb%20AND%20a:java_2.12

[build-badge]: https://github.com/simerplaha/SwayDB.kotlin.examples/workflows/Java%20CI/badge.svg
[build-link]: https://github.com/simerplaha/SwayDB.kotlin.examples/actions?query=workflow%3A%22Java+CI%22

Implements examples demoing [SwayDB](https://github.com/simerplaha/SwayDB)'s Java API.

Requirements
============

Kotlin 1.3 and later.

### Quick start example.

See full [QuickStart.kt](/quickstart/QuickStart_Function.kt).

```kotlin
val map =
  MapConfig
    .functionsOff(intSerializer(), intSerializer())
    .get()

map.put(1, 1) //basic put
map[1].get() //basic get
map.expire(1, Duration.ofSeconds(1)) //basic expire
map.remove(1) //basic remove

//atomic write a Stream of key-value
map.put(Stream.range(1, 100).map { KeyVal.create(it) })

//Create a stream that updates all values within range 10 to 90.
val updatedKeyValues =
  map
    .from(10)
    .stream()
    .takeWhile { it.key() <= 90 }
    .map { KeyVal.create(it.key(), it.value() + 5000000) }

//submit the stream to update the key-values as a single transaction.
map.put(updatedKeyValues)

//print all key-values to view the update.
map
  .stream()
  .forEach(::println)
```
