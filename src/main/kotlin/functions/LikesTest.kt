package functions

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import swaydb.java.PureFunction
import swaydb.java.Return
import swaydb.java.memory.MapConfig
import swaydb.java.serializers.Default
import java.util.stream.IntStream

internal class LikesTest {

  @Test
  fun likesCountTest() {
    //function that increments likes by 1
    //in SQL this would be "UPDATE LIKES_TABLE SET LIKES = LIKES + 1"
    val incrementLikesFunction =
      PureFunction.OnValue<String, Int, Return.Map<Int>> { currentLikes: Int ->
        Return.update(currentLikes + 1)
      }

    val likesMap =
      MapConfig.functionsOn(Default.stringSerializer(), Default.intSerializer())
        .registerFunction(incrementLikesFunction)
        .get()

    likesMap.put("SwayDB", 0) //initial entry with 0 likes.

    //this could also be applied concurrently and the end result is the same.
    //applyFunction is atomic and thread-safe.
    IntStream
      .rangeClosed(1, 100)
      .parallel()
      .forEach { _: Int ->
        likesMap.applyFunction("SwayDB", incrementLikesFunction)
      }

    //assert the number of likes applied.
    assertEquals(100, likesMap["SwayDB"].get())
  }
}