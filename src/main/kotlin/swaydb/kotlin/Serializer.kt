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
package swaydb.kotlin

import java.util.HashMap
import swaydb.serializers.Default
import kotlin.reflect.KClass

object Serializer {

    private val CLASS_TO_TYPE = HashMap<KClass<*>, swaydb.serializers.Serializer<*>>()

    init {
        CLASS_TO_TYPE.put(Int::class, Default.`IntSerializer$`.`MODULE$`)
        CLASS_TO_TYPE.put(String::class, Default.`StringSerializer$`.`MODULE$`)
        CLASS_TO_TYPE.put(Long::class, Default.`LongSerializer$`.`MODULE$`)
        CLASS_TO_TYPE.put(Char::class, Default.`CharSerializer$`.`MODULE$`)
        CLASS_TO_TYPE.put(Float::class, Default.`FloatSerializer$`.`MODULE$`)
        CLASS_TO_TYPE.put(Short::class, Default.`ShortSerializer$`.`MODULE$`)
        CLASS_TO_TYPE.put(ByteArray::class, Default.`ArraySerializer$`.`MODULE$`)
        CLASS_TO_TYPE.put(Double::class, Default.`DoubleSerializer$`.`MODULE$`)
    }

    fun classToType(clazz:Any?):swaydb.serializers.Serializer<*> {
        if (clazz is swaydb.serializers.Serializer<*>) {
            return clazz
        }
        return CLASS_TO_TYPE.getOrDefault((clazz as KClass<*>), Default.`StringSerializer$`.`MODULE$`)
    }
}
