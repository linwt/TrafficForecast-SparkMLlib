package utils

import java.util.Properties

object PropertyUtil {

    val properties = new Properties()
    try {
        val inputStream = ClassLoader.getSystemResourceAsStream("kafka.properties")
        properties.load(inputStream)
    } catch {
        case e: Exception => e.getStackTrace
    } finally {}

    def getProperties(key: String): String = properties.getProperty(key)
}
