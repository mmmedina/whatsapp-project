package Dataframes
import Dataframes.WordList.{blacklist, curseWordsList}
import org.apache.commons.codec.binary.Hex
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, concat, count, desc, explode, first, lower, not, regexp_extract, to_date, udf}
import org.apache.spark.sql.{SparkSession, functions, functions => F}

import java.security.MessageDigest

object Dataframe extends App {
  //FileService.processZipFile("wapp.zip")
  // Initialize SparkSession
  val spark = SparkSession.builder()
    .appName("Spark Text File App")
    .master("local[*]")
    .getOrCreate()

  // REMOVE HIDDEN CHARACTERS --------------------------------
  // ---------------------------------------------------------

  // READ THE NEW FILE ---------------------------------
  val pattern = """\[(\d{2}/\d{2}/\d{4}), (\d{1,2}:\d{2}:\d{2} [APM]{2})\](?: ~ )?([^:]+): (.*)""".r
  val parseLine = F.udf((line: String) => pattern.findFirstMatchIn(line)
    .map(m => (m.group(1), m.group(2), m.group(3), m.group(4)))
    .getOrElse(("", "", "", line)))

  // Load, parse, and transform the data
  private val df = spark.read.text("src/main/resources/data/chat.txt")
    .withColumn("parsed", parseLine(F.col("value")))
    .select(
      F.col("parsed._1").as("date"),
      F.col("parsed._2").as("hour"),
      F.col("parsed._3").as("name"),
      F.col("parsed._4").as("message")
    )
 //-----------------------------------------------------------

//---------------------------------------QUERIES--------------------------------------------

  //df.orderBy("name").show(1000, truncate = false)
  val startDate = "01/11/2024" // Example date
  val endDate = "31/11/2024" // Example date

  // Convert date to a format Spark can recognize and extract day of the week and day of the month
  private val formattedDF = df.withColumn("formattedDate", F.to_date(F.col("date"), "dd/MM/yyyy"))
    .withColumn("dayOfWeek", F.date_format(F.col("formattedDate"), "EEE")) // "EEE" for abbreviated day name
    .withColumn("dayOfMonth", F.date_format(F.col("formattedDate"), "dd")) // "dd" for day of the month
    .withColumn("dayOfWeekAndDate", F.concat(F.col("dayOfWeek"), F.lit("-"), F.col("dayOfMonth")))
    .withColumn("hourOnly", F.split(F.col("hour"), ":")(0))
    .withColumn("amPm", F.split(F.col("hour"), " ")(1))
    .withColumn("hourAmPm", functions.concat(col("hourOnly"), col("amPm")))
  private val formattedDFWithHour = formattedDF.withColumn("hourOnlyInt", F.col("hourOnly").cast("int"))

  // AMOUNT OF CHATS PER USER -----------------------------------
  private val chatAmountUserDF = formattedDF.filter(F.col("date").between(startDate, endDate))
    .groupBy(col("name"))
    .count()
    .orderBy(F.desc("count"))
    //.show(10, truncate = false)
  // -----------------------------------

  // AVERAGE OF CHATS PER HOUR  -----------------------------------
  private val convertTo24HourFormat = udf((time: String) => {
    val Array(hour, ampm) = time.split(" ")
    val hour24 = ampm match {
      case "AM" => if (hour.toInt == 12) 0 else hour.toInt
      case "PM" => if (hour.toInt == 12) 12 else hour.toInt + 12
    }
    hour24
  })

  private val hourlyCountsDF = formattedDFWithHour.filter(F.col("date").between(startDate, endDate))
    .groupBy("dayOfWeek", "hourOnlyInt", "amPm")
    .agg(F.count("*").as("chatCount"))

  private val avgChatHourAuxDF = hourlyCountsDF
    .groupBy("dayOfWeek","hourOnlyInt", "amPm")
    .agg(F.avg("chatCount").as("averageChatCount"))
    .withColumn("averageChatCount", F.floor(F.col("averageChatCount")))
    .orderBy("dayOfWeek", "hourOnlyInt", "amPm")

  private val avgChatHourDF = avgChatHourAuxDF
    .withColumn("hour", convertTo24HourFormat(concat(F.col("hourOnlyInt"), F.lit(" "), F.col("amPm"))))
    .drop("hourOnlyInt", "amPm")

  val avgChatHourCountDF = avgChatHourDF
    .groupBy("hour")
    .pivot("dayOfWeek", Seq("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"))
    .sum("averageChatCount")
    .orderBy("hour")
    .na.fill(0)
    //.show(100, truncate = false)

  // -----------------------------------

  // AVERAGE DISTINCT PEOPLE PER HOUR -----------------------------------
  private val distinctPeopleDF = formattedDFWithHour.filter(F.col("date").between(startDate, endDate))
    .groupBy("dayOfWeek", "hourOnlyInt", "amPm")
    .agg(F.countDistinct("name").as("distinctPeopleCount"))

  private val distinctPeopleHourDF = distinctPeopleDF
    .groupBy("dayOfWeek", "hourOnlyInt", "amPm")
    .agg(F.avg("distinctPeopleCount").as("averageDistinctPeopleCount"))
    .withColumn("averageDistinctPeopleCount", F.floor(F.col("averageDistinctPeopleCount")))
    .orderBy("dayOfWeek", "hourOnlyInt", "amPm")

  private val distinctPeopleHourAuxDF = distinctPeopleHourDF
    .withColumn("hour", convertTo24HourFormat(concat(F.col("hourOnlyInt"), F.lit(" "), F.col("amPm"))))
    .drop("hourOnlyInt", "amPm")

  val distinctPeopleHourCountDF = distinctPeopleHourAuxDF
    .groupBy("hour")
    .pivot("dayOfWeek", Seq("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"))
    .sum("averageDistinctPeopleCount")
    .orderBy("hour")
    .na.fill(0)
    //.show(400, truncate = false)
  // -----------------------------------

  // AMOUNT OF WORDS -----------------------------------

  private val messagesDF = formattedDF
    .filter(col("date").between(startDate, endDate))

  // Tokenize the message column into words
  private val wordsDF = messagesDF
    .withColumn("word", explode(F.split(lower(col("message")), "\\s+")))
    .filter(functions.length(col("word")) > 3)
    .filter(not(col("word").isin(blacklist.toSeq: _*))) // Exclude blacklisted words

  // Count word frequencies
  val wordCountDF = wordsDF
    .groupBy("word")
    .agg(count("*").as("count"))
    .orderBy(desc("count"))
    //.show(10, truncate = false)

  // -----------------------------------

  // AMOUNT OF PHOTOS -------------------------------
  val photoMessagesDF = formattedDF
    .filter(col("message").contains("-PHOTO-"))
    .filter(col("date").between(startDate, endDate))

  // Group by the sender's name and count the occurrences
  val imagesPerPersonDF = photoMessagesDF
    .groupBy("name")
    .agg(count("*").as("imageCount"))
    .orderBy(desc("imageCount"))

  // Show the result
  //imagesPerPersonDF.show(10, truncate = false)
  // ------------------------------------------------

  // AMOUNT OF AUDIOS -------------------------------
  val audioMessagesDF = formattedDF
    .filter(col("message").contains("-AUDIO-"))
    .filter(col("date").between(startDate, endDate))

  // Group by the sender's name and count the occurrences
  val audiosPerPersonDF = audioMessagesDF
    .groupBy("name")
    .agg(count("*").as("audioCount"))
    .orderBy(desc("audioCount"))

  // Show the result
 // audiosPerPersonDF.show(10, truncate = false)
  // ------------------------------------------------

  // AMOUNT OF STICKERS PER PERSON -------------------------------
  val stickerMessagesDF = formattedDF
    .filter(col("message").contains("-STICKER-"))
    .filter(col("date").between(startDate, endDate))

  // Group by the sender's name and count the occurrences
  val stickersPerPersonDF = stickerMessagesDF
    .groupBy("name")
    .agg(count("*").as("stickerCount"))
    .orderBy(desc("stickerCount"))

  // Show the result
  //stickersPerPersonDF.show(10, truncate = false)
  // ------------------------------------------------

  // AMOUNT OF DELETED MESSAGES -------------------------------
  val deletedMessagesDF = formattedDF
    .filter(col("message").contains("This message was deleted"))
    .filter(col("date").between(startDate, endDate))

  // Group by the sender's name and count the occurrences
  val deletedPerPersonDF = deletedMessagesDF
    .groupBy("name")
    .agg(count("*").as("deletedCount"))
    .orderBy(desc("deletedCount"))

  // Show the result
  //deletedPerPersonDF.show(10, truncate = false)
  // ------------------------------------------------

  // AMOUNT OF STICKERS -----------------------------------

  // Load binary files
  private val binaryFilesDF = spark.read
    .format("binaryFile")
    .option("pathGlobFilter", "*.webp")
    .load("src/main/resources/data/wpdata")

  // Define a UDF to compute an MD5 hash of the image content
  private val md5HashImageUDF: UserDefinedFunction = udf((content: Array[Byte]) => {
    val md = MessageDigest.getInstance("MD5")
    md.update(content)
    Hex.encodeHexString(md.digest())
  })

  // Apply the UDF to compute the hash of the image content
  private val imagesWithHashDF = binaryFilesDF
    .withColumn("imageHash", md5HashImageUDF(col("content")))

  // Extract date from filename using regex
  private val imagesWithDateDF = imagesWithHashDF
    .withColumn("dateFromFilename", regexp_extract(col("path"), """(\d{4}-\d{2}-\d{2})""", 1))
    .withColumn("formattedDate", F.date_format(F.to_date(F.col("dateFromFilename"), "yyyy-MM-dd"), "dd/MM/yyyy"))

  private val filteredDF = imagesWithDateDF
    .filter(col("formattedDate").between(startDate, endDate))

  // Count occurrences of each image hash
  val stickerCountDF = filteredDF
    .groupBy("imageHash")
    .agg(
      count("imageHash").as("count"),
      first("path").as("fileName") // Select one file name per hash
    )
    .orderBy(desc("count"))
    .drop("imageHash")
    //.show(10, truncate = false)

  import org.apache.spark.sql.functions._

  // UDF to count curse words in a message
  private val countCurseWordsUDF = udf((message: String) => {
    message.split("\\s+").count(word => curseWordsList.contains(word.toLowerCase))
  })

  // UDF to extract curse words from a message
  private val extractCurseWordsUDF = udf((message: String) => {
    message.split("\\s+").filter(word => curseWordsList.contains(word.toLowerCase)).toSeq
  })

  // DataFrame with curse word count per person
  val curseWordsPerPersonDF = formattedDFWithHour
    .filter(col("date").between(startDate, endDate))
    .withColumn("curseWordsCount", countCurseWordsUDF(col("message")))
    .groupBy("name")
    .agg(sum("curseWordsCount").as("totalCurseWords"))
    .orderBy(desc("totalCurseWords"))

  // DataFrame with the most used curse word
  val curseWordsDF = formattedDFWithHour
    .filter(col("date").between(startDate, endDate))
    .withColumn("curseWords", explode(extractCurseWordsUDF(col("message"))))
    .groupBy("curseWords")
    .agg(count("*").as("count"))
    .orderBy(desc("count"))

  // ---------------------------------------------------------------

  // AMOUNT OF STICKERS -----------------------------------
  val ingresoSemanalCountDF = formattedDF
    .filter(col("date").between(startDate, endDate))
    .filter(lower(col("message")).contains("#ingresosemanal"))
    .agg(count("*").as("ingresoSemanalCount"))

  // ------------------------------------------------
  // write csv
  chatAmountUserDF.limit(10)
    .write
    .format("csv")
    .option("header", "true")
    .option("sep", ",")
    .save("src/main/resources/data/chatAmountUser")
  /*
    // write csv
    avgChatHourCountDF.limit(100)
      .write
      .format("csv")
      .option("header", "true")
      .option("sep", ",")
      .save("src/main/resources/data/avgChatHour")

    // write csv
    distinctPeopleHourCountDF.limit(100)
      .write
      .format("csv")
      .option("header", "true")
      .option("sep", ",")
      .save("src/main/resources/data/distinctPeopleHourCount")
  */
  // write csv
  wordCountDF.limit(200)
    .write
    .format("csv")
    .option("header", "true")
    .option("sep", ",")
    .save("src/main/resources/data/wordCount")

  audiosPerPersonDF.limit(10)
    .write
    .format("csv")
    .option("header", "true")
    .option("sep", ",")
    .save("src/main/resources/data/audiossPerPerson")

    imagesPerPersonDF.limit(10)
      .write
      .format("csv")
      .option("header", "true")
      .option("sep", ",")
      .save("src/main/resources/data/imagesPerPerson")
  /*
      deletedPerPersonDF.limit(10)
        .write
        .format("csv")
        .option("header", "true")
        .option("sep", ",")
        .save("src/main/resources/data/deletedPerPerson")
    */
  stickersPerPersonDF.limit(10)
    .write
    .format("csv")
    .option("header", "true")
    .option("sep", ",")
    .save("src/main/resources/data/stickersPerPerson")

  stickerCountDF.limit(10)
    .write
    .format("csv")
    .option("header", "true")
    .option("sep", ",")
    .save("src/main/resources/data/stickerCount")

  curseWordsDF.limit(10)
    .write
    .format("csv")
    .option("header", "true")
    .option("sep", ",")
    .save("src/main/resources/data/curseWords")

  curseWordsPerPersonDF.limit(10)
    .write
    .format("csv")
    .option("header", "true")
    .option("sep", ",")
    .save("src/main/resources/data/curseWordsPerPerson")

  ingresoSemanalCountDF
    .write
    .format("csv")
    .option("header", "true")
    .option("sep", ",")
    .save("src/main/resources/data/IngresoSemanalCount")
  // Stop the SparkSession
  spark.stop()
}
