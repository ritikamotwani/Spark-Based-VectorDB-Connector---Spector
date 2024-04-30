import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession
import scala.xml._
//import spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql._
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types._
import com.johnsnowlabs.nlp.annotators.Tokenizer
import com.johnsnowlabs.nlp.base.DocumentAssembler
import com.johnsnowlabs.nlp.embeddings.BertEmbeddings
import com.johnsnowlabs.nlp.embeddings.BertSentenceEmbeddings
import com.johnsnowlabs.nlp.EmbeddingsFinisher
import org.apache.spark.ml.Pipeline
//newer imports
import org.apache.spark.sql.functions.udf
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.functions.col
import java.util.UUID
import org.apache.spark.ml.linalg.Vector


object XmlParser {
   def main(args: Array[String]): Unit = args(0) match {
     case "CSV" => {
        val sc = new SparkContext()
        val spark = SparkSession.builder().appName("Project").master("local").getOrCreate()
        import spark.implicits._
        
        var filePath = ""
        var ColToEncode = ""
        var Primary_Id = ""
        var Secondary_Id = ""

        if (args.length != 5) {
          println("Usage: MyApp CSV <filePath> <colToEncode> <Primary_Id> <Secondary_Id>")
          System.exit(1)
        } else {
          filePath = args(1)
          ColToEncode = args(2)
          Primary_Id = args(3)
          Secondary_Id =args(4)
        }

       
        val finalDf = spark.read.option("header", "true").option("multiLine", "true").option("inferSchema", "true").option("escape", "\"").csv(filePath)
        val documentAssembler = new DocumentAssembler().setInputCol(ColToEncode).setOutputCol("document") 
        // Embedding code
        //val documentAssembler = new DocumentAssembler().setInputCol("sentence").setOutputCol("document")

        val embeddings = BertSentenceEmbeddings.pretrained("sent_small_bert_L2_128").setInputCols("document").setOutputCol("sentence_bert_embeddings")

        val embeddingsFinisher = new EmbeddingsFinisher().setInputCols("sentence_bert_embeddings").setOutputCols("finished_embeddings").setOutputAsVector(true)

        val pipeline = new Pipeline().setStages(Array(
          documentAssembler,
          embeddings,
          embeddingsFinisher
        ))

        val result = pipeline.fit(finalDf).transform(finalDf)

        val flattenEmbeddings = udf((vectors: Seq[Vector]) =>vectors.flatMap(_.toArray))
        val flattenedResult = result.withColumn("flattened_embeddings", flattenEmbeddings(col("finished_embeddings"))).select(Primary_Id,Secondary_Id, ColToEncode, "flattened_embeddings")
        val vectorDataFrame=flattenedResult.withColumnRenamed(Primary_Id, "Main_id").withColumnRenamed(Secondary_Id, "id").withColumnRenamed("flattened_embeddings", "Embedding").withColumnRenamed(ColToEncode, "raw")
        vectorDataFrame.show(1)
        println("started writing to JSON")
        vectorDataFrame.write.option("lineSep", "\n").mode("overwrite").json("partitioned_jsonl")
        println("finished writing to json")
        sc.stop()  // optional
     }

     case "JSON" => {
       val sc = new SparkContext()
        val spark = SparkSession.builder().appName("Project").master("local").getOrCreate()
        import spark.implicits._
        
        var filePath = ""
        var ColToEncode = ""
        var Primary_Id = ""
        var Secondary_Id = ""

        if (args.length != 5) {
          println("Usage: MyApp JSON <filePath> <colToEncode> <Primary_Id> <Secondary_Id>")
          System.exit(1)
        } else {
          filePath = args(1)
          ColToEncode = args(2)
          Primary_Id = args(3)
          Secondary_Id =args(4)
        }

        val df = sc.wholeTextFiles("/user/sxp8182_nyu_edu/articles-json/*").toDF("DocID", "Content")
    
        // Defining schema for reading
        val jsonSchema = new StructType().add("docId", StringType).add("body_text", ArrayType(new StructType().add("sentence", StringType).add("secId", StringType)))
     
        // Applying the schema
        val parsedDf = df.withColumn("parsedContent", from_json($"Content", jsonSchema))
    
        // Explode the 'body_text' array to create a row for each sentence along with its associated 'secId'
        val explodedDf = parsedDf.select($"parsedContent.DocId".alias("DocId"),explode($"parsedContent.body_text").as("body_text"))

        // This will create the desired dataframe (document Id, sentenceId and the sentence)
        val finalDf = explodedDf.select($"DocId",$"body_text.sentence".as("sentence"),$"body_text.secId".as("secID"))
      
        // Embedding code
        val documentAssembler = new DocumentAssembler().setInputCol("sentence").setOutputCol("document")

        val embeddings = BertSentenceEmbeddings.pretrained("sent_small_bert_L2_128").setInputCols("document").setOutputCol("sentence_bert_embeddings")

        val embeddingsFinisher = new EmbeddingsFinisher().setInputCols("sentence_bert_embeddings").setOutputCols("finished_embeddings").setOutputAsVector(true)

        val pipeline = new Pipeline().setStages(Array(
          documentAssembler,
          embeddings,
          embeddingsFinisher
        ))

        val result = pipeline.fit(finalDf).transform(finalDf)
result.show(5)
   //     val flattenEmbeddings = udf((vectors: Seq[Vector]) =>vectors.flatMap(_.toArray))
     //   val flattenedResult = result.withColumn("flattened_embeddings", flattenEmbeddings(col("finished_embeddings"))).select(Primary_Id,Secondary_Id, ColToEncode, "flattened_embeddings")
       // val vectorDataFrame=flattenedResult.withColumnRenamed(Primary_Id, "Main_id").withColumnRenamed(Secondary_Id, "id").withColumnRenamed("flattened_embeddings", "Embedding").withColumnRenamed(ColToEncode, "raw")
        //vectorDataFrame.show(1)
        //println("started writing to JSON")
        //vectorDataFrame.write.option("lineSep", "\n").mode("overwrite").json("partitioned_jsonl")
        //println("finished writing to json")
        sc.stop()  // optional 
     }
     case "TXT"  => {
        val sc = new SparkContext()
        val spark = SparkSession.builder().appName("Project").master("local").getOrCreate()
        import spark.implicits._
        
        var filePath = ""
        var Primary_Id = ""
        var ChunkSize = ""
        var ContentLineStart = -1

        if (args.length != 5) {
          println("Usage: MyApp <TXT> <filePath> <ContentLineStart> <Primary_Id> <ChunkSize>")
          System.exit(1)
        } else {
          filePath = args(1)
          ContentLineStart = args(2).toInt
          Primary_Id = args(3)
          ChunkSize =args(4)
        }

        val df = spark.sparkContext.wholeTextFiles(filePath).toDF("BookID", "Content")
        def extractBookName(content: String): String = {
            val lines = content.split("\n")
            val titleLine = lines.find(line => line.startsWith("Title: "))
            titleLine.getOrElse("Unknown").stripPrefix("Title: ")
        }

        val extractBookNameUDF = udf(extractBookName _)

        val extractedDF = Primary_Id match {
            case "" => df.withColumn("BookName", extractBookNameUDF(col("Content")))
            case _ => df.withColumn("BookName", lit(Primary_Id))
        }

        val bookNameDF =extractedDF.drop("Content")
        bookNameDF.show(1)
        def extractBookContent(content: String): String = {
            val lines = content.split("\n")
            var startIndex = 0
            if (ContentLineStart != -1) {
                startIndex = ContentLineStart
            } else {
                startIndex = lines.indexWhere(line => line.startsWith("*** START OF THE PROJECT GUTENBERG EBOOK"))
            }
            if (startIndex != -1 && startIndex + 1 < lines.length) {
                val slicedLines = lines.slice(startIndex + 1, lines.length)
                val nonEmptyContent = slicedLines.map(_.trim).filter(_.nonEmpty).mkString("\n")
                nonEmptyContent
            } else {
                println("Start index not found or no content after start index")
                content
            }
        }
        val extractBookContentUDF = udf(extractBookContent _)

        val temp = df.withColumn("BookContent", extractBookContentUDF(col("Content")))
        val bookContentDF = temp.drop("Content")

        def splitIntoChunks(content: String, chunkSize: Int): Seq[String] = {
        content.split("\n").grouped(chunkSize).map(_.mkString("\n")).toSeq
        }
        val splitIntoChunksUDF = udf(splitIntoChunks(_: String, _: Int))


        val chunkSize = ChunkSize

        val chunkedDF = bookContentDF.withColumn("Chunks", explode(splitIntoChunksUDF(col("BookContent"), lit(chunkSize))))

        val addChunkIdUDF = udf(() => UUID.randomUUID().toString)
        val chunkedDataFrame = chunkedDF
        .withColumn("ChunkId", addChunkIdUDF())
        .select("BookId", "ChunkId", "Chunks")
        val documentAssembler = new DocumentAssembler()
        .setInputCol("Chunks")
        .setOutputCol("document")
        val embeddings = BertSentenceEmbeddings.pretrained("sent_small_bert_L2_128")
        .setInputCols("document")
        .setOutputCol("sentence_bert_embeddings")
        val embeddingsFinisher = new EmbeddingsFinisher()
        .setInputCols("sentence_bert_embeddings")
        .setOutputCols("finished_embeddings")
        .setOutputAsVector(true)
        val pipeline = new Pipeline().setStages(Array(
        documentAssembler,
        embeddings,
        embeddingsFinisher
        ))
        val result = pipeline.fit(chunkedDataFrame).transform(chunkedDataFrame)

        val flattenEmbeddings = udf((vectors: Seq[Vector]) => 
            vectors.flatMap(_.toArray)
        )

        val flattenedResult = result.withColumn("flattened_embeddings", flattenEmbeddings(col("finished_embeddings"))).select("BookId", "ChunkId", "Chunks", "flattened_embeddings")
        val vectorDataFrame = flattenedResult
        .withColumnRenamed("ChunkId", "chunk_id")
        .withColumnRenamed("Chunks", "raw")
        .withColumnRenamed("flattened_embeddings", "vector")
        .withColumnRenamed("BookId", "id")
        vectorDataFrame.show(10)
        vectorDataFrame.write
        .format("json")
        .option("lineSep", "\n")
        .mode("overwrite")
        .save("partitioned_jsonl")
        sc.stop()  // optional 
     }

     case _ => {
       println("unsupported format please use either CSV or TXT or JSON")
     }
   }
}