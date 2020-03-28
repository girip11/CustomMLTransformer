package imageTransformer

import imageTransformer.Main.getImageAsByteArray
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class ImageToWidthMapperTest extends AnyFlatSpec {
  private val sparkSession = SparkSession
    .builder()
    .appName("Spark Image width transformer")
    .master("local")
    .getOrCreate()

  "Image to width mapper" should "return width 1920 for image project-1287781_1920.jpg" in {
    val imagePath: String = "/home/girish/Pictures/project-1287781_1920.jpg"

    // Converting the image to byte array since the transformer expect byte array as the input
    val image: Array[Byte] = getImageAsByteArray(imagePath)

    val data = sparkSession.createDataFrame(Seq(
      (image, 1),
    )).toDF("Image", "ImageID")

    val imageToWidthMapper = new ImageToWidthMapper("ImageToWidthMapper")
    val output = imageToWidthMapper.transform(data)
    val collectedResults = output.collect()

    assert(collectedResults.head.getAs[Int](imageToWidthMapper.getOutputCol) == 1920)
    assert(collectedResults.head.getAs[Int]("ImageID") == 1)
  }
}
