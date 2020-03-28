package imageTransformer

import java.io.{ByteArrayOutputStream, File}
import java.nio.file.Paths

import javax.imageio.ImageIO
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    println("Hello world")

    val sparkSession = SparkSession
      .builder()
      .appName("Spark Image width transformer")
      .master("local")
      .getOrCreate()

    // Converting the image to byte array since the transformer expect byte array as the input
    val image: Array[Byte] = getImageAsByteArray("/home/girish/Pictures/project-1287781_1920.jpg")


    val data = sparkSession.createDataFrame(Seq(
      (image, 1),
    )).toDF("Image", "ImageID")
    // This transformer maps each image to its width value
    val imageToWidthMapper = new ImageToWidthMapper("ImageToWidthMapper")
    println(s"Original data has ${data.count()} rows.")

    val output = imageToWidthMapper.transform(data)
    output.foreach(row => println(row))
  }

  /**
   * Returns the byte representation of the image
   */
  def getImageAsByteArray(imagePath: String): Array[Byte] = {
    val imageType: String = Paths.get(imagePath).getFileName.toString.split('.').last
    val bufferedImage = ImageIO.read(new File(imagePath))

    val byteOutStream = new ByteArrayOutputStream()
    ImageIO.write(bufferedImage, imageType, byteOutStream)

    val imageByteRep = byteOutStream.toByteArray
    byteOutStream.close()
    imageByteRep
  }
}
