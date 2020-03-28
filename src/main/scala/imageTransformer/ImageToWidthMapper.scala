package imageTransformer

import java.io.ByteArrayInputStream

import javax.imageio.ImageIO
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{DataTypes, StructType}

//References:
// 1. https://docs.databricks.com/applications/machine-learning/mllib/advanced-mllib.html
// 2. https://www.oreilly.com/content/extend-spark-ml-for-your-own-modeltransformer-types/
// 3. https://nlp.johnsnowlabs.com/api/index#com.johnsnowlabs.nlp.DocumentAssembler
class ImageToWidthMapper(override val uid: String) extends Transformer {
    //Must implement transform, transformSchema and copy
    /**
     * Param for input column name.
     */
    final val inputCol: Param[String] = new Param[String](this, "inputCol", "Input Column containing image as byte array")

    final def getInputCol: String = $(inputCol)

    final def setInputCol(value: String): ImageToWidthMapper = set(inputCol, value)

    /**
     * Param for output column name.
     */
    final val outputCol: Param[String] = new Param[String](this, "outputCol", "output column containing the width of the file")

    final def getOutputCol: String = $(outputCol)

    final def setOutputCol(value: String): ImageToWidthMapper = set(outputCol, value)

    // (Optional) You can set defaults for Param values if you like.
    setDefault(inputCol -> "Image", outputCol -> "ImageWidth")

  /**
   * This does the actual transformation of image to its width
   * @param image - Image as byte array
   * @return Width of the image
   */
    private def getImageWith(image: Array[Byte]): Int = {
      val byteInStream = new ByteArrayInputStream(image)
      val bufferedImage = ImageIO.read(byteInStream)
      byteInStream.close()
      bufferedImage.getWidth
    }

    /**
     * Returns the dataframe containing the width of the image along with the image id
     * @param dataset - Input dataframe containing the image and its ID
     * @return
     */
    override def transform(dataset: Dataset[_]): DataFrame = {
      // pass the lambda to udf. Below method call uses the postfix method call notation
      val widthFinder = udf { image: Array[Byte] => this.getImageWith(image) }

      // Since we are returning new columns, select only the newly generated columns
      // Just for image identification sake, I am returning the id from the source data frame
      dataset.select(col("ImageID"), widthFinder(col($(inputCol))).as($(outputCol)))
    }

    override def transformSchema(schema: StructType): StructType = {
      val actualDataType = schema($(inputCol)).dataType

      require(actualDataType.equals(DataTypes.BinaryType),
        s"Column ${$(inputCol)} must be BinaryType but was actually $actualDataType.")

      DataTypes.createStructType(
        Array(
          DataTypes.createStructField($(outputCol), DataTypes.IntegerType, false)
        )
      )
    }

    override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}
