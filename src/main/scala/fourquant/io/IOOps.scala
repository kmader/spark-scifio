package fourquant.io

import fourquant.io.ScifioOps._
import io.scif.img.{ImgOpener, SCIFIOImgPlus}
import net.imglib2.`type`.NativeType
import net.imglib2.`type`.numeric.RealType
import net.imglib2.`type`.numeric.integer.{ByteType, IntType, LongType}
import net.imglib2.`type`.numeric.real.{DoubleType, FloatType}
import net.imglib2.img.ImgFactory
import net.imglib2.img.array.{ArrayImg, ArrayImgFactory}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.reflect.ClassTag

//import net.imglib2.`type`.numeric.real.FloatType
//import net.imglib2.`type`.numeric.integer.IntType

import scala.collection.JavaConversions._

/**
 * A general set of opertions for importing images
 * Created by mader on 2/27/15.
 */
object IOOps {

  /**
   * So they needn't be manually created, they can just be implicitly thrown in
   */
  implicit val floatMaker = () => new FloatType()
  implicit val doubleMaker = () => new DoubleType()
  implicit val intMaker = () => new IntType()
  implicit val longMaker = () => new LongType()
  implicit val byteMaker = () => new ByteType()

  implicit class fqContext(sc: SparkContext) extends Serializable {

    private def staticTypeReadImages[T<: RealType[T]](file: String,iFactory: ImgFactory[T],
                                                      iType: T):
    RDD[(String,SCIFIOImgPlus[T])] = {
      sc.binaryFiles(file).mapPartitions{
        curPart =>
          val io = new ImgOpener()
          curPart.flatMap{
            case (filename,pds) =>
              for (img<-io.openPDS[T](filename,pds,iFactory,iType))
              yield (filename,img)
          }
      }
    }

    /**
     * A generic tool for opening images as Arrays
     * @param filepath the path to the files that need to be loaded
     * @param bType a function which creates new FloatType objects and can be serialized
     * @tparam T the primitive type for the array representation of the image
     * @tparam U the imglib2 type for the ArrayImg representation
     * @return a list of pathnames (string) and image objects (SparkImage)
     */
    def genericArrayImages[T,U <: NativeType[U] with RealType[U]](filepath: String)(implicit
                                    tm: ClassTag[T], bType: () => U):
    RDD[(String, ArraySparkImg[T,U])] = {
      sc.binaryFiles(filepath).mapPartitions{
        curPart =>
          val io = new ImgOpener()
          curPart.flatMap{
            case (filename,pds) =>
              for (img<-io.openPDS[U](filename,pds,new ArrayImgFactory[U], bType() ))
              yield (filename,
                new ArraySparkImg[T,U](Right(img.getImg.asInstanceOf[ArrayImg[U,_]]))
                )
          }
      }
    }

    /**
     * A generic tool for opening images as Arrays
     * @param filepath the path to the files that need to be loaded
     * @param partCount is the number of partitions in the output
     * @param regions is the regions of interest described as (x,y, size in x, size in y)
     * @param bType a function which creates new FloatType objects and can be serialized
     * @tparam T the primitive type for the array representation of the image
     * @tparam U the imglib2 type for the ArrayImg representation
     * @return a list of pathnames (string) and image objects (SparkImage)
     */
    def genericArrayImagesRegion2D[T,
    U <: NativeType[U] with RealType[U]](filepath: String,
                                        partCount: Int,
                                         regions: Array[(Int,Int,Int,Int)])(
      implicit tm: ClassTag[T], bType: () => U):
    RDD[((String,Int,Int), ArraySparkImg[T,U])] = {
      import ScifioOps.FQImgOpener
      val regValues = sc.parallelize(regions)
      sc.binaryFiles(filepath).cartesian(regValues).repartition(partCount).
        mapPartitions{
        curPart =>
          val io = new ImgOpener()
          val localNames = new mutable.HashMap[String,String]() // cache for locally copied files
          curPart.flatMap{
            case ((filename,pds),(sx,sy,wx,wy)) =>
              val suffix = filename.split("[.]").reverse.head;
              val localFileName = localNames.getOrElseUpdate(filename,io.flattenPDS(pds,suffix));
              for (img<-io.openRegion2D[U](localFileName,bType(),sx,sy,wx,wy))
                yield (
                  (filename,sx,sy),
                    new ArraySparkImg[T,U](Right(img.getImg.asInstanceOf[ArrayImg[U,_]]))
                  )
          }
      }
    }



    /**
     * A version of generic array Images for float-type images
     * @return float-formatted images
     */
    def floatImages(filepath: String) =
      sc.genericArrayImages[Float,FloatType](filepath)

    /**
     * A version of generic array Images for double-type images
     * @return float-formatted images
     */
    def doubleImages(filepath: String) =
      sc.genericArrayImages[Double,DoubleType](filepath)

    /**
     * A version of generic array Images for float-type images
     * @return float-formatted images
     */
    def intImages(filepath: String) =
      sc.genericArrayImages[Int,IntType](filepath)
      


  }

}
