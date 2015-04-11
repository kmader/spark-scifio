package fourquant.io

import _root_.io.scif.img.ImgOpener
import fourquant.io.IOOps._
import fourquant.io.ScifioOps.ImgWithDim
import net.imglib2.`type`.numeric.RealType
import net.imglib2.`type`.numeric.complex.ComplexFloatType
import net.imglib2.`type`.numeric.real.FloatType
import net.imglib2.algorithm.fft2.FFT
import net.imglib2.img.Img
import net.imglib2.img.array.{ArrayImg, ArrayImgFactory}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.scalatest.{FunSuite, Matchers}

class ImgLibOpsTests extends FunSuite with Matchers {
  lazy val sc = new SparkContext("local[4]","Test")
  import fourquant.io.ImgLibOpsTests._
  val io = new ImgOpener()

  val xdim = 100
  val ydim = 120

  lazy val testImgPath = fourquant.io.ImageIOTests.makeImage(xdim,ydim)

  def compareImages[T<: RealType[T]](imgList: Seq[Img[T]]): Unit = {
    val crs = imgList.map(_.cursor())

    while(crs.head.hasNext()) {
      crs.map(_.fwd())
      val floatVals = crs.map(_.get.getRealDouble())
      val ref = floatVals.head
      for(otherVal <- floatVals.tail) {
        ref should equal (otherVal+-0.01)
      }
    }
  }


  test("Complex number arrays") {
    /**
     * this test is to make sure I understand how arraydata access works for complex numbers
     */
    val mf = new ArrayImgFactory[ComplexFloatType]()
    val mr = mf.create(Array[Long](10,10,1),new ComplexFloatType())
    val cursor = mr.localizingCursor()
    // set the first two points
    cursor.fwd() // start off
    cursor.get().setComplexNumber(1,2)
    cursor.fwd()
    cursor.get().setComplexNumber(3,4)
    import fourquant.io.ScifioOps.ImgWithDim // for primitive array access


    mr.getUnderlyingObj match {
          case Some(fArr: Array[Float]) =>
            fArr.length should equal (200)
            fArr(0) should equal (1.0f+-0.01f)
            fArr(1) should equal (2.0f+-0.01f)
            fArr(2) should equal (3.0f+-0.01f)
            fArr(3) should equal (4.0f+-0.01f)
          case Some(junk) =>
            println("Actual data inside:" + junk)
            assert(false,"Should have an array")
          case None =>
            assert(false,"Should support array data access")
        }
  }

  test("Single Random Image FFT") {
    val mf = new ArrayImgFactory[FloatType]()
    val startingImage =
    {
      val sf = mf.create(Array[Long](100,100),new FloatType())
      println("Seed Image:"+ sf.getPrimitiveArray[Array[Float]].map(_.length))
      val cursor = sf.localizingCursor()
      // set the first two points
      var cCounter = 0.0f
      while(cursor.hasNext()) {
        cursor.fwd()
        cursor.get().setReal(cCounter)
        cCounter+=1
      }
      sf
    }
    val forRevFFT = fftOp(startingImage)

    compareImages(Seq(startingImage,forRevFFT))

  }


  test("Gaussian apply op") {

    val pImgData = sc.floatImages(testImgPath).
      mapValues(iImg => iImg.applyOp(gaussianOp))

    assert(pImgData.count==1,"only one image")

    val firstImage = pImgData.first._2.getImg

    firstImage.firstElement().getRealDouble should equal (9720.0+-100)

  }




  test("Image FFT Test") {

    val startingImages = sc.floatImages(testImgPath).cache

    val firstStartingImage = startingImages.first._2.getImg

    val pImgData = startingImages.
      mapValues(iImg => iImg.applyOp(fftOp))

    assert(pImgData.count==1,"only one image")

    val firstOutImage = pImgData.first._2.getImg
    compareImages(Seq(firstStartingImage,firstOutImage))
    firstOutImage.firstElement().getRealDouble should equal (
      firstStartingImage.firstElement().getRealDouble+-0.01)

  }






  test("Read a big image in spark") {

    val pImgData = sc.doubleImages("/Users/mader/Dropbox/4Quant/Volume_Viewer_2.tif").cache

    pImgData.count should equal (1)

    val firstImage = pImgData.first._2.getImg.asInstanceOf[ArrayImg[FloatType,_]]
    firstImage.dimension(0) should equal (684)

    firstImage.dimension(1) should equal (800)

  }

}

object ImgLibOpsTests extends Serializable {


  val gaussianOp =
    (x: ArrayImg[FloatType,_]) => net.imglib2.algorithm.gauss.Gauss.toFloat(Array(3.0,3.0),x)

  val fftOp =
    (input: ArrayImg[FloatType,_]) => {
      val complexFactory = input.factory().imgFactory(new ComplexFloatType())
      val fftImage = FFT.realToComplex(input, complexFactory)
      println("FFT Image:"+ fftImage.getPrimitiveArray[Array[Float]].map(_.length))
      // calculate the inverse
      val inverse = input.factory().create(input.getTrimmedDimensions,
        new FloatType())
      println("Out Image:"+ inverse.getPrimitiveArray[Array[Float]].map(_.length))
      FFT.complexToRealUnpad(fftImage,inverse)
      inverse
    }







}


