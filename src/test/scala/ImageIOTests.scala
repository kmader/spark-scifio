package fourquant

import java.awt.image.BufferedImage
import java.io.File
import javax.imageio.ImageIO

import _root_.io.scif.img.ImgOpener
import net.imglib2.`type`.numeric.real.FloatType
import net.imglib2.img.array.ArrayImgFactory
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import fourquant.io.IOOps._
import scala.collection.JavaConversions._

class ImageIOTests extends FunSuite {
  lazy val sc = new SparkContext("local[4]","Test")
  import fourquant.ImageIOTests._
  val io = new ImgOpener()

  val xdim = 100
  val ydim = 120

  lazy val testImgPath = makeImage(xdim,ydim)
  test("Create a fake image") {
    val a = new File(testImgPath)
    assert(a.exists,"Does the file exist after creating it")
    val iimg = ImageIO.read(a)
    assert(iimg.getWidth==xdim,"Correct width")
    assert(iimg.getHeight==ydim,"Correct height")
  }

  test("Read a fake image in ImgLib2") {
    val inImage = io.openImgs[FloatType](testImgPath,new ArrayImgFactory[FloatType],new FloatType)
    assert(inImage.size()==1,"There is only one image in the file")
    val firstImage = inImage.head
    assert(firstImage.numDimensions()==2,"is 2D Image")
    assert(firstImage.dimension(0)==xdim,"has the right width")
    assert(firstImage.dimension(1)==ydim,"has the right height")
  }

  test("Read a fake image in spark") {
    val pImgData = sc.floatImages(testImgPath).cache

    assert(pImgData.count==1,"only one image")

    val firstImage = pImgData.first._2.FloatImg

    assert(firstImage.numDimensions()==3,"is 2D Image with one slice")
    assert(firstImage.dimension(0)==xdim,"has the right width")
    assert(firstImage.dimension(1)==ydim,"has the right height")
    assert(firstImage.dimension(2)==1,"has only one slice")
  }

}


object ImageIOTests extends Serializable {
  def makeImage(xdim: Int, ydim: Int): String = {
    val tempFile = File.createTempFile("junk",".png")
    val emptyImage = new BufferedImage(xdim,ydim,BufferedImage.TYPE_USHORT_GRAY)
    val g = emptyImage.getGraphics()
    g.drawString("Hey!",50,50)
    ImageIO.write(emptyImage,"png",tempFile)
    println("PNG file written:"+tempFile.getAbsolutePath)
    tempFile.getAbsolutePath

  }
}