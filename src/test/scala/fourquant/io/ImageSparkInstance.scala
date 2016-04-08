package fourquant

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{Matchers, FunSuite}


/**
 * Created by mader on 4/14/15.
 */
trait ImageSparkInstance extends FunSuite with Matchers {

  def useLocal: Boolean

  def bigTests: Boolean

  def useCloud: Boolean

  def defaultPersistLevel = StorageLevel.MEMORY_AND_DISK

  val driverLocalDir = if (bigTests) "/Volumes/WORKDISK/scratch/" else "/scratch/"

  System.setProperty("java.io.tmpdir",driverLocalDir)

  lazy val sconf = {

    val nconf = if(useLocal) {
      new SparkConf().
        setMaster("local[4]").
        set("spark.local.dir",driverLocalDir)
    } else {
      new SparkConf().
        setMaster("spark://merlinc60:7077"). //"spark://MacBook-Air.local:7077"
        set("java.io.tmpdir","/scratch/").
        set("spark.local.dir","/scratch/")
    }
    nconf.
      set("spark.executor.memory", "4g").
      setAppName(this.getClass().getCanonicalName)
  }
  lazy val sc = {
    println(sconf.toDebugString)
    var tsc = new SparkContext(sconf)
    if (!useLocal) {
      SparkContext.jarOfClass(this.getClass()) match {
        case Some(jarFile) =>
          println("Adding "+jarFile)
          tsc.addJar(jarFile)
        case None =>
          println(this.getClass()+" jar file missing")
      }
      tsc.addJar("/Users/mader/Dropbox/Informatics/spark-imageio/assembly/target/spio-assembly-0.1-SNAPSHOT.jar")
      tsc.addJar("/Users/mader/Dropbox/Informatics/spark-imageio/target/spark-imageio-1.0-SNAPSHOT-tests.jar")
    }
    if (useCloud) {
      tsc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "AKIAJM4PPKISBYXFZGKA")
      tsc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey",
        "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")
    }
    tsc
  }

  lazy val testDataDir = if(useLocal) {
    "/Users/mader/Dropbox/Informatics/spark-imageio/test-data/"
  } else {
    "/scratch/"
  }

  lazy val esriImage = if (!useCloud) {
    testDataDir + "Hansen_GFC2014_lossyear_00N_000E.tif"
  } else {
    "s3n://geo-images/"+"Hansen_GFC2014_lossyear_00N_000E.tif"
  }


}
