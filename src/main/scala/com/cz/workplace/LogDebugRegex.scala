package com.cz.workplace

package cn.edu360.spark.scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.matching.Regex



// Assume all log are one log per line
// sample logDebugRegex -a | * * ^[0-9] src dest
//sample 2 logDebugRegex -s |
//sample 3 lgoDebugRegex | src des
object LogDebugRegex {

  //创建sparkConf对象

  //创建SparkContext对象

  def main(args:Array[String]): Unit = {
//Assumption one entry of log per line

// verify user have input enough argument
//verify argument input and get correctly
// scala throw error

    // split wif deliniter
    // use matcher to match the corresponding out saveTextFile if not match
    //
    // check simple version
    // unit test
  // log (src,des, delimiter,regex1, field no(1-n)

    val conf = new SparkConf().setAppName("LogDebugRegex").setMaster("local[4]")

    val sc = new SparkContext(conf)





    val src = if (args.length >=5 ) args(0) else throw new IllegalArgumentException("no enough argument")

    val log:RDD[String] = sc.textFile(src)

   // val des = args(1)

    val delimiter:String = args(2)

    val matchJobArgNo = (args.length - 3)

    if (matchJobArgNo%2 == 1 ) throw new IllegalArgumentException("no enough argument")

    var matchJobNo = matchJobArgNo/2

    val logField = log.map(line => line.split(delimiter))
    var currentRegex:Int = 3
    var currentFieldNo:Int = 4
    while(matchJobNo != 0){


      val Reg = args(currentRegex).r
      var matchFieldNo = args(currentFieldNo).toInt-1
      val badSingleRecords = logField.filter(x => !(Reg.pattern.matcher(x(matchFieldNo)).matches)).map(x => (x(0),x(1),x(7)))

      currentFieldNo+=2
      currentRegex+=2
      matchJobNo-= 1
      badSingleRecords.repartition(1).saveAsTextFile(args(1))
    }
    sc.stop()
    /*firstArg match {
      case "-s" => {println("single field")
        val delimiter = args(1)
        val matchMode = "s"
      }      //single field match
      case "-a" => {println("all field match")


        }    //all field match
      case _ => {println("single")
        val  delimiter = args(0)
      }
    }

    if(firstArg !== ""){
      val delimiter = args(1)

    }else
      {
      val  delimiter = args(0)
      }
*/

  }
  }
