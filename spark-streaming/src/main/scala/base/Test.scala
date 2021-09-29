package base

object Test {
  def main(args: Array[String]): Unit = {

    val list = List("1 1", "2 2", "3 3")
    /*list.flatMap{
      println("xxx")
      _.split(" ")
    }*/

    /*list.flatMap{
      s => {
        println("xxx")
        s.split(" ")
      }
    }*/

    list.flatMap ({
      println("xxx")
      s => s.split(" ")

    })









  }
}
