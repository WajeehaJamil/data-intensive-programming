object ScalaWorksheet {
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet
  def sum( a:Int, b:Int ) : Int = {
      val sum = a + b
      return sum
   }                                              //> sum: (a: Int, b: Int)Int
  val s = sum(6,8)                                //> s  : Int = 14
  
  def pascal(c: Int, r: Int): Int = {
    if (c == 0 || c == r) 1
    else
      pascal(c - 1, r - 1) + pascal(c, r - 1)
  }                                               //> pascal: (c: Int, r: Int)Int
  
	for (row <- 0 to 4) {
	  for (col <- 0 to row)
	    print(pascal(col, row) + " ")
	  println()
	}                                         //> 1 
                                                  //| 1 1 
                                                  //| 1 2 1 
                                                  //| 1 3 3 1 
                                                  //| 1 4 6 4 1 
    
    def balance_string(chars: List[Char]): Boolean = {

    def check(score: Int, chars: List[Char]): Int = {

      if (chars.isEmpty || score < 0)
        score

      else if (chars.head == '(')
        check(score + 1, chars.tail)

      else if (chars.head == ')')
        check(score - 1, chars.tail)

      else check(score, chars.tail)
    }

    0 == check(0, chars)

  }                                               //> balance_string: (chars: List[Char])Boolean
  
  balance_string("(Lord of the (Rings)".toList)   //> res0: Boolean = false
  
  
 	val a = Array(1,2,3,4,5)                  //> a  : Array[Int] = Array(1, 2, 3, 4, 5)
 	val sqs = a.map(s => scala.math.pow(s,2).toInt)
                                                  //> sqs  : Array[Int] = Array(1, 4, 9, 16, 25)
 	sqs.reduceLeft((v1, v2) => v1 + v2)       //> res1: Int = 55
 	

 	val words = "sheena is a punk rocker she is a punk punk".split(" ")
                                                  //> words  : Array[String] = Array(sheena, is, a, punk, rocker, she, is, a, pun
                                                  //| k, punk)
                                                  
	// .map() changes words array elements into a tuple
 	val map = words.map(s => (s, 1))          //> map  : Array[(String, Int)] = Array((sheena,1), (is,1), (a,1), (punk,1), (r
                                                  //| ocker,1), (she,1), (is,1), (a,1), (punk,1), (punk,1))

	// it group the elements to key-value pairs
 	val groups = map.groupBy(p => p._1)       //> groups  : scala.collection.immutable.Map[String,Array[(String, Int)]] = Map
                                                  //| (sheena -> Array((sheena,1)), is -> Array((is,1), (is,1)), a -> Array((a,1)
                                                  //| , (a,1)), she -> Array((she,1)), punk -> Array((punk,1), (punk,1), (punk,1)
                                                  //| ), rocker -> Array((rocker,1)))
                                                 
	// .mapValues also transform each element of collection like .map() function does,
	// but it only applies the predicate function to values of the key-value pairs.
 	val result = groups.mapValues(v => v.length)
                                                  //> result  : scala.collection.immutable.Map[String,Int] = Map(sheena -> 1, is 
                                                  //| -> 2, a -> 2, she -> 1, punk -> 3, rocker -> 1)
                                                  
	val words1 = "sheena is a punk rocker she is a punk punk".split(" ")
                                                  //> words1  : Array[String] = Array(sheena, is, a, punk, rocker, she, is, a, pu
                                                  //| nk, punk)
	val map1 = words1.map((_, 1))             //> map1  : Array[(String, Int)] = Array((sheena,1), (is,1), (a,1), (punk,1), (
                                                  //| rocker,1), (she,1), (is,1), (a,1), (punk,1), (punk,1))
	val groups1 = map1.groupBy(_._1)          //> groups1  : scala.collection.immutable.Map[String,Array[(String, Int)]] = Ma
                                                  //| p(sheena -> Array((sheena,1)), is -> Array((is,1), (is,1)), a -> Array((a,1
                                                  //| ), (a,1)), she -> Array((she,1)), punk -> Array((punk,1), (punk,1), (punk,1
                                                  //| )), rocker -> Array((rocker,1)))
                                                  
	// _ is used a placeholder instead of some variable name
	// difference is here in this mapValues() function
	// here instead of counting length of each map values, we map through the 2nd element of each map and calculate their sum
	val result1 = groups1.mapValues(v => v.map(_._2).reduce(_+_))
                                                  //> result1  : scala.collection.immutable.Map[String,Int] = Map(sheena -> 1, is
                                                  //|  -> 2, a -> 2, she -> 1, punk -> 3, rocker -> 1)


}