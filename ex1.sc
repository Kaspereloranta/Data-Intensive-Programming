object ex1 {
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet
		/** Task #1 **/
	
	def sum(a: Int, b: Int): Int = {
	a+b
	}                                         //> sum: (a: Int, b: Int)Int
	
	val s = sum(20,21)                        //> s  : Int = 41
	
	/** Task #2 **/
	
	def pascal(column: Int, row: Int): Int = {
		if(column > row ) 0
		else if(column == 0) 1
		else if (row == 0) column
		else (row*pascal(column-1,row-1))/column
	}                                         //> pascal: (column: Int, row: Int)Int
	
	val b = pascal(3,6)                       //> b  : Int = 20
	/** Task #3**/
	def balance(chars:List[Char]):Boolean = {
		if(chars.indexOf('(') == -1 % chars.lastIndexOf(')') == -1) true
		else if(chars.indexOf('(') != -1 % chars.lastIndexOf(')') != -1) balance(chars.slice(chars.indexOf('('),chars.lastIndexOf(')')))
		else false
	}                                         //> balance: (chars: List[Char])Boolean
	
	val str = List('a','(','b',')')           //> str  : List[Char] = List(a, (, b, ))-
	val t = balance(str)
	
		/** Task #4 **/
	def square(a:Int):Int = {
	a*a
	}
	
	val a = Array(1,2,3,4,5)
	
	val squares = a.map(square)
	val sumSquares = squares.reduceLeft(_+_)
	
		/** Task #5 **/
		
		"sheena is a punk rocker she is a punk punk".split(" ").map(s => (s, 1)).
           groupBy(p => p._1).mapValues(v => v.length)
		
		/** First the string is splitted by space " " so basically the words are distinguished from the sentence and
		then the frequencies of each word is counted and saved to map-structure in which the key is the word of interest
		and the value of that key is the number of occurences of that word in the sentence. **/
	
	"sheena is a punk rocker she is a punk punk".split(" ").map((_, 1)).
           groupBy(_._1).mapValues(v => v.map(_._2).reduce(_+_))
	/** The outcome of the second code snippet is similar to first one, so I guess the point of it is to show that same kind
	of things can be coded in multiple ways in Scala. **/
	}