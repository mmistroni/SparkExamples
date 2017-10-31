package edgar

case class Transaction(
  cik: String, purchase: Int, sale: Int, voluntaryTransaction: Int, award: Int, disposition: Int, exercise: Int,
  discretionaryTransaction: Int, exercise2: Int, conversion: Int, expirationShort: Int, expirationLong: Int, //H
  outOfMoneyExercise: Int, inTheMonyExercise: Int, bonaFideGift: Int, smallAcquisition: Int, willAcqOrDisp: Int, //W
  depositOrWithdrawal: Int, otherAcquisitionOrDisp: Int, transInEquitySwap: Int, dispositionChangeControl: Int) //U

object Transaction {
  def apply(cik: String, input: Map[Char, Int]): Transaction = {
    Transaction(cik,
      input.getOrElse('P', 0), input.getOrElse('S', 0), input.getOrElse('V', 0), input.getOrElse('A', 0),
      input.getOrElse('D', 0), input.getOrElse('F', 0), input.getOrElse('I', 0), input.getOrElse('M', 0),
      input.getOrElse('C', 0), input.getOrElse('E', 0), input.getOrElse('H', 0), input.getOrElse('O', 0),
      input.getOrElse('X', 0), input.getOrElse('G', 0), input.getOrElse('L', 0), input.getOrElse('W', 0),
      input.getOrElse('Z', 0), input.getOrElse('J', 0), input.getOrElse('K', 0), input.getOrElse('U', 0))
  }
}


case class Form4Filing(transactionType:String, transactionCount:Long) extends Serializable
  
  
