
package main

import( 
  "time"
)



func main(){
  for{
    GenerateCoinbaseCurrencyMap()
    //loadCryptsyData()
    time.Sleep(time.Minute)
  }
}