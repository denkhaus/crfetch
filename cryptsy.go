package main


import(
  "github.com/stretchr/objx" 
  "fmt"
  "time"
)

func loadCryptsyData(){

  ts := time.Now().Unix()
  data, err := fetchData(CRYPTSY_API_URL)
  
  if err != nil {
    fmt.Println(err)
  }

  m, err := objx.FromJSON(string(data))
  
  if err != nil {
    fmt.Println(err)
  }
    
  if suc := m.Get("success").Float64(); suc == 1 {  
      ret := m.Get("return.markets").MSI()     
    
      for _, symdata := range ret {
        sd := objx.New(symdata)
        
        path := fmt.Sprintf("/mkt/cryptsy/quotes/%d/%s", ts, sd.Get("marketid").Str())
        price := sd.Get("lasttradeprice").Str()
        volume :=sd.Get("volume").Str()
        
        fmt.Printf("%s/v/%s\n", path, volume)
        fmt.Printf("%s/p/%s\n", path, price)      
                   
      }    
  }
}