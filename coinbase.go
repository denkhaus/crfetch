package main

import(
 // "github.com/coreos/go-etcd/etcd"
   "github.com/stretchr/objx" 
  "fmt"  
)

func GenerateCoinbaseCurrencyMap(){

  data, err := fetchData(COINBASE_API_CURRENCIES_URL)

  if err != nil {
    fmt.Println(err)
  }
  
  if data != nil && len(data) != 0{    
    
    jdata := fmt.Sprintf("{ \"data\": %s}",string(data))
    m, err := objx.FromJSON(jdata)
  
    if err != nil {
      fmt.Println(err)
    }   
    
    //machines := []string{}
    //client := etcd.NewClient(machines)     
      
    k := m.Get("data").Data()    
    for symid, symdata := range k.([]interface{}) {      
      f := symdata.([]interface{})          
      path := fmt.Sprintf("/mkt/cnbase/map/%s", f[1])    
      fmt.Printf("%s/id/%d\n",path, symid) 
      fmt.Printf("%s/name/%s\n",path, f[0])
      
      //client.Set(fmt.Sprintf("%s/id",path), symid) 
      //client.Set(fmt.Sprintf("%s/name",path), symname) 
    
    }    
  }    
} 