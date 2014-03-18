package main

import( 
  "net/http"
  "io/ioutil"  
)



func fetchData(url string) ([]byte, error){
  resp, err := http.Get(url)
  
  if err != nil{
    return nil, err
  }
  
  defer resp.Body.Close()
  return ioutil.ReadAll(resp.Body) 
}
