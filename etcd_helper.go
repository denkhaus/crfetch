package main

import (
		"github.com/coreos/go-etcd/etcd"
  "fmt"
)


func (c *etcd.Client) GetValue(path string) (string, error){
  
  resp, err := client.Get(key, false, false)
  
  if err != nil{
    return nil, err  
  }
  
  if resp.Node.Dir {
    return nil, fmt.Errorf("provided path % is direcory and no key", path)
  }
  
  return resp.Node.Key, nil  
}

func (c *etcd.Client) KeyCount(path string) (int, error){
  
  resp, err := client.Get(key, false, false)
  
  if err != nil{
    return 0, err  
  }
  
  if ! resp.Node.Dir {
    return 0 , fmt.Errorf("provided path % is key and no directory", path)
  }
  
  nCount := 0
  for _, node := range resp.Node.Nodes {
    if ! node.Dir {
		    nCount++
    }
	}
  
  return nCount, nil  
}