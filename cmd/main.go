package main

import (
	"DeltaReceiver/pkg/mongo/conf"
	"fmt"
	"gopkg.in/yaml.v3"
	"log"
	"os"
)

func main() {
	data, err := os.ReadFile("cmd/test.yaml")
	if err != nil {
		log.Println(err.Error())
	}
	var cfg conf.MongoRepoConfig
	yaml.Unmarshal(data, &cfg)
	fmt.Println(cfg)
}
