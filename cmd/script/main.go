package main

import (
	"DeltaReceiver/internal/common/conf"
	"DeltaReceiver/internal/common/model"
	"DeltaReceiver/internal/common/repo/cs"
	"DeltaReceiver/internal/common/repo/parquet"
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gocql/gocql"
	xs3 "github.com/xitongsys/parquet-go-source/s3"
	"github.com/xitongsys/parquet-go/reader"
	"gopkg.in/yaml.v3"
)

func main() {
	csRepoCfg := conf.CsRepoConfig{
		Hosts:                 []string{"91.198.220.162"},
		KeySpace:              "binance_data",
		DeltaTableName:        "deltas",
		SnapshotTableName:     "snapshots",
		ExchangeInfoTableName: "exchange_info",
		BookTicksTableName:    "book_ticks",
	}
	cluster := gocql.NewCluster(csRepoCfg.Hosts...)
	cluster.Keyspace = csRepoCfg.KeySpace
	cluster.Port = 30042
	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	repo := cs.NewCsDeltaStorage(session, csRepoCfg.DeltaTableName)
	deltas := []model.Delta{{
		Timestamp:     123,
		Price:         "asd",
		Count:         "sdfdsf",
		UpdateId:      123123,
		FirstUpdateId: 321321,
		T:             true,
		Symbol:        "ABOBA",
	},
	}
	err = repo.SendDeltas(context.Background(), deltas)
	if err != nil {
		panic(err)
	}
}

func GetDeltas() {
	s3CfgPath := addS3()
	bucketName := addBucketName()
	key := addKey()
	flag.Parse()
	s3Cfg := getS3Cfg(s3CfgPath)
	s3Storage := parquet.NewS3ParquetStorage(s3Cfg)
	obj := s3Storage.GetObject(&s3.GetObjectInput{
		Bucket: bucketName,
		Key:    key,
	})
	fmt.Printf("%s/%s\n", *bucketName, *key)
	fmt.Println(obj.GoString())
	fileReader, err := xs3.NewS3FileReaderWithClient(context.Background(), s3Storage.Client, *bucketName, *key)
	if err != nil {
		panic(err)
	}
	deltaReader, err := reader.NewParquetReader(fileReader, new(model.Delta), 4)

	if err != nil {
		panic(err)
	}
	deltas := make([]model.Delta, deltaReader.GetNumRows())
	err = deltaReader.Read(&deltas)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Got %d deltas\n", len(deltas))
	for _, delta := range deltas {
		fmt.Println(delta)
	}
}

func ListKeysInBucket() {
	s3CfgPath := addS3()
	bucketName := addBucketName()
	prefix := addPrefix()
	flag.Parse()
	s3Cfg := getS3Cfg(s3CfgPath)
	s3Storage := parquet.NewS3ParquetStorage(s3Cfg)
	out := s3Storage.ListObjects(&s3.ListObjectsV2Input{
		Bucket: bucketName,
		Prefix: prefix,
	})
	log.Printf("Got %d objects\n", out.KeyCount)
	for i, object := range out.Contents {
		if i == 10 {
			break
		}
		log.Println(*object.Key)
	}
}

func GetBuckets() {
	s3CfgPath := addS3()
	flag.Parse()
	s3Cfg := getS3Cfg(s3CfgPath)
	s3Storage := parquet.NewS3ParquetStorage(s3Cfg)
	buckets := s3Storage.ListBuckets()
	for _, bucket := range buckets {
		log.Println(*bucket.Name)
	}
}

func addS3() *string {
	return flag.String("s3Cfg", "s3.yaml", "path to s3 config file")
}

func addBucketName() *string {
	return flag.String("bucket", "", "bucket name")
}

func addPrefix() *string {
	return flag.String("prefix", "", "prefix")
}

func addKey() *string {
	return flag.String("key", "", "key name")
}

func getS3Cfg(cfgPath *string) *conf.S3Cfg {
	s3CfgData, err := os.ReadFile(*cfgPath)
	if err != nil {
		panic(err)
	}
	var s3Cfg conf.S3Cfg
	if err = yaml.Unmarshal(s3CfgData, &s3Cfg); err != nil {
		panic(err)
	}
	return &s3Cfg
}
