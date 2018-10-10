package main

import (
	"bytes"
	"fmt"
	"os"

	"golang.org/x/net/context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	kafka "github.com/segmentio/kafka-go"
)

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func main() {
	os.Setenv("AWS_ACCESS_KEY_ID", "")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "")
	os.Setenv("AWS_REGION", "eu-west-1")

	bucket := "logs.store.my"

	sess, err := session.NewSession()
	if err != nil {
		fmt.Println("No credentials found")
	}

	s3put := s3manager.NewUploader(sess)
	ctx := context.Background()
	// make a new reader that consumes from "secaf-cloudtrail-logs" topic of kafka
	r := kafka.NewReader(
		kafka.ReaderConfig{
			Brokers: []string{"my-kafka-kafka:9092"},
			Topic:   "secaf-cloudtrail-logs",
		})

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			break
		}
		if len(m.Value) == 0 {
			continue
		}
		allValue := m.Value[:]
		fnameSize := int(allValue[0])
		filename := string(allValue[1 : fnameSize+1])
		fmt.Println("fnameSize:       ", fnameSize)
		//fmt.Println("print filename:      ", filename)

		msgValue := allValue[fnameSize+1:]
		// Upload gz content to a s3 bucket for long term storage
		_, err = s3put.Upload(
			&s3manager.UploadInput{
				Body:   bytes.NewReader(msgValue),
				Bucket: aws.String(bucket),
				Key:    aws.String(filename),
				//Key: aws.String(string(fname)),
			})

		if err != nil {
			exitErrorf("Unable to upload %d to %s, %v", m.Offset, bucket, err)
		}
		fmt.Printf("message at offset %d:  %s\n", m.Offset, filename)

		r.CommitMessages(ctx)
	}
	r.Close()

}
