package s3

import (
	"bytes"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3Config struct {
	AccessKey     string `json:"accessKey"`
	SecretKey     string `json:"secretKey"`
	Region        string `json:"region"`
	Bucket        string `json:"bucket"`
	RequestFormat string `json:"requestFormat"`
	RequestMethod string `json:"requestMethod"`
}

func UploadToS3(s3config S3Config, data []byte) error {
	// Create an AWS session with the provided credentials
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(s3config.Region),
		Credentials: credentials.NewStaticCredentials(s3config.AccessKey, s3config.SecretKey, ""),
	})
	if err != nil {
		return err
	}

	// Create an S3 client
	svc := s3.New(sess)

	// Upload the JSON data to the specified S3 bucket
	_, err = svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s3config.Bucket),
		Key:    aws.String(fmt.Sprintf("%v.json", time.Now().Unix())), // Use a unique key for each upload
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return err
	}

	return nil
}
