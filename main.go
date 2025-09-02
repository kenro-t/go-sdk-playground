package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type Presigner struct {
	PresignClient *s3.PresignClient
}

type BucketBasics struct {
	S3Client *s3.Client
}

func main() {
	ctx := context.Background()

	sdkConfig, err := config.LoadDefaultConfig(ctx, config.WithRegion("ap-northeast-1"))
	if err != nil {
		fmt.Println("Couldn't load default configuration. Have you set up your AWS account?")
		fmt.Println(err)
		return
	}

	basics := BucketBasics{
		S3Client: s3.NewFromConfig(sdkConfig),
	}
	bucketName := "my-test-bucket-221343"
	key := "my-object-key"
	if err := basics.CreateBucket(ctx, bucketName, "ap-northeast-1"); err != nil {
		fmt.Println("Couldn't create bucket")
		fmt.Println(err)
		return
	}

	presigner := Presigner{
		PresignClient: s3.NewPresignClient(s3.NewFromConfig(sdkConfig)),
	}

	presignedHttpRequest, err := presigner.PresignClient.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucketName,
		Key:    &key,
	})
	if err != nil {
		fmt.Println("Couldn't generate a presigned URL for PutObject")
		fmt.Println(err)
		return
	}

	fmt.Println("The presigned URL is:", presignedHttpRequest.URL)
}

func (basics BucketBasics) CreateBucket(ctx context.Context, name string, region string) error {
	_, err := basics.S3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(name),
		CreateBucketConfiguration: &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(region),
		},
	})
	if err != nil {
		var owned *types.BucketAlreadyOwnedByYou
		var exists *types.BucketAlreadyExists
		if errors.As(err, &owned) {
			log.Printf("You already own bucket %s.\n", name)
			err = owned
		} else if errors.As(err, &exists) {
			log.Printf("Bucket %s already exists.\n", name)
			err = exists
		}
	} else {
		err = s3.NewBucketExistsWaiter(basics.S3Client).Wait(
			ctx, &s3.HeadBucketInput{Bucket: aws.String(name)}, time.Minute)
		if err != nil {
			log.Printf("Failed attempt to wait for bucket %s to exist.\n", name)
		}
	}
	return err
}
