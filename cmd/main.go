package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/bestxp/test-one/reader"
)

var fileName string

func init() {
	file := flag.String("database", "", "Path to file with db (json or csv)")
	flag.Parse()
	if file == nil || strings.TrimSpace(*file) == "" {
		log.Fatal("-database not specified, run app with flag --help")
		os.Exit(1)
	}
	fileName = strings.TrimSpace(*file)
}

func main() {

	var (
		ctx       = context.Background()
		topPrice  *reader.Item
		topRating *reader.Item
	)

	r, err := reader.NewReaderFromPath(fileName)
	if err != nil {
		log.Fatal(err)
	}

	if err = r.OnReadItem(ctx, func(_ context.Context, item reader.Item) error {
		if topPrice == nil || topPrice.Price < item.Price {
			topPrice = &item
		}
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	if err = r.OnReadItem(ctx, func(_ context.Context, item reader.Item) error {
		if topRating == nil || topRating.Rating < item.Rating {
			topRating = &item
		}
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	if err := r.Read(ctx); err != nil {
		log.Fatal(err)
	}

	if topPrice != nil {
		fmt.Printf("TOP Price  (Product=%s, Price=%d, Rating=%d) \n", topPrice.Product, topPrice.Price, topPrice.Rating)
	} else {
		fmt.Println("TOP Price not found")
	}
	if topRating != nil {
		fmt.Printf("TOP Rating  (Product=%s, Price=%d, Rating=%d) \n", topRating.Product, topRating.Price, topRating.Rating)
	} else {
		fmt.Println("TOP Rating not found")
	}
}
