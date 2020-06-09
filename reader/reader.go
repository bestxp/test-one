package reader

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
)

// Item from database
type (
	Item struct {
		Product string `json:"product"`
		Price   int64  `json:"price"`
		Rating  int64  `json:"rating"`
	}

	// When any item readed
	OnItemReadFn func(context.Context, Item) error

	// Reader fo read any type of file
	Reader interface {
		OnReadItem(context.Context, OnItemReadFn) error
		Read(context.Context) error
	}

	jsonReader struct {
		file      string
		callbacks []OnItemReadFn
	}
	csvReader struct {
		file      string
		callbacks []OnItemReadFn
	}
)

const (
	extJSON = ".json"
	extCSV  = ".csv"
)

var (
	_ Reader = &jsonReader{}
	_ Reader = &csvReader{}

	ErrFileNotExist = errors.New("file not exists")
	ErrUnknownFile  = errors.New("unknown filetype")
)

// NewReaderFromPath detects type of reader and start reads
func NewReaderFromPath(filepath string) (Reader, error) {
	filepath = strings.TrimSpace(filepath)
	if !fileExists(filepath) {
		return nil, ErrFileNotExist
	}
	if strings.HasSuffix(filepath, extJSON) {
		return &jsonReader{file: filepath, callbacks: []OnItemReadFn{}}, nil
	}
	if strings.HasSuffix(filepath, extCSV) {
		return &csvReader{file: filepath, callbacks: []OnItemReadFn{}}, nil
	}

	return nil, ErrUnknownFile
}

// fileExists checks if a file exists and is not a directory before we
// try using it to prevent further errors.
func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

// OnReadItem callback on read every item
func (j *jsonReader) OnReadItem(_ context.Context, fn OnItemReadFn) error {
	j.callbacks = append(j.callbacks, fn)
	return nil
}

// Read read by rows file (stop on first error )
func (j *jsonReader) Read(ctx context.Context) error {
	f, err := os.Open(j.file)
	if err != nil {
		return err
	}
	defer f.Close()
	dec := json.NewDecoder(f)

	_, err = dec.Token()
	if err != nil {
		log.Fatal(err)
	}

	// while the array contains values
	for dec.More() {
		var it Item
		// decode an array value (Message)
		err := dec.Decode(&it)
		if err != nil {
			log.Fatal(err)
		}
		for _, fn := range j.callbacks {
			if err = fn(ctx, it); err != nil {
				return err
			}
		}
	}

	// read closing bracket
	_, err = dec.Token()
	if err != nil {
		log.Fatal(err)
	}

	return nil
}

// OnReadItem callback on read every item
func (c *csvReader) OnReadItem(_ context.Context, fn OnItemReadFn) error {
	c.callbacks = append(c.callbacks, fn)
	return nil
}

// Read read by rows file (stop on first error )
func (c *csvReader) Read(ctx context.Context) error {
	f, err := os.Open(c.file)
	if err != nil {
		return err
	}
	defer f.Close()

	r := csv.NewReader(f)

	for i := 1; ; i++ {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if len(record) != 3 {
			return ErrUnknownFile
		}
		if i == 1 {
			continue
		}
		item := Item{
			Product: record[0],
			Price:   0,
			Rating:  0,
		}
		item.Price, err = strconv.ParseInt(record[1], 10, 64)
		if err != nil {
			return fmt.Errorf("error parce price at row %d, err = %v", i, err)
		}
		item.Rating, err = strconv.ParseInt(record[2], 10, 64)
		if err != nil {
			return fmt.Errorf("error parce rating at row %d, err = %v", i, err)
		}
		for _, fn := range c.callbacks {
			if err = fn(ctx, item); err != nil {
				return err
			}
		}
	}

	return nil
}
