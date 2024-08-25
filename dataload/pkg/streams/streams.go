package streams

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sync"
)

type AsyncStreams struct {
	reader io.Reader
	in     chan []byte
	items  int
	wg     sync.WaitGroup

	pool sync.Pool

	// close these channels on client side
	Err chan error
	Out chan []string
}

func newAsyncReader(r io.Reader) *AsyncStreams {
	return &AsyncStreams{
		reader: r,
		in:     make(chan []byte),
		items:  3,

		Err: make(chan error, 10),
		Out: make(chan []string),
		pool: sync.Pool{
			New: func() any {
				return bytes.NewBuffer(nil)
			},
		},
	}
}

func StarChunksStreaming(r io.Reader) *AsyncStreams {
	as := newAsyncReader(r)

	buffSize := 10 * 1024
	leftOver := make([]byte, 0, buffSize/4)
	buff := make([]byte, buffSize)

	go func() {
		for {
			n, err := r.Read(buff)
			if err != nil {
				if err == io.EOF {
					if len(leftOver) > 0 {
						as.in <- leftOver
					}
					break
				}
				as.Err <- err
				return
			}
			lastIndex := bytes.LastIndex(buff[:n], []byte("\n"))
			if lastIndex == -1 {
				leftOver = append(leftOver, buff[:n]...)
				continue
			}

			merged := append(leftOver, buff[:lastIndex]...)
			leftOver = buff[lastIndex+1 : n]
			as.in <- merged
		}
		close(as.in)
	}()

	go func() {
		as.Process()
	}()
	return as
}

func MethodTest(r io.Reader) error {
	buffSize := 10 * 1024
	leftOver := make([]byte, 0, buffSize)
	buff := make([]byte, buffSize)

	tar, _ := os.Create("./test-rows.csv")
	var rem []byte
	for {
		n, err := r.Read(buff)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		lastIndex := bytes.LastIndex(buff[:n], []byte("\n"))
		if lastIndex == -1 {
			leftOver = append(leftOver, buff[:n]...)
			continue
		}

		merged := append(leftOver, buff[:lastIndex]...)
		rmcopy := make([]byte, len(rem))
		copy(rmcopy, rem)
		rmcopy = append(rmcopy, buff[:lastIndex]...)
		rem = append(rem, buff[lastIndex+1:n]...)
		// fmt.Println(string(buff[:lastIndex]), "\n remaning", string(rem))

		fmt.Println(string(rmcopy))
		// fmt.Println("left:", string(leftOver), "\ndata:", string(merged))
		leftOver = rem

		fmt.Println("----")

		// tar.Write(buff[:n])
		// fmt.Println(string(merged), "------")
		tar.Write(merged)
		tar.Write([]byte("\n"))
	}
	if len(leftOver) > 0 {
		tar.Write(leftOver)
	}
	return nil
}

func (a *AsyncStreams) Process() {
	for range a.items {
		a.wg.Add(1)
		go func() {
			defer a.wg.Done()
			for data := range a.in {
				buff := a.pool.Get().(*bytes.Buffer)
				buff.Reset()
				buff.Write(data)

				csvReader := csv.NewReader(buff)
				for {
					row, err := csvReader.Read()
					if err != nil {
						if err == io.EOF {
							break
						}
						a.Err <- err
						return
					}
					fmt.Println("row: ", len(row))
					a.Out <- row
					a.pool.Put(buff)
				}
			}
		}()
	}
	a.wg.Wait()
	close(a.Out)
}
