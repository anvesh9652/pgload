package streams

import (
	"bytes"
	"encoding/csv"
	"io"
	"sync"
)

type AsyncStreams struct {
	reader io.Reader
	in     chan []byte
	items  int
	wg     sync.WaitGroup

	pool sync.Pool

	// Close these channels on the client side
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
	leftOver := make([]byte, 0, buffSize)
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
			leftOver = make([]byte, n-lastIndex-1)
			copy(leftOver, buff[lastIndex+1:n])

			// Do not use this; it's just a copy of buff's underlying array, so leftover will get
			// modified when buff gets updated in read.
			// leftOver = buff[lastIndex+1:n]
			as.in <- merged
		}
		close(as.in)
	}()

	go func() {
		as.Process()
	}()
	return as
}

func (a *AsyncStreams) Process() {
	for range a.items {
		a.wg.Add(1)
		go func() {
			defer a.wg.Done()
			for data := range a.in {
				// buff := a.pool.Get().(*bytes.Buffer)
				// buff.Reset()
				// buff.Write(data)

				// Why is this working but not the pool one?
				buff := bytes.NewBuffer(data)

				csvReader := csv.NewReader(buff)
				for {
					row, err := csvReader.Read()
					if err != nil {
						if err == io.EOF {
							break
						}
						// After encountering the first error, we are closing the channel, but other in-progress
						// goroutines that encounter errors are also sending to this closed channel,
						// which causes panics. Investigate this.
						a.Err <- err
						return
					}
					a.Out <- row
					// a.pool.Put(buff)
				}
			}
		}()
	}
	a.wg.Wait()
	close(a.Out)
}
