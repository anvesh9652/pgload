package jsonloader

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"io"
	"os"
	"sync"
)

type AsyncReader struct {
	cols []string
	r    io.Reader

	ErrCh chan error

	OutCh chan []string
	InCh  chan []byte

	pool sync.Pool
	Trw  io.Writer
}

func NewAsyncReader(r io.Reader, cw *csv.Writer, cols []string) *AsyncReader {
	w := &AsyncReader{
		cols:  cols,
		r:     r,
		ErrCh: make(chan error, 10),
		InCh:  make(chan []byte),
		OutCh: make(chan []string),
		pool: sync.Pool{
			New: func() any {
				buff := bytes.NewBuffer(nil)
				return buff
			},
		},
	}

	tar := "test.json"
	f, _ := os.Create(tar)
	w.Trw = f

	size := 1 * 1024 * 1024 // 30 MB
	buff := make([]byte, size)
	var leftOver []byte

	go func() {
		for {
			n, err := r.Read(buff)
			if err != nil {
				if err == io.EOF {
					break
				}
				w.ErrCh <- err
				break
			}

			lastNewLineIdx := bytes.LastIndex(buff[:n], []byte("\n"))
			if lastNewLineIdx == -1 {
				leftOver = append(leftOver, buff[:n]...)
				continue
			}
			merged := append(leftOver, buff[:lastNewLineIdx]...)
			leftOver = make([]byte, n-lastNewLineIdx-1)
			copy(leftOver, buff[lastNewLineIdx+1:n])
			// fmt.Println(string(merged))
			w.InCh <- merged
		}
		close(w.InCh)
	}()
	return w
}

// just go to know that writer are not thread safe in go
func (a *AsyncReader) parseRows(cw *csv.Writer) {
	workers := 4
	wg := new(sync.WaitGroup)
	mu := new(sync.Mutex)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for data := range a.InCh {

				// data = append(data, '\n')
				// buff := a.pool.Get().(*bytes.Buffer)
				// buff.Reset()
				buff := bytes.NewBuffer(data)
				if _, err := buff.Write(data); err != nil {
					a.ErrCh <- err
					return
				}
				dec := json.NewDecoder(buff)
				mu.Lock()
				if err := writeAsCSV(cw, dec, a.cols); err != nil {
					mu.Unlock()
					a.ErrCh <- err
					return
				}
				mu.Unlock()

				// io.Copy(a.Trw, buff)
				a.pool.Put(buff)
			}
		}()
	}
	wg.Wait()
}
