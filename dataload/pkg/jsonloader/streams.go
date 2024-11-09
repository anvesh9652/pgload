package jsonloader

import (
	"bufio"
	"bytes"
	"io"
	"sync"

	"github.com/anvesh9652/side-projects/shared"
	"github.com/buger/jsonparser"
)

const (
	chanSize = 50
	// after multiple tries seems 6 MB was a sweet spot
	buffSize = 6 * 1024 * 1024 // 10 MB (also)

	numWorkers = 5
)

type AsyncReader struct {
	cols []string
	r    io.Reader

	ErrCh chan error

	OutCh chan []string
	InCh  chan []byte

	pool    sync.Pool
	rowPool sync.Pool
}

func NewAsyncReader(r io.Reader, cols []string) *AsyncReader {
	w := &AsyncReader{
		cols:  cols,
		r:     r,
		ErrCh: make(chan error, 10),
		InCh:  make(chan []byte, chanSize),
		OutCh: make(chan []string, chanSize),
		pool: sync.Pool{
			New: func() any {
				buff := bytes.NewBuffer(nil)
				return buff
			},
		},
		rowPool: sync.Pool{
			New: func() any {
				row := make([]string, len(cols))
				return &row
			},
		},
	}

	buff := make([]byte, buffSize)
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

			// lastNewLineIdx := bytes.LastIndex(buff[:n], []byte("\n"))
			lastNewLineIdx := getNewLineLastIndex(buff[:n])
			if lastNewLineIdx == -1 {
				leftOver = append(leftOver, buff[:n]...)
				continue
			}
			merged := append(leftOver, buff[:lastNewLineIdx]...)
			leftOver = make([]byte, n-lastNewLineIdx-1)
			copy(leftOver, buff[lastNewLineIdx+1:n])
			w.InCh <- merged
		}
		close(w.InCh)
	}()
	return w
}

// just got to know that writer are not thread safe in go
func (a *AsyncReader) parseRows() {
	colIdxMap := map[string]int{}
	for i, col := range a.cols {
		colIdxMap[col] = i
	}

	wg := new(sync.WaitGroup)
	defer close(a.OutCh)
	for range numWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for data := range a.InCh {
				buff := a.pool.Get().(*bytes.Buffer)
				if _, err := buff.Write(data); err != nil {
					a.ErrCh <- err
					return
				}

				if err := a.sendToOutput(buff, colIdxMap); err != nil {
					a.ErrCh <- err
					return
				}

				buff.Reset()
				a.pool.Put(buff)
			}
		}()
	}
	wg.Wait()
}

func (a *AsyncReader) sendToOutput(buff *bytes.Buffer, colToIdx map[string]int) error {
	sc := bufio.NewScanner(buff)
	eachRow := a.rowPool.Get().(*[]string)
	defer a.rowPool.Put(eachRow)

	mp := map[string]int{}

	for sc.Scan() {
		// todo: this package was few causing issues, quotes weren't being handled properly
		err := jsonparser.ObjectEach(sc.Bytes(), func(key, value []byte, dataType jsonparser.ValueType, offset int) error {
			(*eachRow)[colToIdx[string(key)]] = string(value)
			mp[dataType.String()] += 1
			return nil
		})
		if err != nil {
			return err
		}
		a.OutCh <- *eachRow
	}
	shared.AsJson(mp, nil)
	return sc.Err()
}

func getNewLineLastIndex(buff []byte) int {
	n := len(buff)
	for i := n - 1; i >= 0; i-- {
		if buff[i] == 10 {
			return i
		}
	}
	return -1
}
