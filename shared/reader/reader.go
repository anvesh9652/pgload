package reader

import (
	"compress/gzip"
	"errors"
	"io"
	"os"

	"github.com/anvesh9652/side-projects/shared"
)

type FileGzipReader struct {
	actualReader *os.File
	gzReader     *gzip.Reader
}

// return a reader which internally handles both compressed and uncompressed files
func NewFileGzipReader(file string) (io.ReadCloser, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	fzr := &FileGzipReader{actualReader: f}
	if shared.IsGZIPFile(file) {
		gr, err := gzip.NewReader(f)
		if err != nil {
			return nil, err
		}
		fzr.gzReader = gr
	}
	return fzr, nil
}

func (r *FileGzipReader) Read(p []byte) (int, error) {
	if r.gzReader != nil {
		return r.gzReader.Read(p)
	}
	return r.actualReader.Read(p)
}

func (r *FileGzipReader) Close() error {
	var err error
	if r.gzReader != nil {
		err = r.gzReader.Close()
	}
	return errors.Join(err, r.actualReader.Close())
}
