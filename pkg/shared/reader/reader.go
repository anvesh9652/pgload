package reader

import (
	"compress/gzip"
	"errors"
	"io"
	"os"

	"github.com/anvesh9652/pgload/pkg/shared"
)

type FileGzipReader struct {
	actualReader *os.File
	gzReader     *gzip.Reader
}

// Return a reader that internally handles both compressed and uncompressed files.
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

// Close both the GZIP reader and the actual file reader.
func (r *FileGzipReader) Close() error {
	var err error
	if r.gzReader != nil {
		err = r.gzReader.Close()
	}
	return errors.Join(err, r.actualReader.Close())
}
