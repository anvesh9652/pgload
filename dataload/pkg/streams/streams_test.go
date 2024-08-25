package streams

import (
	"fmt"
	"os"
	"testing"
)

func TestAsyncReader(t *testing.T) {
	t.Run("test", func(t *testing.T) {
		f, _ := os.Open("/Users/agali/Desktop/Work/Product/bills-data/mca-data-1105/2023-12-actual-usage-details-part-0.csv")
		as := StarChunksStreaming(f)
		for out := range as.Out {
			if len(as.Err) > 0 {
				fmt.Println(<-as.Err)
				t.FailNow()
			}
			fmt.Println(out)
		}
	})
}
