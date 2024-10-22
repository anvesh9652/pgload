package streams

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/anvesh9652/side-projects/shared/csvutils"
)

func TestAsyncReader(t *testing.T) {
	// f, _ := os.Open("/Users/agali/Desktop/Work/Product/bills-data/mca-data-1105/2023-12-actual-usage-details-part-0.csv")
	// error one
	f, _ := os.Open("/Users/agali/Desktop/Work/Product/bills-data/mca-data-1105/2023-12-actual-usage-details-part-0.csv")

	tar, _ := os.Create("./test-rows.csv")

	t.Run("AsyncStreams test", func(t *testing.T) {
		var err error
		as := StarChunksStreaming(f)
		cr := csv.NewWriter(tar)

		for data := range as.Out {
			if len(as.Err) > 0 {
				fmt.Println("err: ", <-as.Err)
				t.FailNow()
			}
			err = cr.Write(data)
			if err != nil {
				fmt.Println("err:", err)
				t.FailNow()
			}
		}
		cr.Flush()
		close(as.Err)
	})
	t.Run("test", func(t *testing.T) {
		var err error
		h, r, err := csvutils.GetCSVHeaders(f)
		if err != nil {
			t.FailNow()
		}
		fmt.Println(h)

		// f, _ := os.Open("/Users/agali/Desktop/Work/Product/bills-data/mca-data-1105/fake.csv")
		// tar, _ := os.Create("./test-rows.csv")
		// ct := csv.NewWriter(tar)
		// as := StarChunksStreaming(f)
		err = MethodTest(r)
		if err != nil {
			t.FailNow()
		}
		// for out := range as.Out {
		// 	if len(as.Err) > 0 {
		// 		fmt.Println(<-as.Err)
		// 		t.FailNow()
		// 	}
		// 	fmt.Println(len(out))
		// 	ct.Write(out)
		// 	ct.Flush()
		// 	// fmt.Println(out)
		// }
	})

	t.Run("generate fake data", func(t *testing.T) {
		f, _ := os.Open("/Users/agali/Desktop/Work/Product/bills-data/mca-data-1105/2023-12-amortized-usage-details-part-0.csv")
		tr, _ := os.Create("/Users/agali/Desktop/Work/Product/bills-data/mca-data-1105/2023-12-amortized-usage-details-part-0-fake.csv")

		cr := csv.NewReader(f)
		cw := csv.NewWriter(tr)

		var h bool

		for {
			rec, err := cr.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
				t.FailNow()
			}
			if !h {
				cw.Write(rec)
				h = true
				continue
			}
			for range 100 {
				cw.Write(rec)
			}
		}
		cw.Flush()
	})
}
