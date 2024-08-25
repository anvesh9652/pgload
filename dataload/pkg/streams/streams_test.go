package streams

import (
	"os"
	"testing"
)

func TestAsyncReader(t *testing.T) {
	t.Run("test", func(t *testing.T) {
		// f, _ := os.Open("/Users/agali/Desktop/Work/Product/bills-data/mca-data-1105/2023-12-actual-usage-details-part-0.csv")

		f, _ := os.Open("/Users/agali/Desktop/Work/Product/bills-data/mca-data-1105/fake.csv")
		// tar, _ := os.Create("./test-rows.csv")
		// ct := csv.NewWriter(tar)
		// as := StarChunksStreaming(f)
		err := MethodTest(f)
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
}
