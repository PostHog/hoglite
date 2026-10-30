// Command earlyinit runs StartMemoryCaps after another package has
// initialized SQLite (TestStartMemoryCapsFailsClosed).
package main

import (
	"fmt"
	"os"
	"runtime/debug"
	"strconv"

	"hoglite/internal/hoglite"
	_ "hoglite/internal/hoglite/testdata/earlyinit/early"
)

func main() {
	hard, _ := strconv.ParseInt(os.Args[1], 10, 64)
	_, err := hoglite.StartMemoryCaps(hoglite.MemoryConfig{SQLiteHard: hard})
	h, s := hoglite.SQLiteHeapLimits()
	fmt.Printf("memstatus=%v\nstart=%v\nhard=%d soft=%d go=%d\n", hoglite.SQLiteMemStatus(), err, h, s, debug.SetMemoryLimit(-1))
}
