package main

import (
	"fmt"
	"time"

	"main/lib"
)

func main() {
	start := time.Now()

	lib.ImportEvents()

	end := time.Now()
	fmt.Println("Runtime:", formatDuration(start, end))
}

func formatDuration(start time.Time, end time.Time) string {
	duration := end.Sub(start)

	hours := int(duration.Hours())
	minutes := int(duration.Minutes()) % 60
	seconds := int(duration.Seconds()) % 60
	millis := duration.Milliseconds() % 1000
	return fmt.Sprintf("%02d:%02d:%02d.%03d", hours, minutes, seconds, millis)
}
