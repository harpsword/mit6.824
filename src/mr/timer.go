package mr

import "time"

type Timer struct {
	time.Timer

	jobType JobType
	jobID uint8
}
