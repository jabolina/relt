package internal

import "sync/atomic"

const (
	// Constant to represent the `active` state on the Flag.
	active = 0x0

	// Constant to represent the `inactive` state on the Flag.
	inactive = 0x1
)

// An atomic boolean implementation, to act specifically as a flag.
type Flag struct {
	flag int32
}

// Verify if the flag still on `active` state.
func (f *Flag) IsActive() bool {
	return atomic.LoadInt32(&f.flag) == active
}

// Verify if the flag is on `inactive` state.
func (f *Flag) IsInactive() bool {
	return atomic.LoadInt32(&f.flag) == inactive
}

// Transition the flag from `active` to `inactive`.
func (f *Flag) Inactivate() bool {
	return atomic.CompareAndSwapInt32(&f.flag, active, inactive)
}
