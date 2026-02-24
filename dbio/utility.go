package dbio

// ZeroFill left-pads a string with zeros to the given size.
func ZeroFill(a string, size int) string {
	num := len(a)
	if num >= size {
		return a
	}
	return "0000000"[:size-num] + a
}
