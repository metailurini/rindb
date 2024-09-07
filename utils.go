package rindb

func isEmpty[T comparable](v T) bool {
	var initValue T
	return v == initValue
}
