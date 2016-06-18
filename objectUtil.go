package test

import "strconv"

// GenPartName generates the partName
func GenPartName(uuid string, partNum int) string {
	return uuid + DefaultSeparator + strconv.Itoa(partNum)
}
