package test

import "strconv"

// GenPartName generates the partName
// part name, format: uuid.partNum
func GenPartName(uuid string, partNum int) string {
	return uuid + DefaultSeparator + strconv.Itoa(partNum)
}
