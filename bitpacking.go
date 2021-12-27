package goparquet

import (
	"unsafe"
)

type (
	unpack8Func[T intType] func([]byte) [8]T
	pack8Func[T intType]   func([8]T) []byte
)

func packer[T intType](size int) pack8Func[T] {
	return func(data [8]T) []byte {
		var (
			left = size
			indx int
			rev  bool
		)

		result := []byte{}
		for i := 0; i < size; i++ {
			fieldValue := byte(0)

			for right := 0; right < 8; {
				if left == 0 {
					indx++
					left = size
					rev = false
				}
				fieldValue |= getBits(data, indx, size, left, right, rev)
				if left >= 8-right {
					left -= (8 - right)
					right = 8
					rev = true
				} else {
					right += left
					left = 0
				}
			}

			result = append(result, fieldValue)
		}
		return result
	}
}

func getBits[T intType](data [8]T, idx int, size, left, pos int, rev bool) byte {
	if rev {
		return byte(data[idx] >> T(size-left+pos))
	}
	return byte(data[idx] << T(size-left+pos))
}

func unpacker[T intType](bw int) unpack8Func[T] {
	var x T

	maxWidth := int(unsafe.Sizeof(x) * 8)

	return func(data []byte) (a [8]T) {
		startBit := 7
		var value T
		for i := 0; i < 8; i++ {
			value, startBit = getValue[T](data, maxWidth, bw, i, startBit)
			a[i] = value
		}
		return
	}
}

func getValue[T intType](data []byte, maxWidth, bw, i, startBit int) (result T, newStartBit int) {
	byteShift := 0
	firstCurByteBit := startBit - startBit%8
	for bw != 0 {
		curByte := startBit / 8
		bitsInCurByte := bw
		if bitsLeft := startBit - firstCurByteBit + 1; bitsInCurByte > bitsLeft {
			bitsInCurByte = bitsLeft
		}
		shiftSize := 7 - startBit%8
		mask := 1<<uint(bitsInCurByte) - 1

		value := T((data[curByte]>>byte(shiftSize))&byte(mask)) << T(byteShift)

		result |= value

		bw -= bitsInCurByte
		startBit -= bitsInCurByte
		if startBit < firstCurByteBit {
			startBit = firstCurByteBit + 15
			firstCurByteBit += 8
		}
		byteShift += bitsInCurByte
	}
	return result, startBit
}
