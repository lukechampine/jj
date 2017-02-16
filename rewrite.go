package jj

import (
	"bytes"
	"strconv"
	"strings"
)

// rewritePath replaces the value at path in json with val. The returned slice
// may share underlying memory with json. If path is malformed, the original
// json is returned.
func rewritePath(json []byte, path string, val []byte) []byte {
	if path == "" {
		return val
	}

	var lastAcc string
	var i int
	for j := 0; lastAcc == ""; j++ {
		// determine next accessor by seeking to .
		dotIndex := strings.IndexByte(path[j:], '.')
		if dotIndex == -1 {
			// not found; this is the last accessor
			dotIndex = len(path[j:])
			lastAcc = path[j:]
		}
		acc := path[j : j+dotIndex]
		j += dotIndex

		// seek to accessor
		accIndex := locateAccessor(json[i:], acc)
		if accIndex == -1 {
			// not found; return unmodified
			return json
		} else if json[accIndex] == ']' && lastAcc == "" {
			// only the last accessor may use the "append" index
			return json
		}
		i += accIndex
	}

	// replace old value
	newJSON := make([]byte, 0, len(json)+len(val)) // reasonable guess
	newJSON = append(newJSON, json[:i]...)
	if json[i] == ']' {
		// we are appending. If the array is not empty, insert an extra ,
		if lastAcc != "0" {
			newJSON = append(newJSON, ',')
		}
	}
	newJSON = append(newJSON, val...)
	newJSON = append(newJSON, consumeValue(json[i:])...)

	return newJSON
}

// locateAccessor returns the offset of acc in json.
func locateAccessor(json []byte, acc string) int {
	origLen := len(json)
	json = consumeWhitespace(json)
	if len(json) == 0 || len(json) < len(acc) {
		return -1
	}

	// acc must refer to either an object key or an array index. So if we
	// don't see a { or [, the path is invalid.
	switch json[0] {
	default:
		return -1

	case '{': // object
		json = consumeSeparator(json) // consume {
		// iterate through keys, searching for acc
		for json[0] != '}' {
			var key []byte
			key, json = parseString(json)
			json = consumeWhitespace(json)
			json = consumeSeparator(json) // consume :
			if bytes.Equal(key, []byte(acc)) {
				// acc found
				return origLen - len(json)
			}
			json = consumeValue(json)
			json = consumeWhitespace(json)
			if json[0] == ',' {
				json = consumeSeparator(json) // consume ,
			}
		}
		// acc not found
		return -1

	case '[': // array
		// is accessor possibly an array index?
		n, err := strconv.Atoi(acc)
		if err != nil || n < 0 {
			// invalid index
			return -1
		}
		json = consumeSeparator(json) // consume [
		// consume n keys, stopping early if we hit the end of the array
		var arrayLen int
		for n > arrayLen && json[0] != ']' {
			json = consumeValue(json)
			arrayLen++
			json = consumeWhitespace(json)
			if json[0] == ',' {
				json = consumeSeparator(json) // consume ,
			}
		}
		if n > arrayLen {
			// Note that n == arrayLen is allowed. In this case, an append
			// operation is desired; we return the offset of the closing ].
			return -1
		}
		return origLen - len(json)
	}
}

func parseString(json []byte) ([]byte, []byte) {
	after := consumeString(json)
	strLen := len(json) - len(after) - 2
	return json[1 : 1+strLen], after
}

func consumeWhitespace(json []byte) []byte {
	for i := range json {
		if c := json[i]; c > ' ' || (c != ' ' && c != '\t' && c != '\n' && c != '\r') {
			return json[i:]
		}
	}
	return json[len(json):]
}

func consumeSeparator(json []byte) []byte {
	json = json[1:] // consume one of [ { } ] : ,
	return consumeWhitespace(json)
}

func consumeValue(json []byte) []byte {
	// determine value type
	switch json[0] {
	case '{': // object
		return consumeObject(json)
	case '[': // array
		return consumeArray(json)
	case '"': // string
		return consumeString(json)
	case 't', 'n': // true or null
		return json[4:]
	case 'f': // false
		return json[5:]
	default: // number
		return consumeNumber(json)
	}
}

func consumeObject(json []byte) []byte {
	json = json[1:] // consume {
	// seek to next {, }, or ". Each time we encounter a {, increment n. Each
	// time encounter a }, decrement n. Exit when n == 0. If we encounter ",
	// consume the string.
	n := 1
	for n > 0 {
		json = json[bytes.IndexAny(json, `{}"`):]
		switch json[0] {
		case '{':
			n++
			json = json[1:] // consume {
		case '}':
			n--
			json = json[1:] // consume }
		case '"':
			json = consumeString(json)
		}
	}
	return json
}

func consumeArray(json []byte) []byte {
	json = json[1:] // consume [
	// seek to next [, ], or ". Each time we encounter a [, increment n. Each
	// time encounter a ], decrement n. Exit when n == 0. If we encounter ",
	// consume the string.
	n := 1
	for n > 0 {
		json = json[bytes.IndexAny(json, `[]"`):]
		switch json[0] {
		case '[':
			n++
			json = json[1:] // consume [
		case ']':
			n--
			json = json[1:] // consume ]
		case '"':
			json = consumeString(json)
		}
	}
	return json
}

func consumeString(json []byte) []byte {
	i := 1 // consume "
	// seek forward until we find a " without a preceeding \
	i += bytes.IndexByte(json[i:], '"')
	for json[i-1] == '\\' {
		i++
		i += bytes.IndexByte(json[i:], '"')
	}
	return json[i+1:] // consume "
}

func consumeNumber(json []byte) []byte {
	if json[0] == '-' {
		json = json[1:]
	}
	// leading digits
	for '0' <= json[0] && json[0] <= '9' {
		json = json[1:]
		if len(json) == 0 {
			return json
		}
	}
	// decimal digits
	if json[0] == '.' {
		json = json[1:]
		for '0' <= json[0] && json[0] <= '9' {
			json = json[1:]
			if len(json) == 0 {
				return json
			}
		}
	}
	// exponent
	if json[0] == 'e' || json[0] == 'E' {
		json = json[1:]
		if json[0] == '+' || json[0] == '-' {
			json = json[1:]
		}
		for '0' <= json[0] && json[0] <= '9' {
			json = json[1:]
			if len(json) == 0 {
				return json
			}
		}
	}
	return json
}
