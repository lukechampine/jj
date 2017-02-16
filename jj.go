// Package jj implements a JSON transaction journal. It enables efficient ACID
// transactions on JSON objects.
//
// Each Journal represents a single JSON object. The object is serialized as
// an "initial object" followed by a series of update sets, one per line. Each
// update specifies a field and a modification. See the Update type for a full
// specification.
//
// During operation, the object is first loaded by reading the file and
// applying each update to the initial object. It is subsequently modified by
// appending update sets to the file, one per line. At any time, a
// "checkpoint" may be created, which clears the Journal and starts over with
// a new initial object. This allows for compaction of the Journal file.
//
// In the event of power failure or other serious disruption, the most recent
// update set may be only partially written. Partially written update sets are
// simply ignored when reading the Journal. Individual updates may also be
// ignored if they are malformed, though other updates in the set may be
// applied. See the Update docstring for an explanation of malformed updates.
package jj

import (
	"encoding/json"
	"io"
	"os"
)

// A Journal is a log of updates to a JSON object.
type Journal struct {
	f        *os.File
	filename string
}

// Update applies the updates atomically to j. It syncs the underlying file
// before returning.
func (j *Journal) Update(us []Update) error {
	buf := make([]byte, 0, 1024) // reasonable guess; avoids GC if we're lucky
	buf = append(buf, '[')
	for i, u := range us {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, `{"p":"`...)
		buf = append(buf, u.Path...)
		buf = append(buf, `","v":`...)
		buf = append(buf, *u.Value...)
		buf = append(buf, '}')
	}
	buf = append(buf, ']', '\n')
	if _, err := j.f.Write(buf); err != nil {
		return err
	}
	return j.f.Sync()
}

// Checkpoint refreshes the Journal with a new initial object. It syncs the
// underlying file before returning.
func (j *Journal) Checkpoint(obj interface{}) error {
	// write to a new temp file
	//
	// TODO: a separate file may not be necessary. We could use an update with
	// path "" instead, and then overwrite the beginning of the file and
	// truncate. If the overwrite fails, we still have the full rewrite update
	// left at the end. Just need to be careful not to overflow into the
	// update if the new object is large.
	tmp, err := os.Create(j.filename + "_tmp")
	if err != nil {
		return err
	}
	if err := json.NewEncoder(tmp).Encode(obj); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}

	// atomically replace the old file with the new one
	if err := j.f.Close(); err != nil {
		return err
	}
	if err := os.Rename(j.filename+"_tmp", j.filename); err != nil {
		return err
	}

	j.f = tmp
	return nil
}

// Close closes the underlying file.
func (j *Journal) Close() error {
	return j.f.Close()
}

// OpenJournal opens the supplied Journal and decodes the reconstructed object
// into obj, which must be a pointer.
func OpenJournal(filename string, obj interface{}) (*Journal, error) {
	// open file handle, creating the file if it does not exist
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	// decode the initial object
	var initObj json.RawMessage
	dec := json.NewDecoder(f)
	if err = dec.Decode(&initObj); err != nil {
		return nil, err
	}
	// decode each set of updates
	for {
		var set []Update
		if err = dec.Decode(&set); err == io.EOF {
			break
		} else if _, ok := err.(*json.SyntaxError); ok {
			// skip malformed update sets
			continue
		} else if err != nil {
			return nil, err
		}
		for _, u := range set {
			initObj = u.apply(initObj)
		}
	}
	// decode the final object into obj
	if err = json.Unmarshal(initObj, obj); err != nil {
		return nil, err
	}

	return &Journal{
		f:        f,
		filename: filename,
	}, nil
}

// An Update is a modification of a path in a JSON object. A "path" in this
// context means an object or array element. Syntactically, a path is a set of
// accessors joined by the '.' character. An accessor is either an object key
// or an array index. For example, given this object:
//
//    {
//        "foo": {
//            "bars": [
//                {"baz":3}
//            ]
//        }
//    }
//
// The following path accesses the value "3":
//
//    foo.bars.0.baz
//
// The path is accompanied by a new object. Thus, to increment the value "3"
// in the above object, we would use the following Update:
//
//    {
//        "p": "foo.bars.0.baz",
//        "v": 4
//    }
//
// All permutations of the Update object are legal. However, malformed updates
// are ignored during application. An Update is considered malformed in three
// circumstances:
//
// - Its Path references an element that does not exist at application time.
//   This includes out-of-bounds array indices.
// - Its Path contains invalid characters (e.g. "). See the JSON spec.
// - Value contains invalid JSON or is empty.
//
// Other special cases are handled as follows:
//
// - If Path is "", the entire object is replaced.
// - If an object contains duplicate keys, the first key encountered is used.
//
// Finally, to enable efficient array updates, the length of the array (at
// application time) may be used as a special array index.  When this index is
// the last accessor in Path, Value will be appended to the end of the array.
// If the index is not the last accessor, the Update is considered malformed
// (and thus is ignored).
type Update struct {
	// Path is an arbitrarily-nested JSON element, such as foo.bars.1.baz
	Path string `json:"p"`
	// Value contains the new value of Path.
	// TODO: remove pointer once Go 1.8 is released.
	Value *json.RawMessage `json:"v"`
}

func (u Update) MarshalJSON() ([]byte, error) {
	j := make([]byte, 0, 128) // reasonable guess; avoids GC if we're lucky
	j = append(j, `{"p":"`...)
	j = append(j, u.Path...)
	j = append(j, `","v":`...)
	j = append(j, *u.Value...)
	j = append(j, '}')
	return j, nil
}

// apply applies u to obj, returning the new JSON, which may share underlying
// memory with obj or u.Value. If u is malformed, obj is returned unaltered.
// See the Update docstring for an explanation of malformed Updates. If obj is
// not valid JSON, the result is undefined.
func (u Update) apply(obj json.RawMessage) json.RawMessage {
	if len(*u.Value) == 0 {
		// u is malformed
		return obj
	}
	return rewritePath(obj, u.Path, *u.Value)
}

// NewUpdate constructs an update using the provided path and val. If val
// cannot be marshaled, NewUpdate panics. If val implements the json.Marshaler
// interface, it is called directly. Note that this bypasses validation of the
// produced JSON, which may result in a malformed Update.
func NewUpdate(path string, val interface{}) Update {
	var data []byte
	var err error
	if m, ok := val.(json.Marshaler); ok {
		// bypass validation
		data, err = m.MarshalJSON()
	} else {
		data, err = json.Marshal(val)
	}
	if err != nil {
		panic(err)
	}
	rm := json.RawMessage(data)
	return Update{
		Path:  path,
		Value: &rm,
	}
}
