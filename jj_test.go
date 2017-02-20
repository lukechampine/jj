package jj

import (
	"io/ioutil"
	"os"
	"testing"
)

func tempFile(t interface {
	Fatal(...interface{})
}, name string) (*os.File, func()) {
	f, err := ioutil.TempFile("", name)
	if err != nil {
		t.Fatal(err)
	}
	return f, func() {
		f.Close()
		os.RemoveAll(f.Name())
	}
}

func tempJournal(t interface {
	Fatal(...interface{})
}, obj interface{}, name string) (*Journal, func()) {
	f, err := ioutil.TempFile("", name)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	j, err := OpenJournal(f.Name(), obj)
	if err != nil {
		t.Fatal(err)
	}
	return j, func() {
		j.Close()
		os.RemoveAll(f.Name())
	}
}

func TestJournal(t *testing.T) {
	type bar struct {
		Z int `json:"z"`
	}
	type foo struct {
		X int   `json:"x"`
		Y []bar `json:"y"`
	}

	j, cleanup := tempJournal(t, foo{}, "TestJournal")
	defer cleanup()

	us := []Update{
		NewUpdate("x", 7),
		NewUpdate("y.0", bar{}),
		NewUpdate("y.0.z", 3),
	}
	if err := j.Update(us); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	var f foo
	j2, err := OpenJournal(j.filename, &f)
	if err != nil {
		t.Fatal(err)
	}
	j2.Close()
	if f.X != 7 || len(f.Y) != 1 || f.Y[0].Z != 3 {
		t.Fatal("OpenJournal applied updates incorrectly:", f)
	}
}

func TestJournalMalformed(t *testing.T) {
	f, cleanup := tempFile(t, "TestJournalMalformed")
	defer cleanup()

	// write a partially-malformed log
	f.WriteString(`{"foo": 3}
[{"p": "foo", "v": 4}]
[{"p": "foo", "v": 5}`)
	f.Close()

	// load log into foo
	var foo struct {
		Foo int `json:"foo"`
	}
	j, err := OpenJournal(f.Name(), &foo)
	if err != nil {
		t.Fatal(err)
	}
	j.Close()

	// the last update set should have been discarded
	if foo.Foo != 4 {
		t.Fatal("log was not applied correctly:", foo.Foo)
	}
}

func BenchmarkUpdateJournal(b *testing.B) {
	f, cleanup := tempFile(b, "BenchmarkUpdateJournal")
	defer cleanup()

	j := &Journal{f: f}
	us := []Update{
		NewUpdate("foo.bar", struct{ X, Y int }{3, 4}),
		NewUpdate("foo.bar", nil),
	}

	for i := 0; i < b.N; i++ {
		if err := j.Update(us); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkApply(b *testing.B) {
	u := NewUpdate("foo.bar.baz", "")
	json := []byte(`{"foo": {"bar": {"baz": "quux"}}}`)
	for i := 0; i < b.N; i++ {
		u.apply(json)
	}
}
