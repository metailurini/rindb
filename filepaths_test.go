package rindb

import "testing"

func TestFilePathHelpers(t *testing.T) {
	if walPath(1) != "000001"+walExt {
		t.Fatalf("walPath unexpected")
	}
	if sstPath(2) != "000002"+sstExt {
		t.Fatalf("sstPath unexpected")
	}
	if manifestPath(3) != "MANIFEST-000003" {
		t.Fatalf("manifestPath unexpected")
	}
	n, err := fileNum("/path/000123" + sstExt)
	if err != nil || n != 123 {
		t.Fatalf("fileNum failed: %v %d", err, n)
	}
	mn, err := manifestNum("MANIFEST-000007")
	if err != nil || mn != 7 {
		t.Fatalf("manifestNum failed: %v %d", err, mn)
	}
}
