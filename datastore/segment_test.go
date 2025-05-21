package datastore

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	testMaxSegmentBytes = 256
	testValue           = "xxxxxxxxxxxxxxxxxxxx" // 20 байт
)

// setMaxSegmentSize робить тести незалежними від продакшн-налаштувань
func setMaxSegmentSize(t *testing.T) {
	t.Helper()
	if err := os.Setenv("DS_MAX_SEGMENT_BYTES", strconv.Itoa(testMaxSegmentBytes)); err != nil {
		t.Fatalf("cannot set DS_MAX_SEGMENT_BYTES: %v", err)
	}
	t.Cleanup(func() { _ = os.Unsetenv("DS_MAX_SEGMENT_BYTES") })
}

// helper, щоб почекати, коли бекґраунд-writer/flush точно допише дані на диск
func drainWrites() { time.Sleep(50 * time.Millisecond) }

// TestSegmentRotation перевіряє, що після переповнення активного файла
// з’являється принаймні один «закритий» сегмент, а активний <= maxBytes
func TestSegmentRotation(t *testing.T) {
	setMaxSegmentSize(t)

	tmp := t.TempDir()
	db, err := Open(tmp)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	// "Надути" сегмент. 15 Put-ів достатньо: 15*(len(key)+len(value)+service bytes) > 256
	for i := 0; i < 15; i++ {
		if err := db.Put("key-"+strconv.Itoa(i), testValue); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	drainWrites()

	// активний файл не має перевищувати поріг
	if size, _ := db.Size(); size > testMaxSegmentBytes {
		t.Fatalf("active segment size = %d, want ≤ %d", size, testMaxSegmentBytes)
	}

	// закриті сегменти фактично існують
	files, _ := filepath.Glob(filepath.Join(tmp, "*segment*"))
	if len(files) == 0 {
		// fallback - якщо ви обрали іншу схему імен
		files, _ = filepath.Glob(filepath.Join(tmp, "*data*"))
	}
	if len(files) == 0 {
		t.Fatalf("rotation expected at least one closed segment file, got none")
	}
}

// TestCompaction перевіряє, що після merge-операції:
//
//	значення ключів залишаються актуальними;
//	сумарний розмір усіх файлів меншає (старі версії видалено)
func TestCompaction(t *testing.T) {
	setMaxSegmentSize(t)

	tmp := t.TempDir()
	db, err := Open(tmp)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	// Записуємо декілька ключів і одразу ж їх перезаписуємо новими значеннями
	const n = 10
	for i := 0; i < n; i++ {
		k := "dup-" + strconv.Itoa(i)
		if err := db.Put(k, "old-"+k); err != nil {
			t.Fatalf("put old: %v", err)
		}
	}
	for i := 0; i < n; i++ {
		k, v := "dup-"+strconv.Itoa(i), "new-"+strconv.Itoa(i)
		if err := db.Put(k, v); err != nil {
			t.Fatalf("put new: %v", err)
		}
	}
	drainWrites()

	// фіксуємо розмір ДО компакції
	before := directorySize(t, tmp)

	if err := db.Compact(); err != nil { // <- реалізуйте!
		t.Fatalf("compact: %v", err)
	}
	drainWrites()

	after := directorySize(t, tmp)
	if after >= before {
		t.Fatalf("expected directory to shrink after compaction, was %d, now %d", before, after)
	}

	// Переконуємося, що актуальні значення збережені
	for i := 0; i < n; i++ {
		exp := "new-" + strconv.Itoa(i)
		got, err := db.Get("dup-" + strconv.Itoa(i))
		if err != nil || got != exp {
			t.Fatalf("get after compact(%d): got %q, err=%v, want %q", i, got, err, exp)
		}
	}
}

// directorySize рахує суму розмірів усіх файлів у директорії
func directorySize(t *testing.T, dir string) (total int64) {
	t.Helper()
	_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && // не цікавлять .tmp-/lock-файли
			!strings.HasPrefix(info.Name(), ".") {
			total += info.Size()
		}
		return nil
	})
	return
}
