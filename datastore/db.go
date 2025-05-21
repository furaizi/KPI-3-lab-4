package datastore

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

const (
	activeFileName     = "current-data"         // файл, у який зараз відбувається append
	closedPattern      = "segment-*.seg"        // закриті сегменти після ротації
	envMaxSegmentBytes = "DS_MAX_SEGMENT_BYTES" // для тестів можна перевизначити
	defaultMaxSegBytes = 10 * 1024 * 1024       // 10 MB у production
	getWorkerCount     = 10
)

// ------------------------------------------------------------
// Типи / помилки
// ------------------------------------------------------------

// entry описаний у вихідному репозиторії (Encode / DecodeFromReader).
// Тут припускаємо, що він лишився без змін.

type segPointer struct {
	file   string // абсолютний шлях до файла-сегмента
	offset int64  // позиція всередині цього файла
}

type hashIndex map[string]segPointer

var ErrNotFound = fmt.Errorf("record does not exist")

// ------------------------------------------------------------
// Пакет‑приватні допоміжні структури
// ------------------------------------------------------------

type writeRequest struct {
	key   string
	value string
	done  chan error
}

type getRequest struct {
	key      string
	response chan getResult
}

type getResult struct {
	value string
	err   error
}

// ------------------------------------------------------------
// Db
// ------------------------------------------------------------

type Db struct {
	dir string

	// active segment
	out       *os.File // відкритий для append
	outOffset int64

	maxSegBytes int64

	// in‑memory index
	index   hashIndex
	indexMu sync.RWMutex

	// async writer
	writeCh chan writeRequest
	wg      sync.WaitGroup

	// async readers pool
	getCh chan getRequest
	getWg sync.WaitGroup
}

// ------------------------------------------------------------
// API
// ------------------------------------------------------------

func Open(dir string) (*Db, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	maxSize := defaultMaxSegBytes
	if s := os.Getenv(envMaxSegmentBytes); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 {
			maxSize = v
		}
	}

	// Відкриваємо (або створюємо) активний файл
	activePath := filepath.Join(dir, activeFileName)
	f, err := os.OpenFile(activePath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}

	db := &Db{
		dir:         dir,
		out:         f,
		index:       make(hashIndex),
		writeCh:     make(chan writeRequest, 128),
		getCh:       make(chan getRequest, 128),
		maxSegBytes: int64(maxSize),
	}

	// Відновлюємо індекс з усіх сегментів
	if err := db.recoverAll(); err != nil {
		f.Close()
		return nil, err
	}

	// Запускаємо бекґраунд‑письменника
	db.wg.Add(1)
	go db.backgroundWriter()

	// Пул читачів
	for i := 0; i < getWorkerCount; i++ {
		db.getWg.Add(1)
		go db.backgroundReader()
	}

	return db, nil
}

func (db *Db) Put(key, value string) error {
	done := make(chan error, 1)
	db.writeCh <- writeRequest{key: key, value: value, done: done}
	return <-done
}

func (db *Db) Get(key string) (string, error) {
	resp := make(chan getResult, 1)
	db.getCh <- getRequest{key: key, response: resp}
	r := <-resp
	return r.value, r.err
}

// Size повертає розмір активного файла‑сегмента.
func (db *Db) Size() (int64, error) {
	info, err := db.out.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (db *Db) Close() error {
	close(db.writeCh)
	db.wg.Wait()

	close(db.getCh)
	db.getWg.Wait()

	return db.out.Close()
}

// Compact запускає компакцію закритих сегментів (active не чіпає).
func (db *Db) Compact() error {
	// 1. Тимчасово зупиняємо прийом записів: закриваємо старий канал і чекаємо.
	close(db.writeCh)
	db.wg.Wait()

	// 2. Формуємо список закритих сегментів.
	segs, err := filepath.Glob(filepath.Join(db.dir, closedPattern))
	if err != nil {
		return err
	}
	if len(segs) == 0 {
		// Немає що компактувати — просто перезапускаємо writer і повертаємося.
		db.restartWriter()
		return nil
	}

	// 3. Створюємо тимчасовий файл для compact‑output.
	tmpName := filepath.Join(db.dir, fmt.Sprintf("compact-%d.seg", time.Now().UnixNano()))
	tmp, err := os.OpenFile(tmpName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		db.restartWriter()
		return err
	}

	// 4. Пишемо тільки найновіші записи закритих сегментів.
	db.indexMu.RLock()
	type kv struct {
		key string
		ptr segPointer
	}
	latest := make([]kv, 0, len(db.index))
	for k, p := range db.index {
		if p.file == db.out.Name() { // пропускаємо актуальний сегмент
			continue
		}
		latest = append(latest, kv{k, p})
	}
	db.indexMu.RUnlock()

	// Будуємо нове положення ключів у tmp
	newPointers := make(map[string]segPointer, len(latest))
	var offset int64
	for _, item := range latest {
		// Зчитуємо спочатку entry з старого файлу
		rec, err := db.readEntry(item.ptr)
		if err != nil {
			tmp.Close()
			db.restartWriter()
			return err
		}
		n, err := tmp.Write(rec.Encode())
		if err != nil {
			tmp.Close()
			db.restartWriter()
			return err
		}
		newPointers[item.key] = segPointer{file: tmpName, offset: offset}
		offset += int64(n)
	}
	tmp.Sync()
	tmp.Close()

	// 5. Ренеймуємо compact‑file, щоб він став «новим» сегментом.
	mergedName := filepath.Join(db.dir, fmt.Sprintf("segment-%d-merged.seg", time.Now().UnixNano()))
	if err := os.Rename(tmpName, mergedName); err != nil {
		db.restartWriter()
		return err
	}

	// 6. Оновлюємо індекс і видаляємо старі закриті сегменти.
	db.indexMu.Lock()
	for k, p := range newPointers {
		db.index[k] = p
	}
	db.indexMu.Unlock()

	for _, old := range segs {
		_ = os.Remove(old) // помилки нехай не зупиняють — гірше не стане
	}

	// 7. Перезапускаємо writer.
	db.restartWriter()
	return nil
}

// ------------------------------------------------------------
// Внутрішня реалізація
// ------------------------------------------------------------

func (db *Db) restartWriter() {
	db.writeCh = make(chan writeRequest, 128)
	db.wg.Add(1)
	go db.backgroundWriter()
}

// backgroundWriter — єдиний серіалізований шлях запису в активний сегмент.
func (db *Db) backgroundWriter() {
	defer db.wg.Done()
	for req := range db.writeCh {
		// 1. Кодуємо entry.
		e := entry{key: req.key, value: req.value}
		data := e.Encode()

		// 2. Записуємо.
		pos := db.outOffset
		n, err := db.out.Write(data)
		if err == nil {
			db.indexMu.Lock()
			db.index[req.key] = segPointer{file: db.out.Name(), offset: pos}
			db.outOffset += int64(n)
			db.indexMu.Unlock()
		}
		req.done <- err

		// 3. Перевіряємо, чи треба робити ротацію.
		if db.outOffset >= db.maxSegBytes {
			_ = db.rotateSegment()
		}
	}
}

// rotateSegment закриває поточний файл, перейменовує його в segment‑*.seg і відкриває новий active.
func (db *Db) rotateSegment() error {
	// Закриваємо поточний файл, щоб FS точно фіксувала розмір.
	if err := db.out.Sync(); err != nil {
		return err
	}
	if err := db.out.Close(); err != nil {
		return err
	}

	// Перенеймовуємо «current-data» у «segment-<ts>.seg».
	newName := filepath.Join(db.dir, fmt.Sprintf("segment-%d.seg", time.Now().UnixNano()))
	if err := os.Rename(filepath.Join(db.dir, activeFileName), newName); err != nil {
		return err
	}

	// Відкриваємо новий current-data
	f, err := os.OpenFile(filepath.Join(db.dir, activeFileName), os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	db.out = f
	db.outOffset = 0
	return nil
}

// backgroundReader — пул конкурентних читачів.
func (db *Db) backgroundReader() {
	defer db.getWg.Done()
	for req := range db.getCh {
		db.indexMu.RLock()
		ptr, ok := db.index[req.key]
		db.indexMu.RUnlock()
		if !ok {
			req.response <- getResult{"", ErrNotFound}
			continue
		}

		rec, err := db.readEntry(ptr)
		if err != nil {
			req.response <- getResult{"", err}
			continue
		}
		req.response <- getResult{rec.value, nil}
	}
}

// readEntry читає entry за вказаним pointer.
func (db *Db) readEntry(ptr segPointer) (entry, error) {
	var rec entry
	file, err := os.Open(ptr.file)
	if err != nil {
		return rec, err
	}
	defer file.Close()

	if _, err = file.Seek(ptr.offset, io.SeekStart); err != nil {
		return rec, err
	}
	_, err = rec.DecodeFromReader(bufio.NewReader(file))
	return rec, err
}

// recoverAll будує індекс із усіх файлів‑сегментів + active.
func (db *Db) recoverAll() error {
	// Знаходимо всі сегменти.
	patterns := []string{filepath.Join(db.dir, closedPattern), filepath.Join(db.dir, activeFileName)}
	var files []string
	for _, p := range patterns {
		matches, _ := filepath.Glob(p)
		files = append(files, matches...)
	}

	// Сортуємо за часом модифікації (від старого до нового).
	sort.Slice(files, func(i, j int) bool {
		fi, _ := os.Stat(files[i])
		fj, _ := os.Stat(files[j])
		return fi.ModTime().Before(fj.ModTime())
	})

	for _, path := range files {
		if err := db.recoverFile(path); err != nil {
			return err
		}
	}
	return nil
}

// recoverFile сканує окремий файл і оновлює індекс.
func (db *Db) recoverFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	var offset int64
	for {
		var e entry
		n, err := e.DecodeFromReader(r)
		if errors.Is(err, io.EOF) {
			if n != 0 {
				return fmt.Errorf("corrupted segment %s", path)
			}
			break
		}
		if err != nil {
			return err
		}
		db.index[e.key] = segPointer{file: path, offset: offset}
		offset += int64(n)
	}

	if filepath.Base(path) == activeFileName {
		// Запам'ятовуємо поточний розмір active файла
		db.outOffset = offset
	}
	return nil
}
