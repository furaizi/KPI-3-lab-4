package datastore

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const outFileName = "current-data"
const getWorkerCount = 10


type getRequest struct {
	key      string
	response chan getResult
}

type getResult struct {
	value string
	err   error
}

var ErrNotFound = fmt.Errorf("record does not exist")

type hashIndex map[string]int64

type writeRequest struct {
	key   string
	value string
	done  chan error
}

type Db struct {
	out       *os.File
	outOffset int64

	index   hashIndex
	indexMu sync.RWMutex

	writeCh chan writeRequest
	wg      sync.WaitGroup

	getCh  chan getRequest
	getWg  sync.WaitGroup
}

func Open(dir string) (*Db, error) {
	outputPath := filepath.Join(dir, outFileName)
	f, err := os.OpenFile(outputPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}

	db := &Db{
		out:     f,
		index:   make(hashIndex),
		writeCh: make(chan writeRequest, 128), // буферизований канал
		getCh:   make(chan getRequest, 128),
	}

	// Відновлюємо індекс
	if err := db.recover(); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	db.wg.Add(1)
	go db.backgroundWriter()

    // Додавання воркерів
    for i := 0; i < getWorkerCount; i++ {
		db.getWg.Add(1)
		go db.backgroundReader()
	}

	return db, nil
}

func (db *Db) backgroundWriter() {
	defer db.wg.Done()
	for req := range db.writeCh {
		e := entry{key: req.key, value: req.value}
		data := e.Encode()

		n, err := db.out.Write(data)
		if err == nil {
			db.indexMu.Lock()
			db.index[req.key] = db.outOffset
			db.outOffset += int64(n)
			db.indexMu.Unlock()
		}
		req.done <- err
	}
}
func (db *Db) backgroundReader() {
	defer db.getWg.Done()
	for req := range db.getCh {
		// читаємо запис для ключа
		db.indexMu.RLock()
		position, ok := db.index[req.key]
		db.indexMu.RUnlock()
		if !ok {
			req.response <- getResult{"", ErrNotFound}
			continue
		}

		file, err := os.Open(db.out.Name())
		if err != nil {
			req.response <- getResult{"", err}
			continue
		}
		// закриваємо файл після читання
		func() {
			defer file.Close()

			_, err = file.Seek(position, 0)
			if err != nil {
				req.response <- getResult{"", err}
				return
			}

			var record entry
			if _, err = record.DecodeFromReader(bufio.NewReader(file)); err != nil {
				req.response <- getResult{"", err}
				return
			}

			req.response <- getResult{record.value, nil}
		}()
	}
}

func (db *Db) recover() error {
	f, err := os.Open(db.out.Name())
	if err != nil {
		return err
	}
	defer f.Close()

	in := bufio.NewReader(f)
	for err == nil {
		var (
			record entry
			n      int
		)
		n, err = record.DecodeFromReader(in)
		if errors.Is(err, io.EOF) {
			if n != 0 {
				return fmt.Errorf("corrupted file")
			}
			break
		}

		db.index[record.key] = db.outOffset
		db.outOffset += int64(n)
	}
	return err
}

func (db *Db) Close() error {
	close(db.writeCh)
	db.wg.Wait()

	close(db.getCh)
	db.getWg.Wait()

	return db.out.Close()
}

func (db *Db) Put(key, value string) error {
	done := make(chan error, 1)
	db.writeCh <- writeRequest{
		key:   key,
		value: value,
		done:  done,
	}
	return <-done
}

// Get відправляє запит на отримання значення для ключа
func (db *Db) Get(key string) (string, error) {
	respCh := make(chan getResult, 1)
	db.getCh <- getRequest{key: key, response: respCh}
	res := <-respCh
	return res.value, res.err
}

func (db *Db) Size() (int64, error) {
	info, err := db.out.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}