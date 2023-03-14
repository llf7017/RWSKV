//go:build !windows && !plan9
// +build !windows,!plan9

/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package badger

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/dgraph-io/badger/v3/y"
	"golang.org/x/sys/unix"
)

// directoryLockGuard holds a lock on a directory and a pid file inside.  The pid file isn't part
// of the locking mechanism, it's just advisory.
type directoryLockGuard struct {
	// File handle on the directory, which we've flocked.
	f *os.File
	// The absolute path to our pid file.
	path string
	// Was this a shared lock for a read-only database?
	readOnly bool
}

// acquireDirectoryLock gets a lock on the directory (using flock). If
// this is not read-only, it will also write our pid to
// dirPath/pidFileName for convenience.
//用于创建锁
func acquireDirectoryLock(dirPath string, pidFileName string, readOnly bool) (
	*directoryLockGuard, error) {
	// Convert to absolute path so that Release still works even if we do an unbalanced
	// chdir in the meantime.
	//转换为绝对路径
	// filepath.Abs()检测地址是否是绝对地址，是绝对地址直接返回，不是绝对地址，会添加当前工作路径到参数path前，然后返回
	absPidFilePath, err := filepath.Abs(filepath.Join(dirPath, pidFileName))
	if err != nil {
		return nil, y.Wrapf(err, "cannot get absolute path for pid lock file")
	}
	//Open打开一个文件用于读取。如果操作成功，返回的文件对象的方法可用于读取数据
	f, err := os.Open(dirPath)
	if err != nil {
		return nil, y.Wrapf(err, "cannot open directory %q", dirPath)
	}
	//含有多个线程/进程的系统在运行过程中，往往会出现多个线程/进程在同一时间去读取或修改同一内存地址的数据，为了保证数据的完整性，最常用的方式是使用锁机制
	//LOCK_EX	对 fd 引用的文件加排他锁 exclusive lock
	//LOCK_NB	无法建立锁定时，此操作不会被阻塞，将立即返回操作结果。
	opts := unix.LOCK_EX | unix.LOCK_NB
	if readOnly {
		//LOCK_SH：建立共享锁定。多个进程可同时对同一个文件作共享锁定。
		opts = unix.LOCK_SH | unix.LOCK_NB
	}
	// Fd返回整数的Unix文件描述符，引用打开的文件。
	// 如果f被关闭，文件描述符就会失效。
	// 如果f被垃圾回收，终结者可能会关闭文件描述符，使其失效；关于何时可能运行终结者的更多信息，见runtime.SetFinalizer。在Unix系统上，这将导致SetDeadline方法停止工作。
	// 因为文件描述符可以被重复使用，返回的文件描述符只能通过f的Close方法关闭，或者在垃圾收集期间由它的终结者关闭。否则，在垃圾收集过程中，终结者 可能会关闭一个具有相同（重复使用）编号的无关的文件描述符。
	//
	// 作为一种替代方法，请参见f.SyscallConn方法。
	//unix.Flock函数用于加锁
	err = unix.Flock(int(f.Fd()), opts)
	if err != nil {
		f.Close()
		return nil, y.Wrapf(err,
			"Cannot acquire directory lock on %q.  Another process is using this Badger database.",
			dirPath)
	}

	if !readOnly {
		// Yes, we happily overwrite a pre-existing pid file.  We're the
		// only read-write badger process using this directory.
		err = ioutil.WriteFile(absPidFilePath, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0666)
		if err != nil {
			f.Close()
			return nil, y.Wrapf(err,
				"Cannot write pid file %q", absPidFilePath)
		}
	}
	return &directoryLockGuard{f, absPidFilePath, readOnly}, nil
}

// Release deletes the pid file and releases our lock on the directory.
func (guard *directoryLockGuard) release() error {
	var err error
	if !guard.readOnly {
		// It's important that we remove the pid file first.
		err = os.Remove(guard.path)
	}

	if closeErr := guard.f.Close(); err == nil {
		err = closeErr
	}
	guard.path = ""
	guard.f = nil

	return err
}

// openDir opens a directory for syncing.
func openDir(path string) (*os.File, error) { return os.Open(path) }

// When you create or delete a file, you have to ensure the directory entry for the file is synced
// in order to guarantee the file is visible (if the system crashes). (See the man page for fsync,
// or see https://github.com/coreos/etcd/issues/6368 for an example.)
func syncDir(dir string) error {
	f, err := openDir(dir)
	if err != nil {
		return y.Wrapf(err, "While opening directory: %s.", dir)
	}

	err = f.Sync()
	closeErr := f.Close()
	if err != nil {
		return y.Wrapf(err, "While syncing directory: %s.", dir)
	}
	return y.Wrapf(closeErr, "While closing directory: %s.", dir)
}
