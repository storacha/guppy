package testutil

import (
	"bytes"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

// CopyFSResumable copies the file system fsys into the directory dir,
// creating dir if necessary. Unlike os.CopyFS, this function will skip
// files that already exist with identical content, allowing partial copies
// to be resumed.
//
// Files are created with mode 0o666 plus any execute permissions from the
// source, and directories are created with mode 0o777 (before umask).
//
// If a file exists but has different content or size, an error is returned.
// Symbolic links in fsys are not supported.
//
// Example usage:
//
//	// Retrieve as fs.FS
//	fsys, err := Retrieve(ctx, spaceDID, rootCID)
//	if err != nil {
//	    return err
//	}
//
//	// Copy to disk, resuming if interrupted
//	err = testutil.CopyFSResumable("/path/to/dest", fsys)
func CopyFSResumable(dir string, fsys fs.FS) error {
	return fs.WalkDir(fsys, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if path == "." {
			return nil
		}

		newPath := filepath.Join(dir, path)

		if d.IsDir() {
			return os.MkdirAll(newPath, 0777)
		}

		if !d.Type().IsRegular() {
			return &fs.PathError{Op: "CopyFS", Path: path, Err: fs.ErrInvalid}
		}

		// Open source file
		r, err := fsys.Open(path)
		if err != nil {
			return err
		}
		defer r.Close()

		srcInfo, err := r.Stat()
		if err != nil {
			return err
		}

		// Check if destination file exists
		destInfo, err := os.Stat(newPath)
		if err == nil {
			// File exists - check if it's identical
			if destInfo.Size() != srcInfo.Size() {
				return &fs.PathError{
					Op:   "CopyFS",
					Path: newPath,
					Err:  fs.ErrExist,
				}
			}

			// Compare content
			destFile, err := os.Open(newPath)
			if err != nil {
				return err
			}
			defer destFile.Close()

			srcContent, err := io.ReadAll(r)
			if err != nil {
				return err
			}

			destContent, err := io.ReadAll(destFile)
			if err != nil {
				return err
			}

			if bytes.Equal(srcContent, destContent) {
				// Files are identical, skip
				return nil
			}

			// Files differ
			return &fs.PathError{
				Op:   "CopyFS",
				Path: newPath,
				Err:  fs.ErrExist,
			}
		} else if !os.IsNotExist(err) {
			return err
		}

		// File doesn't exist, create it
		w, err := os.OpenFile(newPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0666|srcInfo.Mode()&0777)
		if err != nil {
			return err
		}

		if _, err := io.Copy(w, r); err != nil {
			w.Close()
			return &fs.PathError{Op: "Copy", Path: newPath, Err: err}
		}
		return w.Close()
	})
}
