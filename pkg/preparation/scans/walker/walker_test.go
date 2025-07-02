package walker_test

import (
	"io/fs"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/afero"
	"github.com/storacha/go-ucanto/core/iterable"
	"github.com/storacha/guppy/pkg/preparation/scans/model"
	"github.com/storacha/guppy/pkg/preparation/scans/walker"
	"github.com/stretchr/testify/require"
)

func TestWalker(t *testing.T) {
	memFS := afero.NewMemMapFs()
	memFS.MkdirAll("dir1/subdir1", 0755)
	afero.WriteFile(memFS, "file1.txt", []byte("contents of file1.txt"), 0644)
	afero.WriteFile(memFS, "dir1/file2.txt", []byte("contents of file2.txt"), 0644)
	afero.WriteFile(memFS, "dir1/subdir1/file3.txt", []byte("contents of file3.txt"), 0644)

	mockVisitor := &mockFSVisitor{
		visitedChildren: make(map[string][]model.FSEntry),
	}

	rt, err := walker.WalkDir(afero.NewIOFS(memFS), ".", mockVisitor)
	require.NoError(t, err, "WalkDir should not return an error")
	require.Equal(t, rt.Path(), ".", "Root path should match the provided root")
	require.Len(t, mockVisitor.visitedFiles, 3, "Should visit 3 files")
	require.ElementsMatch(t, mockVisitor.visitedFiles, []string{
		"file1.txt",
		"dir1/file2.txt",
		"dir1/subdir1/file3.txt",
	}, "Visited files should match expected paths")

	require.Len(t, mockVisitor.visitedDirectories, 3, "Should visit 3 directories")
	require.ElementsMatch(t, mockVisitor.visitedDirectories, []string{
		".",
		"dir1",
		"dir1/subdir1",
	}, "Visited directories should match expected paths")

	require.Len(t, mockVisitor.visitedChildren, 3, "Should have children for each visited directory")
	require.Len(t, mockVisitor.visitedChildren["."], 2, "Root directory should have 2 children")
	require.ElementsMatch(t, slices.Collect(iterable.Map(func(child model.FSEntry) string {
		return child.Path()
	}, slices.Values(mockVisitor.visitedChildren["."]))), []string{
		"file1.txt",
		"dir1",
	}, "Children of root directory should match expected paths")

	require.Len(t, mockVisitor.visitedChildren["dir1"], 2, "Directory 'dir1' should have 2 children")
	require.ElementsMatch(t, slices.Collect(iterable.Map(func(child model.FSEntry) string {
		return child.Path()
	}, slices.Values(mockVisitor.visitedChildren["dir1"]))), []string{
		"dir1/file2.txt",
		"dir1/subdir1",
	}, "Children of directory 'dir1' should match expected paths")
	require.Len(t, mockVisitor.visitedChildren["dir1/subdir1"], 1, "Directory 'subdir1' should have 1 child")
	require.ElementsMatch(t, slices.Collect(iterable.Map(func(child model.FSEntry) string {
		return child.Path()
	},
		slices.Values(mockVisitor.visitedChildren["dir1/subdir1"]))), []string{
		"dir1/subdir1/file3.txt",
	}, "Children of directory 'subdir1' should match expected paths")
}

type mockFSVisitor struct {
	visitedFiles       []string
	visitedDirectories []string
	visitedChildren    map[string][]model.FSEntry
}

func (v *mockFSVisitor) VisitFile(path string, dirEntry fs.DirEntry) (*model.File, error) {
	v.visitedFiles = append(v.visitedFiles, path)
	return model.NewFile(path, time.Now(), 0, 0, []byte(path), uuid.New())
}
func (v *mockFSVisitor) VisitDirectory(path string, dirEntry fs.DirEntry, children []model.FSEntry) (*model.Directory, error) {
	v.visitedDirectories = append(v.visitedDirectories, path)
	v.visitedChildren[path] = children
	return model.NewDirectory(path, time.Now(), 0, []byte(path), uuid.New())
}
