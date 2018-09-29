package extract

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

// FileLineExtractor 定义了文件行提取器。它对文件每行进行单独的提取处理，写入目标文件中。
type FileLineExtractor struct {
	SrcFilepath  string
	DestFilepath string

	LineExtractor LineExtractor
}

// LineExtractor 表示对每行内容的提取操作。如果提取到内容，则返回且 ok 必须为 true。
// 否则为 false。
type LineExtractor func(line string) (subLine string, ok bool)

// Extract 对文件每行进行提取操作，并写入目标文件中。
func (f *FileLineExtractor) Extract() error {
	if f.SrcFilepath == "" {
		return errors.New("source file path can't be  empty")
	}
	if f.DestFilepath == "" {
		return errors.New("destination file path can't be  empty")
	}

	srcFile, err := os.Open(f.SrcFilepath)
	if err != nil {
		return fmt.Errorf("open source file fail: %v", err)
	}
	defer srcFile.Close()

	destFile, err := os.OpenFile(
		f.DestFilepath,
		os.O_CREATE|os.O_APPEND|os.O_TRUNC|os.O_WRONLY,
		0644,
	)
	if err != nil {
		return fmt.Errorf("destination source file fail: %v", err)
	}
	defer destFile.Close()

	r := bufio.NewReader(srcFile)

	var wg sync.WaitGroup
	var readError error
	for {
		lineBytes, err := r.ReadBytes('\n')
		if err != nil {
			if err != io.EOF {
				readError = err
			}
			break
		}
		line := string(lineBytes)

		wg.Add(1)
		go func() {
			defer wg.Done()

			if extracted, ok := f.LineExtractor(line); ok {
				_, err := destFile.WriteString(extracted + "\n")
				if err != nil {
					return
				}
			}
		}()
	}

	wg.Wait()
	return readError
}
