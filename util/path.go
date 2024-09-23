package util

import (
	"fmt"
	"os"
	"path/filepath"
)

func CreatePath(path string) error {
	base := filepath.Base(path)
	dir := filepath.Dir(path)

	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("无法创建目录: %v", err)
	}

	if filepath.Ext(base) != "" {

		file, err := os.Create(path)
		if err != nil {
			return fmt.Errorf("无法创建文件: %v", err)
		}
		defer file.Close()

		fmt.Printf("成功创建了文件: %s\n", path)
	} else {

		err := os.MkdirAll(path, os.ModePerm)
		if err != nil {
			return fmt.Errorf("无法创建目录: %v", err)
		}
	}

	return nil
}
