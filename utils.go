package main

import (
	"crypto/rand"
	"fmt"
	"github.com/sirupsen/logrus"
	"os"
	"path"
	"runtime"
	"strconv"
)

func setLogger(level string) {
	lvl, err := logrus.ParseLevel(level)
	if err != nil {
		panic(err)
	}
	log.SetLevel(lvl)

	if production {
		e := os.RemoveAll(fmt.Sprintf("./logs/mystate%d", myServerID))
		if e != nil {
			log.Fatal(e)
		}
		log.Infof(">> old logs removed at " + fmt.Sprintf("./logs/mystate%d", myServerID))

		if err := os.Mkdir(fmt.Sprintf("./logs/mystate%d", myServerID), os.ModePerm); err != nil {
			log.Error(err)
		}
		log.Infof(">> new log folder created at " + fmt.Sprintf("./logs/mystate%d", myServerID))

		log.Formatter = &logrus.TextFormatter{
			ForceColors:               false,
			DisableColors:             true,
			ForceQuote:                false,
			EnvironmentOverrideColors: false,
			DisableTimestamp:          true,
			FullTimestamp:             false,
			TimestampFormat:           "",
			DisableSorting:            false,
			SortingFunc:               nil,
			DisableLevelTruncation:    false,
			PadLevelText:              false,
			QuoteEmptyFields:          false,
			FieldMap:                  nil,
			CallerPrettyfier:          nil,
		}

		log.Out = os.Stdout
		fileName := fmt.Sprintf("./logs/mystate%d", myServerID)
		file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Info("Failed to log to file, using default stderr")
		}
		log.Out = file
		return
	}

	log.SetReportCaller(true)
	log.SetFormatter(&logrus.TextFormatter{
		CallerPrettyfier: func(frame *runtime.Frame) (function string, file string) {
			fileName := path.Base(frame.File) + ":" + strconv.Itoa(frame.Line) + ""
			//return frame.Function, fileName
			return "", fileName + " >>"
		},
	})
}

func genRandomBytes(length int) []byte {
	b := make([]byte, length)
	_, err := rand.Read(b)
	if err != nil {
		log.Println("Error generating random bytes:", err)
	}
	return b
}
