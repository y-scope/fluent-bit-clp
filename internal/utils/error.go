// Package implements internal utility functions

package utils

import (
	"log"
)

// If error, log the error then call os.Exit(1)
//
// Parameters:
//   - err
func CheckFatal(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// If error, log it
//
// Parameters:
//   - err
func CheckPrint(err error) {
	if err != nil {
		log.Print(err)
	}
}
