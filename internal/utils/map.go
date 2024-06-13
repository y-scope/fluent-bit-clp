// convertMap recursively converts a map[interface{}]interface{} to map[string]interface{}.

package utils

import "fmt"
import "encoding/base64"

// SafeMap recursively converts a map[interface{}]interface{} to map[string]interface{}.
func SafeMap(record map[interface{}]interface{}) map[string]interface{} {
	safeRecord := make(map[string]interface{})
	for k, v := range record { // Corrected: Range over the input map
		switch t := v.(type) {
		case map[interface{}]interface{}:
			safeRecord[k.(string)] = SafeMap(t)
		case []interface{}:
			safeRecord[k.(string)] = SafeSlice(t)
		case []byte:
			safeRecord[k.(string)] = string(t)
		default:
			safeRecord[k.(string)] = v
		}
	}
	return safeRecord
}

// SafeSlice recursively converts a slice of interface{} to []interface{}.
func SafeSlice(slice []interface{}) []interface{} {
	safeSlice := make([]interface{}, len(slice))
	for i, v := range slice {
		switch t := v.(type) {
		case map[interface{}]interface{}:
			safeSlice[i] = SafeMap(t)
		case []interface{}:
			safeSlice[i] = SafeSlice(t)
		case []byte:
			safeSlice[i] = string(t)
		default:
			safeSlice[i] = v
		}
	}
	return safeSlice
}

func main() {
	// Test map with various data types
	record := map[interface{}]interface{}{
		"name":   "John",
		"age":    30,
		"scores": []int{90, 85, 95},
		"info": map[interface{}]interface{}{
			"address": base64.StdEncoding.EncodeToString([]byte("123 Main St")),
			"zip":     12345,
		},
	}

	// Convert the map to a safe map
	safeRecord := SafeMap(record)

	// Print the safe map
	fmt.Println("Safe Record:")
	printMap(safeRecord)

	// Test slice with various data types
	slice := []interface{}{
		"Alice",
		25,
		[]int{80, 75, 85},
		map[interface{}]interface{}{
			"address": "456 Elm St",
			"zip":     54321,
		},
	}

	// Convert the slice to a safe slice
	safeSlice := SafeSlice(slice)

	// Print the safe slice
	fmt.Println("\nSafe Slice:")
	printSlice(safeSlice)
}

// Helper function to print the contents of a map
func printMap(m map[string]interface{}) {
	for k, v := range m {
		fmt.Printf("%s: %v\n", k, v)
	}
}

// Helper function to print the contents of a slice
func printSlice(slice []interface{}) {
	for i, v := range slice {
		fmt.Printf("[%d]: %v\n", i, v)
	}
}
