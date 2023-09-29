package utils

import "encoding/json"

func MappingToString(mapping interface{}) (string, error) {
	mappingJson, err := json.Marshal(mapping)
	if err != nil {
		return "", err
	}
	return string(mappingJson), nil
}
