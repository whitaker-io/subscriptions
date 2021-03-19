package utils

import (
	"fmt"
	"reflect"
	"time"
)

func MapStringInterface(key string, m map[string]interface{}) (map[string]interface{}, bool) {
	if x, ok := m[key]; ok {
		if val, ok := x.(map[string]interface{}); !ok {
			panic(fmt.Errorf("%s invalid type want string, have %s", key, reflect.TypeOf(x)))
		} else {
			return val, true
		}
	}

	return map[string]interface{}{}, false
}

func MapStringString(key string, m map[string]interface{}) (map[string]string, bool) {
	if x, ok := m[key]; ok {
		if val, ok := x.(map[string]string); !ok {
			panic(fmt.Errorf("%s invalid type want string, have %s", key, reflect.TypeOf(x)))
		} else {
			return val, true
		}
	}

	return map[string]string{}, false
}

func PStringSlice(key string, m map[string]interface{}) ([]*string, bool) {
	retVal := []*string{}
	if x, ok := m[key]; ok {
		if val, ok := x.([]string); !ok {
			panic(fmt.Errorf("%s invalid type want string, have %s", key, reflect.TypeOf(x)))
		} else {
			for _, v := range val {
				retVal = append(retVal, &v)
			}
			return retVal, true
		}
	}

	return retVal, false
}

func StringSlice(key string, m map[string]interface{}) ([]string, bool) {
	if x, ok := m[key]; ok {
		if val, ok := x.([]string); !ok {
			panic(fmt.Errorf("%s invalid type want string, have %s", key, reflect.TypeOf(x)))
		} else {
			return val, true
		}
	}

	return []string{}, false
}

func String(key string, m map[string]interface{}) (string, bool) {
	if x, ok := m[key]; ok {
		if val, ok := x.(string); !ok {
			panic(fmt.Errorf("%s invalid type want string, have %s", key, reflect.TypeOf(x)))
		} else {
			return val, true
		}
	}

	return "", false
}

func Boolean(key string, m map[string]interface{}) (bool, bool) {
	if x, ok := m[key]; ok {
		if val, ok := x.(bool); !ok {
			panic(fmt.Errorf("%s invalid type want string, have %s", key, reflect.TypeOf(x)))
		} else {
			return val, true
		}
	}

	return false, false
}

func Integer(key string, m map[string]interface{}) (int, bool) {
	if x, ok := m[key]; ok {
		switch val := x.(type) {
		case int64:
			return int(val), ok
		case int:
			return int(val), ok
		case float64:
			return int(val), ok
		case string:
		default:
			panic(fmt.Errorf("%s invalid type want int, have %s", key, reflect.TypeOf(x)))
		}
	}

	return 0, false
}

func Duration(key string, m map[string]interface{}) (time.Duration, bool) {
	if x, ok := m[key]; ok {
		switch val := x.(type) {
		case int64:
			return time.Duration(val), ok
		case int:
			return time.Duration(val), ok
		case float64:
			return time.Duration(val), ok
		case string:
		default:
			panic(fmt.Errorf("%s invalid type want int, have %s", key, reflect.TypeOf(x)))
		}
	}

	return 0, false
}
