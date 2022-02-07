package string_set

import (
	//"fmt"
	"regexp"
	"strings"
	"sync"
)

type StringSet interface {
	// Add string s to the StringSet and return whether the string was inserted
	// in the set.
	Add(key string) bool

	// Return the number of unique strings in the set
	Count() int

	// Return all strings matching a regex `pattern` within a range `[begin,
	// end)` lexicographically (for Part C)
	PredRange(begin string, end string, pattern string) []string
}

type LockedStringSet struct {
	items map[string]bool
	sync.RWMutex
}

func MakeLockedStringSet() LockedStringSet {
	return LockedStringSet{
		items: make(map[string]bool),
	}
}

func (stringSet *LockedStringSet) Add(key string) bool {
	stringSet.Lock()
	defer stringSet.Unlock()
	if stringSet.items[key] {
		return false
	} else {
		stringSet.items[key] = true
		return true
	}
}

func (stringSet *LockedStringSet) Count() int {
	var size int
	stringSet.RLock()
	size = len(stringSet.items)
	stringSet.RUnlock()
	return size
}

func (stringSet *LockedStringSet) PredRange(begin string, end string, pattern string) []string {
	matches := make([]string, 0)

	stringSet.RLock()
	defer stringSet.RUnlock()
	for word, _ := range stringSet.items {
		//fmt.Println("the word: ", word)
		if strings.Compare(begin, word) <= 0 && strings.Compare(end, word) >= 0 { // check range
			//fmt.Println("inside range: ", word)
			if matched, _ := regexp.MatchString(pattern, word); matched { // check regexp match
				matches = append(matches, word)
			}
		}
	}
	return matches
}
