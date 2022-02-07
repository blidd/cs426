package string_set

import (
	"hash/fnv"
)

type StripedStringSet struct {
	stripes []*LockedStringSet
	stripeCount uint32
}

func hash32(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

func MakeStripedStringSet(stripeCount int) StripedStringSet {
	stringSet := StripedStringSet{
		stripes: make([]*LockedStringSet, stripeCount),
		stripeCount: uint32(stripeCount),
	}
	for i := 0; i < stripeCount; i++ {
		lss := MakeLockedStringSet()
		stringSet.stripes[i] = &lss
	}
	return stringSet
}

func (stringSet *StripedStringSet) Add(key string) bool {
	bucket := stringSet.stripes[hash32(key) % stringSet.stripeCount]
	return bucket.Add(key)
}

func (stringSet *StripedStringSet) Count() int {
	count := 0
	for _, stripe := range stringSet.stripes {
		count += stripe.Count()
	}
	return count
}

func (stringSet *StripedStringSet) PredRange(begin string, end string, pattern string) []string {

	//var wg sync.WaitGroup
	ch := make(chan string)
	defer close(ch)

	for _, stripe := range stringSet.stripes {
		//wg.Add(1)
		go func(stripe *LockedStringSet) {
			//defer wg.Done()
			matches := stripe.PredRange(begin, end, pattern)
			//fmt.Println(matches)
			for _, word := range matches {
				ch <- word
			}
			ch <- "\000" // send null terminator to signal that we've checked every string in this stripe
		}(stripe)
	}

	done := 0
	matches := make([]string, 0)
	for {
		word := <-ch
		if word == "\000" { // keep track of number of completed goroutines
			if done++; done == int(stringSet.stripeCount) {
				return matches
			}
		} else {
			matches = append(matches, word)
		}
	}
}
