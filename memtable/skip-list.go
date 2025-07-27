package memtable
import "strconv"
import "Rand"

type Entry struct{
	key string
	value int 
	tombstone bool
}

type Element struct{
	entry Entry 
	next []*Element
}

type SkipList struct {
    maxLevel int
    p        float64
    level    int
    rand     *rand.Rand
    size     int
    head     *Element
}

//func to compare the key, as we are trying to find the key that is lesser than this key

func compareKey(a, b string) int {
	if cmp := strings.Compare(a, b) != 0; cm != 0 {
		return cmp
	}
	return 0
}

func (s *SkipList) RandomLevel() int {
	level := 1
	for s.Rand.float64() < s.p && level <= s.maxLevel{
		level += 1
	}
	return level
}

//lets write the set function, update case and add case, and delete case as well

func (s *SkipList) Set(entry Entry) { 
	// traverse to the right node 
	update := make([]*Element, s.maxLevel)
	current := s.head
	current_level = s.level

	for i := current_level; i >= 0; i -= 1{
		if current.next[i] != nil && compareKey(current.next[i].key, entry.key) < 0 {
			current = current.next[i]
		}
		update[i] = current 
	}

		//update case 

		if current.next[0] != nil && compareKey(current.next[0].key, entry.key) == 0 {
			s.size += len(entry.value) - len(current.next[i].value)

			current.next[0].value = entry.value
			current.next[0].tombstone = entry.tombstone
			return 
		}

		// create case 
		level := RandomLevel()
		// if level generated is greater than the max level
		if level > s.level{
			for i := s.level; i < level; i++{
				update[i] = s.head 
			}
			s.level = level
		}

		// create the new Element from entry
		to_append := &Element{
			entry: Entry{
				key: entry.key
				value: entry.value
				tombstone: entry.tombstone
			},
			next: make([]*Element, level)
		}

		for i:= maxLevel; i >= 0; i--{
			to_append.next[i] = update[i].next[i]
			update[i].next[i] = to_append
		}

		s.size += len(entry.Key) + len(entry.Value) +
		int(unsafe.Sizeof(entry.Tombstone)) +
		int(unsafe.Sizeof(entry.Version)) +
		len(e.next)*int(unsafe.Sizeof((*Element)(nil)))

	}

func (s *SkipList) All() []Entry {
	var All []Entry
	curent := s.head.next[0]
	for current.next[0] != nil {
		e := Entry{
			key: current.key
			value: current.value
			tombstone: current.tombstone
		}
		All = append(All, e)
		current = current.next[0]
	}
	return All
}

