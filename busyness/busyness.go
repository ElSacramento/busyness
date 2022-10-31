package busyness

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/teambition/rrule-go"
)

/*
	10:00 - 11:00 	11:00 - 12:00	12:00 - 13:00

day1	0		1		0
day2	1		1		1
day3	1		0		1
*/
type Busyness struct {
	interval    time.Duration
	startDate   time.Time
	dayBytesLen int

	rwLock *sync.RWMutex
	value  []byte
}

const (
	minPossibleInterval = time.Minute * 15
	maxPossibleInterval = time.Hour * 3
	bitsLen             = 8
)

func NewBusyness(storedDays int, interval time.Duration) (*Busyness, error) {
	if interval < minPossibleInterval {
		return nil, errors.New("too small interval")
	}
	if interval > maxPossibleInterval {
		return nil, errors.New("too big interval")
	}

	// for interval=30min we need 48bits = 6bytes
	// for 7 days we need 6bytes*7 = 42bytes
	bytesNumberForDay := int((24 * time.Hour / interval) / bitsLen)
	value := make([]byte, bytesNumberForDay*storedDays, bytesNumberForDay*(storedDays+1))

	year, month, day := time.Now().Date()
	startDate := time.Date(year, month, day, 0, 0, 0, 0, time.Now().Location())

	return &Busyness{
		interval:    interval,
		value:       value,
		startDate:   startDate,
		dayBytesLen: bytesNumberForDay,
		rwLock:      &sync.RWMutex{},
	}, nil
}

func isEqualDate(t1, t2 time.Time) bool {
	return t1.Year() == t2.Year() && t1.Month() == t2.Month() && t1.Day() == t2.Day()
}

func (b *Busyness) geStoredBitsLen() int {
	return len(b.value) * bitsLen
}

func (b *Busyness) getBitIndex(t time.Time) int {
	index := int(t.Sub(b.startDate).Minutes() / b.interval.Minutes())
	if index < 0 {
		return -1
	}
	return index
}

func (b *Busyness) getTimeFromIndex(index int) time.Time {
	return b.startDate.Add(time.Duration(index * int(b.interval)))
}

func (b *Busyness) getMeetingLength(m *Meeting) (int, error) {
	fromIndex := b.getBitIndex(m.from)
	toIndex := b.getBitIndex(m.to)

	if fromIndex == -1 || toIndex == -1 {
		return 0, errors.New("wrong fromDay or toDay time")
	}

	return toIndex - fromIndex, nil
}

func (b *Busyness) getStartIndexes(m *Meeting) ([]int, error) {
	fromIndex := b.getBitIndex(m.from)
	if fromIndex >= b.geStoredBitsLen() {
		return nil, errors.New("event is out of the period")
	}

	if m.rule == nil {
		return []int{fromIndex}, nil
	}

	indexes := make([]int, 0)
	iterValue := m.rule.Iterator() // complexity?
	event, hasNext := iterValue()
	for hasNext {
		index := b.getBitIndex(event)
		if index == -1 {
			return nil, errors.New("wrong day")
		}

		if index >= b.geStoredBitsLen() {
			return indexes, nil
		}

		indexes = append(indexes, index)
		event, hasNext = iterValue()
	}

	return indexes, nil
}

func getByteIndexes(index int) (int, int) {
	div := index / bitsLen
	mod := index % bitsLen
	return div, mod
}

type ByteInfo struct {
	index    int
	value    byte
	startBit int
	// endBit   int
}

func convertToBytes(startBitIndexes []int, sequenceLength int) []ByteInfo {
	expectedBytes := make([]ByteInfo, 0)
	// max(O(n*k), O(n*8)), if k > 0 , rare situations
	// O(n*8)
	for _, i := range startBitIndexes {
		byteFromIndex, bitFromIndex := getByteIndexes(i)
		bitToIndex := bitFromIndex + sequenceLength - 1
		byteToIndex := byteFromIndex
		for bitToIndex >= bitsLen {
			byteToIndex += 1
			bitToIndex -= bitsLen
		}

		if byteFromIndex == byteToIndex {
			// create 00001110
			// sequenceLength = 3
			// bitFromIndex = 4, bitToIndex = 6
			res := byte(1)
			for i := 0; i < bitToIndex-bitFromIndex; i++ {
				res = res << 1
				res = res + byte(1)
			}
			// now we have 00000111
			res = res << byte(bitsLen-bitToIndex-1)
			// now we have 00001110
			expectedBytes = append(expectedBytes, ByteInfo{
				index:    byteFromIndex,
				value:    res,
				startBit: bitFromIndex,
				// endBit:   bitToIndex,
			})
			continue
		}

		// create 00000011 and 11000000
		// sequenceLength = 4
		// bitFromIndex = 6, bitToIndex = 1
		res := byte(1)
		for i := 0; i < bitsLen-bitFromIndex-1; i++ {
			res = res << 1
			res = res + byte(1)
		}
		// now we have 00000011
		expectedBytes = append(expectedBytes, ByteInfo{
			index:    byteFromIndex,
			value:    res,
			startBit: bitFromIndex,
			// endBit:   7,
		})

		// if period longer than 2 bytes
		for i := 1; i < byteToIndex-byteFromIndex; i++ {
			expectedBytes = append(expectedBytes, ByteInfo{
				index:    byteFromIndex + i,
				value:    byte(255),
				startBit: 0,
				// endBit:   7,
			})
		}

		res = byte(1)
		for i := 0; i < bitToIndex; i++ {
			res = res << 1
			res = res + byte(1)
		}
		// now we have 00000011
		res = res << byte(bitsLen-bitToIndex-1)
		// now we have 11000000
		expectedBytes = append(expectedBytes, ByteInfo{
			index:    byteToIndex,
			value:    res,
			startBit: 0,
			// endBit:   bitToIndex,
		})
	}
	return expectedBytes
}

func (b *Busyness) bookTimeSlot(bytesInfo []ByteInfo) (bool, error) {
	defer b.rwLock.Unlock()
	b.rwLock.Lock()

	if len(bytesInfo) == 0 {
		return false, errors.New("empty bytes array")
	}

	for _, info := range bytesInfo {
		if info.index >= len(b.value) {
			break
		}
		if b.value[info.index]&info.value != 0 {
			return false, nil
		}
	}
	for _, info := range bytesInfo {
		if info.index >= len(b.value) {
			break
		}
		b.value[info.index] = b.value[info.index] | info.value
	}
	return true, nil
}

func (b *Busyness) checkTimeSlot(bytesInfo []ByteInfo) (bool, error) {
	defer b.rwLock.RUnlock()
	b.rwLock.RLock()

	if len(bytesInfo) == 0 {
		return false, errors.New("empty bytes array")
	}

	for _, info := range bytesInfo {
		if info.index >= len(b.value) {
			break
		}
		if b.value[info.index]&info.value != 0 {
			return false, nil
		}
	}
	return true, nil
}

type Meeting struct {
	from time.Time
	to   time.Time
	rule *rrule.RRule
}

func (b *Busyness) AppendDay(meetings []Meeting) error {
	// todo: check that day to append doesn't exist in b.value

	defer b.rwLock.Unlock()
	b.rwLock.Lock()

	bytesInfo := make([]ByteInfo, 0)
	for _, m := range meetings {
		meetingLength, getMLengthErr := b.getMeetingLength(&m)
		if getMLengthErr != nil {
			return getMLengthErr
		}

		if meetingLength == 0 {
			continue
		}

		fromIndex := b.getBitIndex(m.from)
		bytesInfo = append(bytesInfo, convertToBytes([]int{fromIndex}, meetingLength)...)
	}

	newDay := make([]byte, b.dayBytesLen)
	b.value = append(b.value, newDay...)

	for _, info := range bytesInfo {
		if info.index >= len(b.value) {
			continue
		}
		if b.value[info.index]&info.value != 0 {
			return errors.New("crossing events")
		}
		b.value[info.index] = b.value[info.index] | info.value
	}

	b.value = b.value[b.dayBytesLen:]
	b.startDate = b.startDate.Add(24 * time.Hour)
	return nil
}

func getBitsPositions(byteIndex int, v byte) []int {
	if v == 0 {
		return []int{}
	}

	checkValues := []byte{1, 2, 4, 8, 16, 32, 64, 128}
	positions := make([]int, 0, 8)
	defaultIncrement := byteIndex * bitsLen
	for i, checkValue := range checkValues {
		if v&checkValue != 0 {
			positions = append(positions, defaultIncrement+(bitsLen-1-i))
		}
	}
	return positions
}

func (b *Busyness) GetBusyness(period time.Duration) []Meeting {
	defer b.rwLock.RUnlock()
	b.rwLock.RLock()

	byteIndex, bitIndex := getByteIndexes(int(period / b.interval))
	if bitIndex != 0 {
		byteIndex += 1
	}
	if byteIndex >= len(b.value) {
		byteIndex = len(b.value) - 1
	}

	bitsIndexes := make([]int, 0)
	for i := 0; i <= byteIndex; i++ {
		currentByte := b.value[i]
		bitsIndexes = append(bitsIndexes, getBitsPositions(i, currentByte)...)
	}
	sort.Ints(bitsIndexes) // assume later that it's sorted O(nlogn)

	meetings := make([]Meeting, 0)
	// O(n)
	var prevTo time.Time
	for _, index := range bitsIndexes {
		from := b.getTimeFromIndex(index)
		to := from.Add(b.interval)

		if len(meetings) == 0 {
			meetings = append(meetings, Meeting{
				from: from,
				to:   to,
			})
			prevTo = to
			continue
		}

		if from.Equal(prevTo) {
			meetings[len(meetings)-1].to = to
			prevTo = to
			continue
		}

		meetings = append(meetings, Meeting{
			from: from,
			to:   to,
		})
		prevTo = to
	}
	return meetings
}

// func (b *Busyness) getPossibleConfigurations(from time.Time, startIndexes []int, meetingLength int) [][]ByteInfo {
// 	year, month, day := from.Date()
// 	nextDay := time.Date(year, month, day+1, 0, 0, 0, 0, from.Location())

// 	fromIndex := b.getBitIndex(from)
// 	toIndex := b.getBitIndex(nextDay)
// 	// until next day
// 	withLeftShift := startIndexes[0] > fromIndex
// 	withRightShift := startIndexes[0]+meetingLength < toIndex

// 	possibleConfigurations := make([][]ByteInfo, 0)
// 	// todo
// 	return possibleConfigurations
// }

func (b *Busyness) getMeetingFromBytes(bytesInfo []ByteInfo) (Meeting, error) {
	bitsIndexes := make([]int, 0)
	for _, info := range bytesInfo {
		bitsIndexes = append(bitsIndexes, getBitsPositions(info.index, info.value)...)
	}
	sort.Ints(bitsIndexes) // assume later that it's sorted O(nlogn)

	// remove duplicates
	cleanedIndexes := make([]int, 0, len(bitsIndexes))
	for i := 0; i < len(bitsIndexes)-1; i++ {
		if bitsIndexes[i] == bitsIndexes[i+1] {
			continue
		}
		cleanedIndexes = append(cleanedIndexes, bitsIndexes[i])
	}

	if len(cleanedIndexes) == 0 {
		return Meeting{}, errors.New("empty indexes list")
	}

	for i, index := range cleanedIndexes {
		if index == cleanedIndexes[0]+i {
			continue
		}

		return Meeting{
			from: b.getTimeFromIndex(cleanedIndexes[0]),
			to:   b.getTimeFromIndex(index),
		}, nil
	}
	return Meeting{}, nil
}

func (b *Busyness) BookOrGetAvailableSlots(m *Meeting, maxNumber int) (bool, []Meeting, error) {
	// todo: validate that from, to and rrule in our stored period of time
	// todo: validate that reccurent event from and to happen in one day
	// todo: validate that from < to, from > time.Now

	meetingLength, getMLengthErr := b.getMeetingLength(m) // O(1)
	if getMLengthErr != nil {
		return false, nil, getMLengthErr
	}

	startIndexes, getIndexes := b.getStartIndexes(m) // depends on the rrule lib
	if getIndexes != nil {
		return false, nil, getIndexes
	}
	sort.Ints(startIndexes) // assume later that array is sorted - O

	if meetingLength == 0 {
		return false, nil, errors.New("empty period")
	}

	bytesInfo := convertToBytes(startIndexes, meetingLength) // O(n*8)
	bookStatus, bookErr := b.bookTimeSlot(bytesInfo)         // O(n)
	if bookErr != nil {
		return false, nil, bookErr
	}
	if bookStatus {
		return true, nil, nil
	}

	// dont need available slots
	if maxNumber <= 0 {
		return false, nil, nil
	}

	// dont need to find time slots in the past
	// fromTime := m.from
	// if isEqualDate(m.from, time.Now()) {
	// 	fromTime = time.Now()
	// }
	// possibleConfigurations := b.getPossibleConfigurations(fromTime, startIndexes, meetingLength)
	// possibleMeetings := make([]Meeting, 0)
	// for _, bytesInfo := range possibleConfigurations {
	// 	// bytesInfo := convertToBytes(indexes, meetingLength)
	// 	chekStatus, checkErr := b.checkTimeSlot(bytesInfo)
	// 	if checkErr != nil {
	// 		return false, nil, checkErr
	// 	}
	// 	if chekStatus {
	// 		newMeeting, getMeetingErr := b.getMeetingFromBytes(bytesInfo)
	// 		if getMeetingErr != nil {
	// 			return false, nil, getMeetingErr
	// 		}
	// 		if m.rule != nil {
	// 			newRule := *m.rule
	// 			newRule.DTStart(newMeeting.from)
	// 			newMeeting.rule = &newRule
	// 		}

	// 		possibleMeetings = append(possibleMeetings, newMeeting)
	// 	}

	// 	if len(possibleMeetings) == maxNumber {
	// 		return false, possibleMeetings, nil
	// 	}
	// }
	return false, []Meeting{}, nil
}
