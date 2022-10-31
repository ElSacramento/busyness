package busyness

import (
	"errors"
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
	minInterval time.Duration
	startDate   time.Time

	rwLock *sync.RWMutex
	value  [][]byte
}

func NewBusyness(storedLen, storedCap int, minInterval time.Duration) *Busyness {
	value := make([][]byte, storedLen, storedCap)

	var dayIntervalsNumber = 24 * time.Hour / minInterval
	for i := 0; i < len(value); i++ {
		value[i] = make([]byte, dayIntervalsNumber)
	}

	year, month, day := time.Now().Date()
	startDate := time.Date(year, month, day, 0, 0, 0, 0, time.Now().Location())

	return &Busyness{
		minInterval: minInterval,
		value:       value,
		startDate:   startDate,
		rwLock:      &sync.RWMutex{},
	}
}

func (b *Busyness) getStartDate() time.Time {
	return b.startDate
}

func isEqualDate(time1, time2 time.Time) bool {
	year1, month1, day1 := time1.Date()
	year2, month2, day2 := time2.Date()
	return year1 == year2 && month1 == month2 && day1 == day2
}

func (b *Busyness) getRowsLen() int {
	return len(b.value)
}

func (b *Busyness) getColumnsLen() int {
	return len(b.value[0])
}

func (b *Busyness) getRowIndex(t time.Time) int {
	index := int(t.Sub(b.startDate).Hours() / 24)
	if index < 0 {
		return -1
	}
	return index
}

func (b *Busyness) getColumnIndex(t time.Time) int {
	index := (t.Hour()*60 + t.Minute()) / int(b.minInterval.Minutes())
	if index < 0 || index >= b.getColumnsLen() {
		return -1
	}
	return index
}

func (b *Busyness) getTimeFromRowColumn(row, column int) time.Time {
	return b.startDate.Add(24*time.Hour*time.Duration(row) + b.minInterval*time.Duration(column))
}

func (b *Busyness) getReccurentRowsIDs(rule *rrule.RRule) ([]int, error) {
	indexes := make([]int, 0)

	iterValue := rule.Iterator() // complexity?
	event, hasNext := iterValue()
	for hasNext {
		index := b.getRowIndex(event)
		if index == -1 {
			return nil, errors.New("wrong day")
		}

		if index >= b.getRowsLen() {
			return indexes, nil
		}

		indexes = append(indexes, index)
		event, hasNext = iterValue()
	}

	return indexes, nil
}

func (b *Busyness) getColumnsInterval(m *Meeting) (pairInt, error) {
	fromColumn := b.getColumnIndex(m.from)
	toColumn := b.getColumnIndex(m.to)

	if fromColumn == -1 || toColumn == -1 {
		return pairInt{}, errors.New("impossible time of the day")
	}
	return pairInt{fromColumn, toColumn - 1}, nil
}

func (b *Busyness) getRowsIndexes(m *Meeting) ([]int, error) {
	indexes := make([]int, 0)
	if m.rule == nil {
		fromRow := b.getRowIndex(m.from)

		if fromRow == -1 {
			return nil, errors.New("wrong fromDay")
		}

		if fromRow >= b.getRowsLen() {
			return nil, errors.New("event is out of the period")
		}

		toRow := fromRow

		if !isEqualDate(m.from, m.to) {
			// event that starts today and will be finished tomorrow
			toRow = b.getRowIndex(m.to)

			if toRow == -1 {
				return nil, errors.New("wrong toDay")
			}
		}

		for i := fromRow; i <= toRow && i < b.getRowsLen(); i++ {
			indexes = append(indexes, i)
		}
		return indexes, nil
	}

	indexes, err := b.getReccurentRowsIDs(m.rule)
	if err != nil {
		return nil, err
	}
	return indexes, nil
}

func (b *Busyness) bookTimeSlot(rows []int, columnsInterval pairInt) bool {
	defer b.rwLock.Unlock()
	b.rwLock.Lock()

	// O(days*intervals)*2
	// intervals max value is 48 for 30min minInterval
	// days max value is 90 for 3 months stored period
	for _, i := range rows {
		for j := columnsInterval[0]; j <= columnsInterval[1]; j++ {
			if b.value[i][j] == 1 {
				return false
			}
		}
	}
	for _, i := range rows {
		for j := columnsInterval[0]; j <= columnsInterval[1]; j++ {
			b.value[i][j] = 1
		}
	}
	return true
}

func (b *Busyness) checkTimeSlot(rows []int, columnsInterval pairInt) bool {
	defer b.rwLock.RUnlock()
	b.rwLock.RLock()

	// O(days*intervals)*2
	// intervals max value is 48 for 30min minInterval
	// days max value is 90 for 3 months stored period
	for _, i := range rows {
		for j := columnsInterval[0]; j <= columnsInterval[1]; j++ {
			if b.value[i][j] == 1 {
				return false
			}
		}
	}
	return true
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

	columnsIndexesToChange := make([]int, 0)
	for _, m := range meetings {
		columnsInterval, getColumnsErr := b.getColumnsInterval(&m)
		if getColumnsErr != nil {
			return getColumnsErr
		}
		for i := columnsInterval[0]; i <= columnsInterval[1]; i++ {
			columnsIndexesToChange = append(columnsIndexesToChange, i)
		}
	}

	if len(columnsIndexesToChange) > b.getColumnsLen() {
		return errors.New("impossible situation")
	}

	newRow := make([]byte, b.getColumnsLen())
	for _, ind := range columnsIndexesToChange {
		if ind >= len(newRow) || ind < 0 {
			return errors.New("wrong index to change")
		}
		if newRow[ind] == 1 {
			return errors.New("crossing events")
		}
		newRow[ind] = 1
	}

	b.value = append(b.value, newRow)
	b.value = b.value[1:]
	return nil
}

type pairInt [2]int

func (b *Busyness) getPossiblePairs(currentIndex int, startPair pairInt) []pairInt {
	result := make([]pairInt, 0)
	i := 1
	withLeft := startPair[0]-i > currentIndex
	// O(n)
	for (startPair[1]+i < b.getColumnsLen()) || (startPair[0]-i >= 0) {
		if withLeft && (startPair[0]-i >= 0) {
			leftPair := pairInt{startPair[0] - i, startPair[1] - i}
			result = append(result, leftPair)

			withLeft = startPair[0]-(i+1) > currentIndex
		}

		if startPair[1]+i < b.getColumnsLen() {
			rightPair := pairInt{startPair[0] + i, startPair[1] + i}
			result = append(result, rightPair)
		}

		i += 1
	}
	return result
}

func (b *Busyness) BookOrGetAvailableSlots(m *Meeting, maxNumber int) (bool, []Meeting, error) {
	// todo: validate that from, to and rrule in our stored period of time
	// todo: validate that reccurent event from and to happen in one day
	// todo: validate that from < to, from > time.Now

	// time slots
	columnsInterval, getColumnsErr := b.getColumnsInterval(m)
	if getColumnsErr != nil {
		return false, nil, getColumnsErr
	}

	// days
	rows, getRowsErr := b.getRowsIndexes(m)
	if getRowsErr != nil {
		return false, nil, getRowsErr
	}

	if len(rows) == 0 {
		return false, nil, errors.New("empty period")
	}

	bookStatus := b.bookTimeSlot(rows, columnsInterval)
	if bookStatus {
		return true, nil, nil
	}

	// dont need available slots
	if maxNumber <= 0 {
		return false, nil, nil
	}

	currentIndex := -1
	if isEqualDate(time.Now(), m.from) {
		// the same day
		currentIndex = b.getColumnIndex(time.Now())
	}

	possibleIntervals := b.getPossiblePairs(currentIndex, columnsInterval)

	possibleMeetings := make([]Meeting, 0)
	for _, interval := range possibleIntervals {
		chekStatus := b.checkTimeSlot(rows, interval)
		if chekStatus {
			newMeeting := Meeting{
				from: b.getTimeFromRowColumn(rows[0], interval[0]),
				to:   b.getTimeFromRowColumn(rows[0], interval[1]+1),
			}
			if m.rule != nil {
				newRule := *m.rule
				newRule.DTStart(newMeeting.from)
				newMeeting.rule = &newRule
			}

			possibleMeetings = append(possibleMeetings, newMeeting)
		}

		if len(possibleMeetings) == maxNumber {
			return false, possibleMeetings, nil
		}
	}
	return false, possibleMeetings, nil
}
