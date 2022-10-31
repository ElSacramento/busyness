package busyness

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/teambition/rrule-go"
)

func TestGetBitIndex(t *testing.T) {
	// one week with 30min slots
	b, err := NewBusyness(7, time.Minute*30)
	require.Nil(t, err)
	startDate := b.startDate

	t.Run("start day", func(t *testing.T) {
		index := b.getBitIndex(startDate.Add(2 * time.Hour))
		require.Equal(t, 4, index)
	})
	t.Run("another day", func(t *testing.T) {
		index := b.getBitIndex(startDate.Add(48 * time.Hour))
		require.Equal(t, 48*2, index)
	})
	t.Run("out of the period", func(t *testing.T) {
		index := b.getBitIndex(startDate.Add(8 * 24 * time.Hour))
		require.Greater(t, index, len(b.value))
	})
}

func TestStartIndexes(t *testing.T) {
	// one week with 30min slots
	b, err := NewBusyness(7, time.Minute*30)
	require.Nil(t, err)
	startDate := b.startDate

	t.Run("same day", func(t *testing.T) {
		m := &Meeting{
			from: startDate,
			to:   startDate.Add(2 * time.Hour),
		}
		indexes, err := b.getStartIndexes(m)
		require.Nil(t, err)
		require.Equal(t, []int{0}, indexes)
	})
	t.Run("two days meeting", func(t *testing.T) {
		m := &Meeting{
			from: startDate.Add(2 * time.Hour),
			to:   startDate.Add(28 * time.Hour),
		}
		indexes, err := b.getStartIndexes(m)
		require.Nil(t, err)
		require.Equal(t, []int{4}, indexes)
	})
	t.Run("fromDay is in the end of the period", func(t *testing.T) {
		m := &Meeting{
			from: startDate.Add(6 * 24 * time.Hour),
			to:   startDate.Add(7 * 24 * time.Hour),
		}
		indexes, err := b.getStartIndexes(m)
		require.Nil(t, err)
		require.Equal(t, []int{288}, indexes)
	})
	t.Run("fromDay is out of the period", func(t *testing.T) {
		m := &Meeting{
			from: startDate.Add(8 * 24 * time.Hour),
			to:   startDate.Add(9 * 24 * time.Hour),
		}
		indexes, err := b.getStartIndexes(m)
		require.Nil(t, indexes)
		require.Equal(t, "event is out of the period", err.Error())
	})
	t.Run("toDay is out of the period", func(t *testing.T) {
		m := &Meeting{
			from: startDate,
			to:   startDate.Add(9 * 24 * time.Hour),
		}
		indexes, err := b.getStartIndexes(m)
		require.Nil(t, err)
		require.Equal(t, []int{0}, indexes)
	})
}

func TestGetMeetingLength(t *testing.T) {
	// one week with 30min slots
	b, err := NewBusyness(7, time.Minute*30)
	require.Nil(t, err)
	startDate := b.startDate

	t.Run("min interval", func(t *testing.T) {
		m := &Meeting{
			from: startDate,
			to:   startDate.Add(30 * time.Minute),
		}
		result, err := b.getMeetingLength(m)
		require.Nil(t, err)
		require.Equal(t, 1, result)
	})
	t.Run("one hour", func(t *testing.T) {
		m := &Meeting{
			from: startDate,
			to:   startDate.Add(time.Hour),
		}
		result, err := b.getMeetingLength(m)
		require.Nil(t, err)
		require.Equal(t, 2, result)
	})
	t.Run("5,5 hours", func(t *testing.T) {
		m := &Meeting{
			from: startDate,
			to:   startDate.Add(5*time.Hour + 30*time.Minute),
		}
		result, err := b.getMeetingLength(m)
		require.Nil(t, err)
		require.Equal(t, 11, result)
	})
}

func TestConvertToBytes(t *testing.T) {
	t.Run("one byte sequence", func(t *testing.T) {
		result := convertToBytes([]int{2}, 2)
		expected := []ByteInfo{
			{index: 0, value: 0b00110000, startBit: 2},
		}
		require.Equal(t, expected, result)

		result = convertToBytes([]int{0}, 2)
		expected = []ByteInfo{
			{index: 0, value: 0b11000000, startBit: 0},
		}
		require.Equal(t, expected, result)

		result = convertToBytes([]int{6}, 2)
		expected = []ByteInfo{
			{index: 0, value: 0b00000011, startBit: 6},
		}
		require.Equal(t, expected, result)
	})
	t.Run("two bytes sequence", func(t *testing.T) {
		result := convertToBytes([]int{6}, 4)
		expected := []ByteInfo{
			{index: 0, value: 0b00000011, startBit: 6},
			{index: 1, value: 0b11000000, startBit: 0},
		}
		require.Equal(t, expected, result)
	})
	t.Run("more bytes sequence", func(t *testing.T) {
		result := convertToBytes([]int{6}, 12)
		expected := []ByteInfo{
			{index: 0, value: 0b00000011, startBit: 6},
			{index: 1, value: 0b11111111, startBit: 0},
			{index: 2, value: 0b11000000, startBit: 0},
		}
		require.Equal(t, expected, result)
	})
	t.Run("periodic bytes", func(t *testing.T) {
		result := convertToBytes([]int{2, 10, 18}, 2)
		expected := []ByteInfo{
			{index: 0, value: 0b00110000, startBit: 2},
			{index: 1, value: 0b00110000, startBit: 2},
			{index: 2, value: 0b00110000, startBit: 2},
		}
		require.Equal(t, expected, result)
	})
}

func TestAppendDay(t *testing.T) {
	t.Run("succes", func(t *testing.T) {
		// one week with 30min slots
		b, err := NewBusyness(7, time.Minute*30)
		require.Nil(t, err)

		appendDay := b.startDate.Add(7 * 24 * time.Hour)
		meetings := []Meeting{
			{from: appendDay.Add(10 * time.Hour), to: appendDay.Add(11 * time.Hour)},
			{from: appendDay.Add(4 * time.Hour), to: appendDay.Add(6 * time.Hour)},
			{from: appendDay.Add(3 * time.Hour), to: appendDay.Add(4 * time.Hour)},
		}

		err = b.AppendDay(meetings)
		require.Nil(t, err)
	})

	t.Run("with crossing events", func(t *testing.T) {
		// one week with 30min slots
		b, err := NewBusyness(7, time.Minute*30)
		require.Nil(t, err)

		appendDay := b.startDate.Add(7 * 24 * time.Hour)
		meetings := []Meeting{
			{from: appendDay.Add(10 * time.Hour), to: appendDay.Add(11 * time.Hour)},
			{from: appendDay.Add(4 * time.Hour), to: appendDay.Add(6 * time.Hour)},
			{from: appendDay.Add(3 * time.Hour), to: appendDay.Add(4 * time.Hour)},
			{from: appendDay.Add(3 * time.Hour), to: appendDay.Add(3*time.Hour + 30*time.Minute)},
		}

		err = b.AppendDay(meetings)
		require.Equal(t, "crossing events", err.Error())
	})
}

// func TestGetPossibleConfigurations(t *testing.T) {
// 	// one week with 30min slots
// 	// 48 slots in a day
// 	b, err := NewBusyness(7, time.Minute*30)
// 	require.Nil(t, err)

// 	t.Run("one meeting, next day", func(t *testing.T) {
// 		index := int(b.getBitIndex(b.startDate.Add(24*time.Hour)) / 8)
// 		info := []ByteInfo{
// 			{value: 0b00110000, startBit: 2, endBit: 3, index: index},
// 		}
// 		result := b.getPossibleConfigurations(b.startDate.Add(24*time.Hour), info)
// 		expected := []ByteInfo{
// 			{value: 0b01100000, startBit: 1, endBit: 2, index: index},
// 			{value: 0b00011000, startBit: 3, endBit: 4, index: index},
// 			{value: 0b11000000, startBit: 0, endBit: 1, index: index},
// 		}
// 		require.Equal(t, expected, result)
// 	})
// }

func TestBusyness(t *testing.T) {
	// one week with 30min slots
	b, err := NewBusyness(7, time.Minute*30)
	require.Nil(t, err)
	startDate := b.startDate

	firstMeeting := &Meeting{
		from: startDate.Add(4 * time.Hour),
		to:   startDate.Add(5 * time.Hour),
	}
	result, _, err := b.BookOrGetAvailableSlots(firstMeeting, 0)
	require.Nil(t, err)
	require.True(t, result)

	secondMeeting := &Meeting{
		from: startDate.Add(50 * time.Hour),
		to:   startDate.Add(50*time.Hour + 30*time.Minute),
	}
	result, _, err = b.BookOrGetAvailableSlots(secondMeeting, 0)
	require.Nil(t, err)
	require.True(t, result)

	everydayMeeting, err := rrule.NewRRule(rrule.ROption{
		Freq:    rrule.DAILY,
		Dtstart: startDate.Add(6 * time.Hour),
		Count:   7,
	})
	require.Nil(t, err)
	thirdMeeting := &Meeting{
		from: everydayMeeting.GetDTStart(),
		to:   startDate.Add(7 * time.Hour),
		rule: everydayMeeting,
	}
	result, _, err = b.BookOrGetAvailableSlots(thirdMeeting, 0)
	require.Nil(t, err)
	require.True(t, result)

	// crossingMeeting := &Meeting{
	// 	from: startDate.Add(30 * time.Hour),
	// 	to:   startDate.Add(30*time.Hour + 30*time.Minute),
	// }
	// result, availableSlots, err := b.BookOrGetAvailableSlots(crossingMeeting, 3)
	// require.Nil(t, err)
	// require.False(t, result)
	// expectedSlots := []Meeting{
	// 	{from: crossingMeeting.from.Add(-30 * time.Minute), to: crossingMeeting.to.Add(-30 * time.Minute)},
	// 	{from: crossingMeeting.from.Add(-2 * 30 * time.Minute), to: crossingMeeting.to.Add(-2 * 30 * time.Minute)},
	// 	{from: crossingMeeting.from.Add(2 * 30 * time.Minute), to: crossingMeeting.to.Add(2 * 30 * time.Minute)},
	// }
	// require.Equal(t, expectedSlots, availableSlots)

	busyness := b.GetBusyness(time.Hour * 24 * 7)
	expectedMeetings := []Meeting{
		{from: startDate.Add(4 * time.Hour), to: startDate.Add(5 * time.Hour)},
		{from: startDate.Add(6 * time.Hour), to: startDate.Add(7 * time.Hour)},
		{from: startDate.Add(30 * time.Hour), to: startDate.Add(31 * time.Hour)},
		{from: startDate.Add(50 * time.Hour), to: startDate.Add(50*time.Hour + 30*time.Minute)},
		{from: startDate.Add(54 * time.Hour), to: startDate.Add(55 * time.Hour)},
		{from: startDate.Add(78 * time.Hour), to: startDate.Add(79 * time.Hour)},
		{from: startDate.Add(102 * time.Hour), to: startDate.Add(103 * time.Hour)},
		{from: startDate.Add(126 * time.Hour), to: startDate.Add(127 * time.Hour)},
		{from: startDate.Add(150 * time.Hour), to: startDate.Add(151 * time.Hour)},
	}
	require.Equal(t, expectedMeetings, busyness)
}

// RRule lib: MO-SU: 0 - 6
// Golang: SU-SAT 0 - 6
func toRRuleWeekday(from time.Weekday) rrule.Weekday {
	switch from {
	case 1:
		return rrule.MO
	case 2:
		return rrule.TU
	case 3:
		return rrule.WE
	case 4:
		return rrule.TH
	case 5:
		return rrule.FR
	case 6:
		return rrule.SA
	case 0:
		return rrule.SU
	}
	return rrule.SU
}

// func TestAvailableSlots(t *testing.T) {
// 	/*
// 			0-6 6-12 12-18 18-24
// 		day1 1	1	1	0
// 		day2 1	1	0	0
// 		day3 1	0	1	0
// 		day4 1	0	0	0
// 		day5 1	0	0	0
// 		day6 1	0	0	0
// 		day7 1	0	0	0
// 	*/

// 	b := NewBusyness(7, 8, time.Hour*6)
// 	startDate := b.getStartDate()
// 	nextDay := startDate.Add(24 * time.Hour)

// 	// prepare
// 	{
// 		dailyMeetingRule, err := rrule.NewRRule(rrule.ROption{
// 			Freq:    rrule.DAILY,
// 			Dtstart: startDate,
// 			Count:   10,
// 		})
// 		require.Nil(t, err)
// 		dailyMeeting := &Meeting{
// 			from: startDate,
// 			to:   startDate.Add(6 * time.Hour),
// 			rule: dailyMeetingRule,
// 		}
// 		result, _, err := b.BookOrGetAvailableSlots(dailyMeeting, 0)
// 		require.Nil(t, err)
// 		require.True(t, result)

// 		twoDaysMeetingRule, err := rrule.NewRRule(rrule.ROption{
// 			Freq:      rrule.WEEKLY,
// 			Dtstart:   startDate,
// 			Count:     10,
// 			Byweekday: []rrule.Weekday{toRRuleWeekday(startDate.Weekday()), toRRuleWeekday(startDate.Weekday() + 1)},
// 		})
// 		require.Nil(t, err)
// 		twoDaysMeeting := &Meeting{
// 			from: startDate.Add(6 * time.Hour),
// 			to:   startDate.Add(12 * time.Hour),
// 			rule: twoDaysMeetingRule,
// 		}
// 		result, _, err = b.BookOrGetAvailableSlots(twoDaysMeeting, 0)
// 		require.Nil(t, err)
// 		require.True(t, result)

// 		monthlyMeetingRule, err := rrule.NewRRule(rrule.ROption{
// 			Freq:    rrule.MONTHLY,
// 			Dtstart: startDate,
// 			Count:   10,
// 		})
// 		require.Nil(t, err)
// 		monthlyMeeting := &Meeting{
// 			from: startDate.Add(12 * time.Hour),
// 			to:   startDate.Add(18 * time.Hour),
// 			rule: monthlyMeetingRule,
// 		}
// 		result, _, err = b.BookOrGetAvailableSlots(monthlyMeeting, 0)
// 		require.Nil(t, err)
// 		require.True(t, result)

// 		oneDayMeeting := &Meeting{
// 			from: startDate.Add(60 * time.Hour),
// 			to:   startDate.Add(66 * time.Hour),
// 		}
// 		result, _, err = b.BookOrGetAvailableSlots(oneDayMeeting, 0)
// 		require.Nil(t, err)
// 		require.True(t, result)
// 	}

// 	t.Run("try everyday meeting", func(t *testing.T) {
// 		dailyMeetingRule, err := rrule.NewRRule(rrule.ROption{
// 			Freq:    rrule.DAILY,
// 			Dtstart: nextDay.Add(6 * time.Hour),
// 			Count:   10,
// 		})
// 		require.Nil(t, err)
// 		dailyMeeting := &Meeting{
// 			from: nextDay.Add(6 * time.Hour),
// 			to:   nextDay.Add(12 * time.Hour),
// 			rule: dailyMeetingRule,
// 		}
// 		result, availableSlots, err := b.BookOrGetAvailableSlots(dailyMeeting, 5)
// 		require.Nil(t, err)
// 		require.False(t, result)

// 		expectedRule, err := rrule.NewRRule(rrule.ROption{
// 			Freq:    rrule.DAILY,
// 			Dtstart: nextDay.Add(18 * time.Hour),
// 			Count:   10,
// 		})
// 		require.Nil(t, err)
// 		expectedAvailableSlots := []Meeting{
// 			{from: nextDay.Add(18 * time.Hour), to: nextDay.Add(24 * time.Hour), rule: expectedRule},
// 		}
// 		require.Equal(t, expectedAvailableSlots, availableSlots)
// 	})

// 	t.Run("try weekly meeting", func(t *testing.T) {
// 		mRule, err := rrule.NewRRule(rrule.ROption{
// 			Freq:      rrule.WEEKLY,
// 			Dtstart:   nextDay.Add(6 * time.Hour),
// 			Count:     10,
// 			Byweekday: []rrule.Weekday{toRRuleWeekday(nextDay.Weekday())},
// 		})
// 		require.Nil(t, err)
// 		meeting := &Meeting{
// 			from: nextDay.Add(6 * time.Hour),
// 			to:   nextDay.Add(12 * time.Hour),
// 			rule: mRule,
// 		}
// 		result, availableSlots, err := b.BookOrGetAvailableSlots(meeting, 5)
// 		require.Nil(t, err)
// 		require.False(t, result)

// 		expectedRule1, err := rrule.NewRRule(rrule.ROption{
// 			Freq:      rrule.WEEKLY,
// 			Dtstart:   nextDay.Add(12 * time.Hour),
// 			Count:     10,
// 			Byweekday: []rrule.Weekday{toRRuleWeekday(nextDay.Weekday())},
// 		})
// 		require.Nil(t, err)

// 		expectedRule2, err := rrule.NewRRule(rrule.ROption{
// 			Freq:      rrule.WEEKLY,
// 			Dtstart:   nextDay.Add(18 * time.Hour),
// 			Count:     10,
// 			Byweekday: []rrule.Weekday{toRRuleWeekday(nextDay.Weekday())},
// 		})
// 		require.Nil(t, err)
// 		expectedAvailableSlots := []Meeting{
// 			{from: nextDay.Add(12 * time.Hour), to: nextDay.Add(18 * time.Hour), rule: expectedRule1},
// 			{from: nextDay.Add(18 * time.Hour), to: nextDay.Add(24 * time.Hour), rule: expectedRule2},
// 		}
// 		require.Equal(t, expectedAvailableSlots, availableSlots)
// 	})

// 	t.Run("try one-time meeting", func(t *testing.T) {
// 		oneDayMeeting := &Meeting{
// 			from: nextDay.Add((24 + 12) * time.Hour),
// 			to:   nextDay.Add((24 + 18) * time.Hour),
// 		}
// 		result, availableSlots, err := b.BookOrGetAvailableSlots(oneDayMeeting, 5)
// 		require.Nil(t, err)
// 		require.False(t, result)

// 		expectedAvailableSlots := []Meeting{
// 			{from: oneDayMeeting.from.Add(-6 * time.Hour), to: oneDayMeeting.to.Add(-6 * time.Hour)},
// 			{from: oneDayMeeting.from.Add(6 * time.Hour), to: oneDayMeeting.to.Add(6 * time.Hour)},
// 		}
// 		require.Equal(t, expectedAvailableSlots, availableSlots)
// 	})
// }
