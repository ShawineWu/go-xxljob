package ratelog

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLimiter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Scenario: events arrive faster than the interval, then after it, then after a Reset
		// Expect: the first passes, the next ones are counted as suppressed until the interval has passed; Reset lets
		//         the next one through at once
		l := New(time.Minute)

		first, _ := l.Allow()
		second, _ := l.Allow()
		third, _ := l.Allow()
		time.Sleep(time.Minute)
		later, suppressed := l.Allow()
		l.Reset()
		afterReset, _ := l.Allow()

		assert.True(t, first)
		assert.False(t, second)
		assert.False(t, third)
		assert.True(t, later)
		assert.Equal(t, 2, suppressed)
		assert.True(t, afterReset)
	})
}
