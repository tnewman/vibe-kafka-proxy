package kafka

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// kgoLogger adapts a zap.Logger to the kgo.Logger interface.
type kgoLogger struct {
	*zap.Logger
}

// Level maps zapcore.Level to kgo.LogLevel.
func (l *kgoLogger) Level() kgo.LogLevel {
	// A direct mapping is not always possible, so we'll map Zap's minimum level
	// to the corresponding kgo level.
	// For simplicity, we'll assume the zap logger is configured for at least info level.
	// We can refine this by checking l.Logger.Core().Enabled(level) if needed.
	switch {
	case l.Logger.Core().Enabled(zapcore.DebugLevel):
		return kgo.LogLevelDebug
	case l.Logger.Core().Enabled(zapcore.InfoLevel):
		return kgo.LogLevelInfo
	case l.Logger.Core().Enabled(zapcore.WarnLevel):
		return kgo.LogLevelWarn
	case l.Logger.Core().Enabled(zapcore.ErrorLevel):
		return kgo.LogLevelError
	default:
		return kgo.LogLevelNone // Don't log if level is higher than any kgo level
	}
}

// Log logs a message with the given level and key-value pairs.
func (l *kgoLogger) Log(level kgo.LogLevel, msg string, keyvals ...interface{}) {
	// Convert keyvals to zap.Field
	zapFields := make([]zap.Field, 0, len(keyvals)/2)
	for i := 0; i < len(keyvals); i += 2 {
		if i+1 < len(keyvals) {
			zapFields = append(zapFields, zap.Any(fmt.Sprintf("%v", keyvals[i]), keyvals[i+1]))
		} else {
			// Handle cases where a key might not have a corresponding value
			zapFields = append(zapFields, zap.Any(fmt.Sprintf("%v", keyvals[i]), "MISSING_VALUE"))
		}
	}

	switch level {
	case kgo.LogLevelDebug:
		l.Logger.Debug(msg, zapFields...)
	case kgo.LogLevelInfo:
		l.Logger.Info(msg, zapFields...)
	case kgo.LogLevelWarn:
		l.Logger.Warn(msg, zapFields...)
	case kgo.LogLevelError:
		l.Logger.Error(msg, zapFields...)
	case kgo.LogLevelNone:
		// Do nothing
	default:
		l.Logger.Info(msg, zapFields...) // Default to Info
	}
}
