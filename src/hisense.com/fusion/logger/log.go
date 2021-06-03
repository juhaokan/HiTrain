package logger

import (
	"fmt"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

//Logger logger 定义
var Logger *zap.SugaredLogger

// LogLevel log level 定义
var LogLevel = zap.NewAtomicLevel()

// InitLogger 初始化logger
func InitLogger(logPath string, logLevel string) error {
	switch logLevel {
	case "debug":
		LogLevel.SetLevel(zap.DebugLevel)
	case "info":
		LogLevel.SetLevel(zap.InfoLevel)
	case "error":
		LogLevel.SetLevel(zap.ErrorLevel)
	case "warn":
		LogLevel.SetLevel(zap.WarnLevel)
	default:
		fmt.Printf("log level should be debug, info, error, warn, default is info")
		LogLevel.SetLevel(zap.InfoLevel)
	}
	encoderCfg := zapcore.EncoderConfig{
		// Keys can be anything except the empty string.
		TimeKey:        "T",
		LevelKey:       "L",
		NameKey:        "N",
		CallerKey:      "C",
		MessageKey:     "M",
		StacktraceKey:  "S",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     TimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	// LogConfig = zap.Config{
	//     Level:            zap.NewAtomicLevelAt(lv),
	//     Development:      false,
	//     DisableStacktrace: true,
	//     Encoding:         "console",
	//     EncoderConfig:    encoder_cfg,
	//     OutputPaths:      []string{"stderr", logPath},
	//     ErrorOutputPaths: []string{"stderr"},
	// }

	errSink := zapcore.AddSync(&lumberjack.Logger{
		Filename:   logPath,
		MaxSize:    10, // megabytes
		MaxBackups: 1000,
		MaxAge:     28, // days
	})

	infocore := zapcore.NewCore(
		zapcore.NewConsoleEncoder(encoderCfg),
		errSink,
		LogLevel,
	)
	op := zap.AddCaller()
	newLogger := zap.New(infocore, op)
	defer newLogger.Sync()
	Logger = newLogger.Sugar()
	return nil
}

// TimeEncoder 设置日志格式
func TimeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString("[" + t.Format("2006-01-02 15:04:05.000") + "]")
}

// IncLogLevel 增加日志等级
func IncLogLevel() {
	level := LogLevel.Level()
	if level >= zap.FatalLevel {
		fmt.Printf("log level to %v \n", level)
		return
	}
	LogLevel.SetLevel(level + 1)
	fmt.Printf("increase log level to %v \n", LogLevel.Level())
}

// DecLogLevel 降低日志等级
func DecLogLevel() {
	level := LogLevel.Level()
	if level <= zap.DebugLevel {
		fmt.Printf("log level to %v \n", level)
		return
	}
	LogLevel.SetLevel(level - 1)
	fmt.Printf("decrease log level to %v \n", LogLevel.Level())
}

// SetLogLevel 设置日志等级
func SetLogLevel(logLevel string) error {
	switch logLevel {
	case "debug":
		LogLevel.SetLevel(zap.DebugLevel)
	case "info":
		LogLevel.SetLevel(zap.InfoLevel)
	case "error":
		LogLevel.SetLevel(zap.ErrorLevel)
	case "warn":
		LogLevel.SetLevel(zap.WarnLevel)
	default:
		fmt.Printf("log level should be debug, info, error, warn,   exit!")
		LogLevel.SetLevel(zap.InfoLevel)
		return fmt.Errorf("log level should be debug, info, error, warn,   exit!")
	}
	fmt.Printf("set log level to %v \n", LogLevel.Level())
	return nil
}
