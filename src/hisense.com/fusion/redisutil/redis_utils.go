package redisutil

import (
	"fmt"
	"time"

	log "hisense.com/fusion/logger"

	//"github.com/siddontang/go-log/log"
	"github.com/garyburd/redigo/redis"
)

// RedisConfig  redis 相关配置
type RedisConfig struct {
	//Addr reids host:port
	Addr string `toml:"addr"`
	//DB use db
	DB string `toml:"db"`
	// Password auth need
	Password string `toml:"password"`
	// MaxActive max active connection
	MaxActive int `toml:"max_active"`
	// MaxIdle max idle connection
	MaxIdle int `toml:"max_idle"`
	// ConnLifeTime
	ConnLifeTime int `toml:"conn_life_time"`
	// IdleTimeout
	IdleTimeout    time.Duration `toml:"idle_timeout"`
	ConnectTimeout time.Duration `toml:"connect_timeout"`
	ReadTimeout    time.Duration `toml:"read_timeout"`
	WriteTimeout   time.Duration `toml:"write_timeout"`
	// TLSEnable AWS ElastiCache若开启密码访问 则需要配置为true
	TLSEnable bool `toml:"tls_enable"`
	// TLSSkipVerify AWS ElastiCache若开启密码访问 可以选择是否跳过证书验证，默认true
	TLSSkipVerify bool `toml:"tls_skip_verify"`
}

var (
	cfg *RedisConfig
	// Pool redis 连接池， 直接从连接池中获取可用连接
	Pool *redis.Pool
)

func newPool() *redis.Pool {
	addr := cfg.Addr
	password := cfg.Password
	db := cfg.DB
        idleTimeout := cfg.IdleTimeout
	connectTimeout := cfg.ConnectTimeout
	readTimeout := cfg.ReadTimeout
	writeTimeout := cfg.WriteTimeout
	return &redis.Pool{
		MaxIdle:         cfg.MaxIdle,
		MaxActive:       cfg.MaxActive,
		IdleTimeout:     idleTimeout * time.Second,
		MaxConnLifetime: time.Duration(cfg.ConnLifeTime) * time.Second, //设置为0，则关闭连接过期清理的功能
		Dial: func() (redis.Conn, error) {
			var c redis.Conn
			var err error
			if cfg.TLSEnable {
				c, err = redis.Dial("tcp", addr,
					redis.DialConnectTimeout(connectTimeout*time.Second),
					redis.DialReadTimeout(readTimeout*time.Second),
					redis.DialWriteTimeout(writeTimeout*time.Second),
					redis.DialUseTLS(true),
					redis.DialTLSSkipVerify(cfg.TLSSkipVerify))
			} else {
				c, err = redis.DialTimeout("tcp", addr, connectTimeout*time.Second, readTimeout*time.Second, writeTimeout*time.Second)
			}
			if err != nil {
				return nil, err
			}
			if len(password) > 0 {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}
			if _, err := c.Do("SELECT", db); err != nil {
				c.Close()
				return nil, err
			}
			return c, nil
		},
	}
}

// NewRedisPool 初始化redis 连接池
func NewRedisPool(_cfg *RedisConfig) error {
	cfg = _cfg
	Pool = newPool()
	if Pool == nil {
		log.Logger.Errorf("init redis pool failed !!!")
		return fmt.Errorf("init redis pool failed")
	}
	return nil
}
