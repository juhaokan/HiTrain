package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	log "hisense.com/fusion/logger"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
)

func newConfigWithFile(name string) (*FusionConfig, error) {
	data, err := ioutil.ReadFile(name)
	if err != nil {
		log.Logger.Errorf("new config with file failed, err = %v", err)
		return nil, errors.Trace(err)
	}

	return newConfig(string(data))
}

func newConfig(data string) (*FusionConfig, error) {
	var c FusionConfig
	_, err := toml.Decode(data, &c)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &c, nil
}

// InitCfgUseFile 加载配置文件
func InitCfgUseFile() *FusionConfig {
	fileName := ConfigFile
	log.Logger.Infof(" initCfgUseFile filename = %s", fileName)
	cfg, err := newConfigWithFile(fileName)
	if err != nil {
		log.Logger.Errorf("new config failed %v", err)
		return nil
	}
	cfg.CanalConfig.Dump = *cfg.DumpConfig
	log.Logger.Infof("get config file data = %s", PrintConf(cfg))
	cfg.CanalConfig.HeartbeatPeriod = cfg.CanalConfig.HeartbeatPeriod * time.Second
	cfg.CanalConfig.ReadTimeout = cfg.CanalConfig.ReadTimeout * time.Second
	ConfigInfo = cfg
	return cfg
}

// PrintConf 打印配置文件
func PrintConf(conf *FusionConfig) string {
	b, err := json.Marshal(*conf)
	if err != nil {
		return fmt.Sprintf("%+v", *conf)
	}
	var out bytes.Buffer
	err = json.Indent(&out, b, "", "    ")
	if err != nil {
		return fmt.Sprintf("%+v", *conf)
	}
	return out.String()

}
