package consumer

import (
	"time"

	"hisense.com/fusion/config"
	"hisense.com/fusion/consumer/kafka"
	"hisense.com/fusion/consumer/redis"
	"hisense.com/fusion/consumer/utility"
	log "hisense.com/fusion/logger"
)

// ConsumeData 消费mysql 的变更
func ConsumeData(interval time.Duration) {
	cfg := config.ConfigInfo
	to := time.NewTimer(interval * time.Second)
	if cfg.KafkaConfig.NeedKafka != 0 {
		err := kafka.InitKafkaProducer(cfg.KafkaConfig)
		if err != nil {
			log.Logger.Fatalf("init kafka producer failed, err = %v", err)
		}
	}
	for {
		to.Reset(interval * time.Second)
		select {
		case data := <-utility.ConsumerDataChan:
			if cfg.KafkaConfig.NeedKafka == 0 {
				log.Logger.Debugf("consume Data to redis, data = %+v ", data)
				utility.KeepPos = false
				redis.FusionProcessing(data)
			} else if cfg.KafkaConfig.NeedKafka == 1 {
				log.Logger.Debugf("consume Data to kakfa, data = %+v ", data)
				utility.KeepPos = false
				kafka.FusionProcessing(cfg.KafkaConfig, data)
			} else if cfg.KafkaConfig.NeedKafka == 2 {
				log.Logger.Debugf("consume Data to redis and kakfa, data = %+v ", data)
				utility.KeepPos = false
				redis.FusionProcessing(data)
				kafka.FusionProcessing(cfg.KafkaConfig, data)
			}

		case <-to.C:
			utility.KeepPos = true
		}
	}
}
