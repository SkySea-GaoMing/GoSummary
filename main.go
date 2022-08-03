package main

import (
	"fmt"
	"github.com/Shopify/sarama"
)

func main() {
	config := sarama.NewConfig()
	// 发送完数据需要leader和follow都确认
	config.Producer.RequiredAcks = sarama.NoResponse
	// 写到随机分区中，我们默认设置32个分区

	config.Producer.Partitioner = sarama.NewManualPartitioner // 人工指定分区

	// 成功交付的消息将在success channel返回
	config.Producer.Return.Successes = true

	// 构造一个消息
	msg := &sarama.ProducerMessage{}
	msg.Topic = "web"
	msg.Value = sarama.StringEncoder("send a task")
	msg.Partition = 0

	// 连接kafka
	client, err := sarama.NewSyncProducer([]string{"81.68.93.154:9092"}, config)
	if err != nil {
		fmt.Println("producer closed, err:", err)
		return
	}
	defer client.Close()
	fmt.Println("连接成功")

	// 发送消息，获取分区信息
	pid, offset, err := client.SendMessage(msg)
	if err != nil {
		fmt.Println("send msg failed, err:", err)
		return
	}
	fmt.Printf("pid:%v offset:%v\n", pid, offset)
}
