package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"sync"
)

func main() {
	var wg sync.WaitGroup
	consumer, err := sarama.NewConsumer([]string{"81.68.93.154:9092"}, nil)
	if err != nil {
		fmt.Printf("fail to start consumer, err:%v\n", err)
		return
	}
	// 根据topic取到所有的分区
	partitionList, err := consumer.Partitions("web")

	if err != nil {
		fmt.Printf("fail to get list of partition:err%v\n", err)
		return
	}
	fmt.Println(partitionList)
	for partition := range partitionList {
		fmt.Println("进入分区")
		// 遍历所有的分区
		// 针对每个分区创建一个对应的分区消费者
		pc, err := consumer.ConsumePartition("web", int32(partition),
			sarama.OffsetNewest)
		if err != nil {
			fmt.Printf("failed to start consumer for partition %d,err:%v\n",
				partition, err)
			return
		}
		defer pc.AsyncClose()
		fmt.Println("wait")
		wg.Add(1)
		// 异步从每个分区消费信息
		go func(sarama.PartitionConsumer) {
			// 阻塞直到有值发送过来，然后再继续等待
			for msg := range pc.Messages() {
				fmt.Printf("Partition:%d Offset:%d Key:%v Value:%v\n",
					msg.Partition, msg.Offset, msg.Key, msg.Value)
			}
			wg.Done()
		}(pc)
	}
	wg.Wait()
}
