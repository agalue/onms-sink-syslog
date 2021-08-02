package main

import (
	"context"
	"encoding/xml"
	"flag"
	"log"
	"os"
	"os/signal"
	"regexp"

	"github.com/Shopify/sarama"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/agalue/onms-sink-syslog/model"
	"github.com/agalue/onms-sink-syslog/protobuf/sink"

	"google.golang.org/protobuf/proto"
)

var kafkaServer, consumerGroup, syslogTopic, srcMatch, msgMatch, minionMatch string

var srcRE, msgRE *regexp.Regexp
var msgBuffer = make(map[string][]byte)
var chunkTracker = make(map[string]int32)

func main() {
	log.SetOutput(os.Stdout)
	flag.StringVar(&kafkaServer, "server", "localhost:9092", "Kafka bootstrap server")
	flag.StringVar(&syslogTopic, "topic", "OpenNMS.Sink.Syslog", "Kafka Topic for Syslog via OpenNMS Sink API")
	flag.StringVar(&consumerGroup, "consumer-group", "onms-sink-syslog", "Kafka consumer group name")
	flag.StringVar(&srcMatch, "src-match", "", "Regex to match IP address of the Syslog producer")
	flag.StringVar(&msgMatch, "msg-match", "", "Regex to match content from the Syslog message")
	flag.StringVar(&minionMatch, "minion-match", "", "Minion ID to match as the source for the Syslog messages")
	flag.Parse()

	if srcMatch != "" {
		srcRE = regexp.MustCompile(srcMatch)
	}

	if msgMatch != "" {
		msgRE = regexp.MustCompile(msgMatch)
	}

	ctx, cancel := context.WithCancel(context.Background())
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	defer func() {
		signal.Stop(signalChan)
	}()
	go func() {
		select {
		case <-signalChan:
			cancel()
		case <-ctx.Done():
		}
	}()

	log.Printf("Consuming syslog messages from %s through Sink Topic %s", kafkaServer, syslogTopic)

	config := kafka.DefaultSaramaSubscriberConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	subscriber, err := kafka.NewSubscriber(
		kafka.SubscriberConfig{
			Brokers:               []string{kafkaServer},
			Unmarshaler:           kafka.DefaultMarshaler{},
			OverwriteSaramaConfig: config,
			ConsumerGroup:         consumerGroup,
		},
		watermill.NewStdLogger(false, false),
	)
	if err != nil {
		log.Panicln(err)
	}

	msgChannel, err := subscriber.Subscribe(ctx, syslogTopic)
	if err != nil {
		log.Panicln(err)
	}

	process(msgChannel)
}

func process(msgChannel <-chan *message.Message) {
	for msg := range msgChannel {
		sinkMsg := new(sink.SinkMessage)
		if err := proto.Unmarshal(msg.Payload, sinkMsg); err != nil {
			log.Printf("[warning] invalid sink message received: %v", err)
			continue
		}
		if data := processMessage(sinkMsg); data != nil {
			logMsg := new(model.SyslogMessageLogDTO)
			if err := xml.Unmarshal(data, logMsg); err != nil {
				log.Printf("[warning] invalid syslog message received: %v", err)
				continue
			}
			if minionMatch != "" && minionMatch != logMsg.SystemID {
				continue
			}
			if srcRE != nil && !srcRE.MatchString(logMsg.SourceAddress) {
				continue
			}
			for _, msg := range logMsg.Messages {
				txt := msg.GetContent()
				if msgRE == nil || msgRE.MatchString(txt) {
					log.Printf("received syslog message from %s through minion %s: %s", logMsg.SourceAddress, logMsg.SystemID, txt)
				}
			}
		}
		msg.Ack()
	}
}

func processMessage(msg *sink.SinkMessage) []byte {
	id := msg.GetMessageId()
	chunk := msg.GetCurrentChunkNumber() + 1 // Chunks starts at 0
	if chunk != msg.GetTotalChunks() {
		if chunkTracker[id] < chunk {
			msgBuffer[id] = append(msgBuffer[id], msg.GetContent()...)
			chunkTracker[id] = chunk
		}
		return nil
	}
	// Retrieve the complete message from the buffer
	var data []byte
	if msg.GetTotalChunks() == 1 { // Handle special case chunk == total == 1
		data = msg.GetContent()
	} else {
		data = append(msgBuffer[id], msg.GetContent()...)
		delete(msgBuffer, id)
		delete(chunkTracker, id)
	}
	return data
}
