package main

import (
	"../util"
	"errors"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"
)

func getLookupdTopics(lookupdAddresses []string) ([]string, error) {
	success := false
	allTopics := make([]string, 0)
	for _, addr := range lookupdAddresses {
		// endpoint := fmt.Sprintf("http://%s/lookup?topic=%s", addr, url.QueryEscape(q.TopicName))
		endpoint := fmt.Sprintf("http://%s/topics", addr)
		log.Printf("LOOKUPD: querying %s", endpoint)

		data, err := util.ApiRequest(endpoint)
		if err != nil {
			log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
			continue
		}
		success = true
		// do something with the data
		// {"data":{"topics":["test"]}}
		topics, _ := data.Get("topics").Array()
		allTopics = stringUnion(allTopics, topics)
	}
	if success == false {
		return nil, errors.New("unable to query any lookupd")
	}
	return allTopics, nil
}

func getLookupdProducers(lookupdAddresses []string) ([]*Producer, error) {
	success := false
	allProducers := make(map[string]*Producer, 0)
	output := make([]*Producer, 0)
	maxVersion := NewVersion("0.0.0")
	for _, addr := range lookupdAddresses {
		endpoint := fmt.Sprintf("http://%s/topology", addr)
		log.Printf("LOOKUPD: querying %s", endpoint)

		data, err := util.ApiRequest(endpoint)
		if err != nil {
			log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
			continue
		}
		success = true

		producers := data.Get("producers")
		producersArray, _ := producers.Array()
		for i, _ := range producersArray {
			producer := producers.GetIndex(i)
			address := producer.Get("address").MustString()
			httpPort := producer.Get("http_port").MustInt()
			tcpPort := producer.Get("tcp_port").MustInt()
			key := fmt.Sprintf("%s:%d:%d", address, httpPort, tcpPort)
			_, ok := allProducers[key]
			if !ok {
				topicList, _ := producer.Get("topics").Array()
				var topics []string
				for _, t := range topicList {
					topics = append(topics, t.(string))
				}
				version := producer.Get("version").MustString("unknown")
				versionObj := NewVersion(version)
				if !maxVersion.Less(versionObj) {
					maxVersion = versionObj
				}
				p := &Producer{
					Address:    address,
					TcpPort:    tcpPort,
					HttpPort:   httpPort,
					Version:    version,
					VersionObj: versionObj,
					Topics:     topics,
				}
				allProducers[key] = p
				output = append(output, p)
			}
		}
	}
	for _, producer := range allProducers {
		if maxVersion.Less(producer.VersionObj) {
			producer.OutOfDate = true
		}
	}
	if success == false {
		return nil, errors.New("unable to query any lookupd")
	}
	return output, nil
}

func getLookupdTopicProducers(topic string, lookupdAddresses []string) ([]string, error) {
	success := false
	allSources := make([]string, 0)
	for _, addr := range lookupdAddresses {
		endpoint := fmt.Sprintf("http://%s/lookup?topic=%s", addr, url.QueryEscape(topic))
		log.Printf("LOOKUPD: querying %s", endpoint)

		data, err := util.ApiRequest(endpoint)
		if err != nil {
			log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
			continue
		}
		success = true

		producers, _ := data.Get("producers").Array()
		for _, producer := range producers {
			producer := producer.(map[string]interface{})
			address := producer["address"].(string)
			port := int(producer["http_port"].(float64))
			key := fmt.Sprintf("%s:%d", address, port)
			allSources = stringAdd(allSources, key)
		}
	}
	if success == false {
		return nil, errors.New("unable to query any lookupd")
	}
	return allSources, nil
}

func getNSQDStats(nsqdAddresses []string, selectedTopic string) ([]TopicHostStats, map[string]*ChannelStats, error) {
	topicHostStats := make([]TopicHostStats, 0)
	channelStats := make(map[string]*ChannelStats)
	success := false
	for _, addr := range nsqdAddresses {
		endpoint := fmt.Sprintf("http://%s/stats?format=json", addr)
		log.Printf("NSQD: querying %s", endpoint)

		data, err := util.ApiRequest(endpoint)
		if err != nil {
			log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
			continue
		}
		success = true
		topics, _ := data.Get("topics").Array()
		for _, topicInfo := range topics {
			topicInfo := topicInfo.(map[string]interface{})
			topicName := topicInfo["topic_name"].(string)
			if topicName != selectedTopic {
				continue
			}
			depth := int64(topicInfo["depth"].(float64))
			backendDepth := int64(topicInfo["backend_depth"].(float64))
			h := TopicHostStats{
				HostAddress:  addr,
				Depth:        depth,
				BackendDepth: backendDepth,
				MemoryDepth:  depth - backendDepth,
				MessageCount: int64(topicInfo["message_count"].(float64)),
				ChannelCount: len(topicInfo["channels"].([]interface{})),
			}
			topicHostStats = append(topicHostStats, h)

			channels := topicInfo["channels"].([]interface{})
			for _, c := range channels {
				c := c.(map[string]interface{})
				channelName := c["channel_name"].(string)
				channel, ok := channelStats[channelName]
				if !ok {
					channel = &ChannelStats{ChannelName: channelName, Topic: selectedTopic}
					channelStats[channelName] = channel
				}
				h := &ChannelStats{HostAddress: addr, ChannelName: channelName, Topic: selectedTopic}
				depth := int64(c["depth"].(float64))
				backendDepth := int64(c["backend_depth"].(float64))
				h.Depth = depth
				h.BackendDepth = backendDepth
				h.MemoryDepth = depth - backendDepth
				h.InFlightCount = int64(c["in_flight_count"].(float64))
				h.DeferredCount = int64(c["deferred_count"].(float64))
				h.MessageCount = int64(c["message_count"].(float64))
				h.RequeueCount = int64(c["requeue_count"].(float64))
				h.TimeoutCount = int64(c["timeout_count"].(float64))
				clients := c["clients"].([]interface{})
				// TODO: this is sort of wrong; client's should be de-duped
				// client A that connects to NSQD-a and NSQD-b should only be counted once. right?
				h.ClientCount = len(clients)
				channel.AddHostStats(h)

				// "clients": [
				//   {
				//     "version": "V2",
				//     "remote_address": "127.0.0.1:49700",
				//     "name": "jehiah-air",
				//     "state": 3,
				//     "ready_count": 1000,
				//     "in_flight_count": 0,
				//     "message_count": 0,
				//     "finish_count": 0,
				//     "requeue_count": 0,
				//     "connect_ts": 1347150965
				//   }
				// ]
				for _, client := range clients {
					client := client.(map[string]interface{})
					connected := time.Unix(int64(client["connect_ts"].(float64)), 0)
					connectedDuration := time.Now().Sub(connected).Seconds()
					clientInfo := ClientInfo{
						HostAddress:       addr,
						ClientVersion:     client["version"].(string),
						ClientIdentifier:  fmt.Sprintf("%s:%s", client["name"].(string), strings.Split(client["remote_address"].(string), ":")[1]),
						ConnectedDuration: time.Duration(int64(connectedDuration)) * time.Second, // truncate to second
						InFlightCount:     int(client["in_flight_count"].(float64)),
						ReadyCount:        int(client["ready_count"].(float64)),
						FinishCount:       int64(client["finish_count"].(float64)),
						RequeueCount:      int64(client["requeue_count"].(float64)),
						MessageCount:      int64(client["message_count"].(float64)),
					}
					channel.Clients = append(channel.Clients, clientInfo)
				}
			}
		}
	}
	if success == false {
		return nil, nil, errors.New("unable to query any lookupd")
	}
	return topicHostStats, channelStats, nil

}

func stringAdd(s []string, a string) []string {
	o := s
	found := false
	for _, existing := range s {
		if a == existing {
			found = true
			return s
		}
	}
	if found == false {
		o = append(o, a)
	}
	return o

}

func stringUnion(s []string, a []interface{}) []string {
	o := s
	for _, entry := range a {
		found := false
		for _, existing := range s {
			if entry.(string) == existing {
				found = true
				break
			}
		}
		if found == false {
			o = append(o, entry.(string))
		}
	}
	return o
}
