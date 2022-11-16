/*
Copyright 2022 The BeeThings Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/beeedge/beethings/pkg/device-access/rest/models"
	ds "github.com/beeedge/beethings/pkg/device-storage/rest/models"
	"github.com/beeedge/beethings/pkg/util"
	"github.com/beeedge/device-plugin-framework/shared"
	"github.com/google/uuid"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"gopkg.in/yaml.v2"
)

type Data struct {
	Device string `json:"device"`
	Type   string `json:"type"`
	Value  string `json:"value"`
}

type Command struct {
	Device string  `json:"device"`
	Params []Param `json:"params"`
}

type Param struct {
	Id    string `json:"id"`
	Value string `json:"value"`
}

// Here is a real implementation of device-plugin.
type Converter struct {
	logger hclog.Logger
}

// ConvertIssueMessage2Device converts issue request to protocol that device understands, which has four return parameters:
// 1. inputMessages: device issue protocols for each of command input param.
// 2. outputMessages: device data report protocols for each of command output param.
// 3. issueTopic: device issue MQTT topic for input params.
// 4. issueResponseTopic: device issue response MQ topic for output params.
func (c *Converter) ConvertIssueMessage2Device(deviceId, modelId, featureId string, values map[string]string, convertedDeviceFeatureMap string) ([]string, []string, string, string, error) {
	var (
		device           *models.Device
		deviceFeatureMap models.DeviceFeatureMap
	)
	if err := yaml.Unmarshal([]byte(convertedDeviceFeatureMap), &deviceFeatureMap); err != nil {
		c.logger.Info("Unmarshal convertedDeviceFeatureMap error: %s\n", err.Error())
		return nil, nil, "", "", err
	}
	if d, ok := deviceFeatureMap.DeviceIdMap[deviceId]; ok {
		device = d
	} else {
		return nil, nil, "", "", fmt.Errorf("No match device.")
	}
	if f, ok := deviceFeatureMap.FeatureIdMap[featureId]; ok {
		switch f.FeatureType {
		case "property":
			bytes, err := json.Marshal(Data{
				Device: device.CustomDeviceId,
				Type:   f.CustomFeatureId,
				Value:  values[f.FeatureId],
			})
			if err != nil {
				c.logger.Info("property:marshal device property issue data err:", err)
				return nil, nil, "", "", err
			}
			return []string{string(bytes)}, nil, device.IssueTopic, "", nil
		case "command":
			var inputParams []Param
			for _, in := range f.InputParams {
				inputParams = append(inputParams, Param{
					Id:    in.CustomParamId,
					Value: values[in.Id],
				})
			}
			bytes, err := json.Marshal(Command{
				Device: device.CustomDeviceId,
				Params: inputParams,
			})
			if err != nil {
				c.logger.Info("command:marshal device command issue err:", err)
				return nil, nil, "", "", err
			}
			outputParams := []string{}
			for _, out := range f.OutputParams {
				outputParams = append(outputParams, out.Id)
			}
			return []string{string(bytes)}, outputParams, device.IssueTopic, device.IssueResponseTopic, nil
		}
	} else {
		return nil, nil, "", "", fmt.Errorf("No match features.")
	}
	return nil, nil, "", "", nil
}

// ConvertDeviceMessages2MQFormat receives device command issue responses and converts it to RabbitMQ normative format.
func (c *Converter) ConvertDeviceMessages2MQFormat(messages []string, convertedDeviceFeatureMap string) (string, []byte, error) {
	var (
		device           *models.Device
		rabbitMQParams   []ds.Param
		deviceFeatureMap models.DeviceFeatureMap
	)
	if err := yaml.Unmarshal([]byte(convertedDeviceFeatureMap), &deviceFeatureMap); err != nil {
		c.logger.Info("Unmarshal convertedDeviceFeatureMap error: %s\n", err.Error())
		return "", nil, err
	}
	for _, msg := range messages {
		var data Data
		if err := json.Unmarshal([]byte(msg), &data); err != nil {
			c.logger.Info("unmarshal data msg error: %s\n", err)
			return "", nil, err
		}
		// Find the desired device.
		for k, v := range deviceFeatureMap.DeviceIdMap {
			if v.CustomDeviceId == data.Device {
				device = deviceFeatureMap.DeviceIdMap[k]
			}
		}
		if device == nil {
			return "", nil, fmt.Errorf("No matching device.")
		}
		// Find the desired feature.
		for k, v := range deviceFeatureMap.FeatureIdMap {
			if v.CustomFeatureId == data.Type {
				var rk string
				switch v.FeatureType {
				case "property":
					rk = fmt.Sprintf("thing.report.%s.%s.property", device.ModelId, device.DeviceId)
				case "alarm":
					rk = fmt.Sprintf("thing.report.%s.%s.alarm", device.ModelId, device.DeviceId)
				}
				bytes, err := json.Marshal(&ds.RabbitMQMsg{
					MsgId:     uuid.New().String(),
					Version:   util.Version,
					Topic:     rk,
					Timestamp: time.Now(),
					Params: []ds.Param{
						{
							Id:    deviceFeatureMap.FeatureIdMap[k].FeatureId,
							Value: data.Value,
						},
					},
				})
				if err != nil {
					c.logger.Info("Marshal rabbitmq msg error: %s\n", err)
					return "", nil, err
				}
				return rk, bytes, nil
			}
		}
		// Not feature but command.
		for k, v := range deviceFeatureMap.OutputParamIdMap {
			if v.CustomParamId == data.Type {
				rabbitMQParams = append(rabbitMQParams, ds.Param{
					Id:    deviceFeatureMap.OutputParamIdMap[k].Id,
					Value: data.Value,
				})
			}
		}
	}
	rk := fmt.Sprintf("thing.issue_reply.%s.%s.command", device.ModelId, device.DeviceId)
	bytes, err := json.Marshal(&ds.RabbitMQMsg{
		Topic:  rk,
		Params: rabbitMQParams,
	})
	if err != nil {
		c.logger.Info("issue_reply marsh rabbitmq message,err:", err)
		return "", nil, err
	}
	return rk, bytes, nil
}

func main() {
	logger := hclog.New(&hclog.LoggerOptions{
		Level:      hclog.Trace,
		Output:     os.Stderr,
		JSONFormat: true,
	})
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: shared.Handshake,
		Plugins: map[string]plugin.Plugin{
			"converter": &shared.ConverterPlugin{Impl: &Converter{
				logger: logger,
			}},
		},
		// A non-nil value here enables gRPC serving for this plugin...
		GRPCServer: plugin.DefaultGRPCServer,
	})
}
