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
	logger                 hclog.Logger
	DeviceCustomerMap      map[string]models.Device
	FeatureCustomerMap     map[string]models.Feature
	InputParamCustomerMap  map[string]models.Param
	OutputParamCustomerMap map[string]models.Param
	CustomerDeviceMap      map[string]models.Device
	CustomerFeatuerMap     map[string]models.Feature
	CustomerInputParamMap  map[string]models.Param
	CustomerOutputParamMap map[string]models.Param
}

// ConvertReportMessage2Devices converts data report request to protocol that device understands for each device of this device model,
// func (c *Converter) ConvertReportMessage2Devices(modelId, featureId string) ([]string, error) {
// 	// TODO: concrete implement
// 	return []string{"Have a good try!!!"}, nil
// }

// ConvertIssueMessage2Device converts issue request to protocol that device understands, which has four return parameters:
// 1. inputMessages: device issue protocols for each of command input param.
// 2. outputMessages: device data report protocols for each of command output param.
// 3. issueTopic: device issue MQTT topic for input params.
// 4. issueResponseTopic: device issue response MQ topic for output params.
func (c *Converter) ConvertIssueMessage2Device(deviceId, modelId, featureId string, values map[string]string, convertedDeviceFeatureMap string) ([]string, []string, string, string, error) {
	// TODO: concrete implement
	return nil, nil, "", "", nil
}

// ConvertDeviceMessages2MQFormat receives device command issue responses and converts it to RabbitMQ normative format.
func (c *Converter) ConvertDeviceMessages2MQFormat(messages []string, convertedDeviceFeatureMapStr string) (string, []byte, error) {
	convertedDeviceFeatureMap := models.DeviceFeatureMap{}
	if err := yaml.Unmarshal([]byte(convertedDeviceFeatureMapStr), &convertedDeviceFeatureMap); err != nil {
		c.logger.Info("Unmarshal convertedDeviceFeatureMap error: %s\n", err.Error())
		return "", nil, err
	}
	for _, msg := range messages {
		var featureType, deviceModelId, deviceId, featureId string
		c.logger.Info("msg error: %s\n", string(msg))
		data := Data{}
		if err := json.Unmarshal([]byte(msg), &data); err != nil {
			return "", nil, err
		}
		for _, v := range convertedDeviceFeatureMap.DeviceIdMap {
			if v.CustomDeviceId == data.Device {
				deviceModelId = v.ModelId
				deviceId = v.DeviceId
			}
		}
		for _, v := range convertedDeviceFeatureMap.FeatureIdMap {
			if v.CustomFeatureId == data.Type && v.FeatureType == "property" {
				featureType = "property"
				featureId = v.FeatureId
			}
			if v.CustomFeatureId == data.Type && v.FeatureType == "alarm" {
				featureType = "alarm"
				featureId = v.FeatureId
			}
		}
		if featureType == "" || deviceModelId == "" || deviceId == "" || featureId == "" {
			return "", nil, fmt.Errorf("Params is nil")
		}
		if featureType == "property" {
			rk := fmt.Sprintf("thing.report.%s.%s.property", deviceModelId, deviceId)
			bytes, err := json.Marshal(&ds.RabbitMQMsg{
				MsgId:     uuid.New().String(),
				Version:   "1.0.0",
				Topic:     rk,
				Timestamp: time.Now(),
				Params: []ds.Param{
					{
						Id:        featureId,
						Value:     data.Value,
						Timestamp: time.Now(),
					},
				},
			})
			if err != nil {
				return "", nil, err
			}
			return rk, bytes, nil
		} else {
			rk := fmt.Sprintf("thing.report.%s.%s.alarm", deviceModelId, deviceId)
			bytes, err := json.Marshal(&ds.RabbitMQMsg{
				MsgId:     uuid.New().String(),
				Version:   "1.0.0",
				Topic:     rk,
				Timestamp: time.Now(),
				Params: []ds.Param{
					{
						Id:        featureId,
						Value:     data.Value,
						Timestamp: time.Now(),
					},
				},
			})
			if err != nil {
				return "", nil, err
			}
			return rk, bytes, nil
		}
	}
	return "", nil, nil
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
