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
	"io/ioutil"
	"os"
	"time"

	"github.com/beeedge/beethings/pkg/device-access/rest/models"
	ds "github.com/beeedge/beethings/pkg/device-storage/rest/models"
	"github.com/beeedge/device-plugin-framework/shared"
	"github.com/google/uuid"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"gopkg.in/yaml.v2"
	"k8s.io/klog/v2"
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
func (c *Converter) ConvertReportMessage2Devices(modelId, featureId string) ([]string, error) {
	// TODO: concrete implement
	return []string{"Have a good try!!!"}, nil
}

// ConvertIssueMessage2Device converts issue request to protocol that device understands, which has four return parameters:
// 1. inputMessages: device issue protocols for each of command input param.
// 2. outputMessages: device data report protocols for each of command output param.
// 3. issueTopic: device issue MQTT topic for input params.
// 4. issueResponseTopic: device issue response MQ topic for output params.
func (c *Converter) ConvertIssueMessage2Device(deviceId, modelId, featureId string, values map[string]string) ([]string, []string, string, string, error) {
	// TODO: concrete implement
	var device models.Device
	if d, ok := c.DeviceCustomerMap[deviceId]; ok {
		device = d
	} else {
		return nil, nil, "", "", fmt.Errorf("No match device.")
	}
	if f, ok := c.FeatureCustomerMap[featureId]; ok {
		switch f.Type {
		case "property":
			bytes, err := json.Marshal(Data{
				Device: device.CustomDeviceId,
				Type:   f.CustomFeatureId,
				Value:  values[f.Id],
			})
			if err != nil {
				c.logger.Info("property:marshal device property issue data err:", err)
				return nil, nil, "", "", err
			}
			return []string{string(bytes)}, nil, device.IssueTopic, "", nil
		case "command":
			var (
				inputParam []Param
			)
			for _, in := range f.InputParams {
				inputParam = append(inputParam, Param{
					Id:    in.CustomParamId,
					Value: values[in.Id],
				})
			}
			bytes, err := json.Marshal(Command{
				Device: device.CustomDeviceId,
				Params: inputParam,
			})
			if err != nil {
				c.logger.Info("command:marshal device command issue err:", err)
				return nil, nil, "", "", err
			}
			//outputParam := []string{}
			// Here should Base on the message number.
			// for _, out := range f.OutputParams {
			// 	outputParam = append(outputParam, out.Id)
			// }
			return []string{string(bytes)}, []string{""}, device.IssueTopic, device.IssueResponseTopic, nil
		}
	} else {
		return nil, nil, "", "", fmt.Errorf("No match features.")
	}
	return nil, nil, "", "", nil
}

// ConvertDeviceMessages2MQFormat receives device command issue responses and converts it to RabbitMQ normative format.
func (c *Converter) ConvertDeviceMessages2MQFormat(messages []string, featureType string) (string, []byte, error) {
	// TODO: concrete implement
	for _, msg := range messages {
		switch featureType {
		case "reportData":
			var (
				data    Data
				device  models.Device
				feature models.Feature
			)
			if err := json.Unmarshal([]byte(msg), &data); err != nil {
				c.logger.Info("unmarshal msg error: %s\n", err)
				return "", nil, err
			}
			if d, ok := c.CustomerDeviceMap[data.Device]; ok {
				device = d
			} else {
				c.logger.Info("No matched device.")
				return "", nil, fmt.Errorf("No matched device.")
			}
			if f, ok := c.CustomerFeatuerMap[data.Type]; ok {
				feature = f
			} else {
				c.logger.Info("No matched feature.")
				return "", nil, fmt.Errorf("No matched feature.")
			}
			rk := fmt.Sprintf("thing.report.%s.%s.property", device.ModelId, device.DeviceId)
			bytes, err := json.Marshal(&ds.RabbitMQMsg{
				MsgId:     uuid.New().String(),
				Version:   "1.0.0",
				Topic:     rk,
				Timestamp: time.Now().Format("2006-01-02 15:04:05"),
				Params: []ds.Param{
					{
						Id:    feature.Id,
						Value: data.Value,
					},
				},
			})
			if err != nil {
				c.logger.Info("Marshal rabbitmq msg error: %s\n", err)
				return "", nil, err
			}
			return rk, bytes, nil
		case "issueReply":
			var (
				commandReply   Command
				device         models.Device
				rabbitMQParams []ds.Param
			)
			if err := json.Unmarshal([]byte(msg), &commandReply); err != nil {
				c.logger.Info("unmarshal issue_reply msg error: %s\n", err)
				return "", nil, err
			}
			if d, ok := c.CustomerDeviceMap[commandReply.Device]; ok {
				device = d
			} else {
				c.logger.Info("issue_reply msg error,No matched device:%s", device)
				return "", nil, fmt.Errorf("No matched device.")
			}
			for _, param := range commandReply.Params {
				if p, ok := c.CustomerOutputParamMap[param.Id]; ok {
					rabbitMQParams = append(rabbitMQParams, ds.Param{
						Id:    p.Id,
						Value: param.Value,
					})
				} else {
					c.logger.Info("issue_reply msg error,No matched params:%s", p.Id)
					continue
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
	}
	return "", nil, nil
}

func main() {
	mqttConfig, err := loadConfig(os.Getenv("PROTOCOL_CONFIG_PATH"))
	if err != nil {
		klog.Errorf("Load configmap error, %s", err.Error())
		return
	}
	// Use map to store the data for fast searching.
	deviceCustomerMap := make(map[string]models.Device)
	featureCustomerMap := make(map[string]models.Feature)
	inputParamCustomerMap := make(map[string]models.Param)
	outputParamCustomerMap := make(map[string]models.Param)
	customerDeviceMap := make(map[string]models.Device)
	customerFeatureMap := make(map[string]models.Feature)
	customerInputParamMap := make(map[string]models.Param)
	customerOutputParamMap := make(map[string]models.Param)
	for _, d := range mqttConfig.Devices {
		deviceCustomerMap[d.DeviceId] = d
		customerDeviceMap[d.CustomDeviceId] = d
		for _, sd := range d.SubDevices {
			deviceCustomerMap[d.DeviceId] = sd
			customerDeviceMap[d.CustomDeviceId] = sd
		}
	}
	for _, m := range mqttConfig.Models {
		for _, f := range m.Features {
			featureCustomerMap[f.Id] = f
			customerFeatureMap[f.CustomFeatureId] = f
			if f.Type == "command" {
				for _, in := range f.InputParams {
					inputParamCustomerMap[in.Id] = in
					customerInputParamMap[in.CustomParamId] = in
				}
				for _, out := range f.OutputParams {
					outputParamCustomerMap[out.Id] = out
					customerOutputParamMap[out.CustomParamId] = out
				}
			}
		}
		// Search in subModels.
		for _, sm := range m.SubModels {
			for _, f := range sm.Features {
				featureCustomerMap[f.Id] = f
				customerFeatureMap[f.CustomFeatureId] = f
				if f.Type == "command" {
					for _, in := range f.InputParams {
						inputParamCustomerMap[in.Id] = in
						customerInputParamMap[in.CustomParamId] = in
					}
					for _, out := range f.OutputParams {
						outputParamCustomerMap[out.Id] = out
						customerOutputParamMap[out.CustomParamId] = out
					}
				}
			}
		}
	}
	logger := hclog.New(&hclog.LoggerOptions{
		Level:      hclog.Trace,
		Output:     os.Stderr,
		JSONFormat: true,
	})
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: shared.Handshake,
		Plugins: map[string]plugin.Plugin{
			"converter": &shared.ConverterPlugin{Impl: &Converter{
				logger:                 logger,
				DeviceCustomerMap:      deviceCustomerMap,
				FeatureCustomerMap:     featureCustomerMap,
				InputParamCustomerMap:  inputParamCustomerMap,
				OutputParamCustomerMap: outputParamCustomerMap,
				CustomerDeviceMap:      customerDeviceMap,
				CustomerFeatuerMap:     customerFeatureMap,
				CustomerInputParamMap:  customerInputParamMap,
				CustomerOutputParamMap: customerOutputParamMap,
			}},
		},

		// A non-nil value here enables gRPC serving for this plugin...
		GRPCServer: plugin.DefaultGRPCServer,
	})
}

func loadConfig(path string) (*models.Protocol, error) {
	c := &models.Protocol{}
	contents, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("Failed to read configuration file %s, error %s", path, err)
	}
	if err = yaml.Unmarshal(contents, c); err != nil {
		return nil, fmt.Errorf("Failed to parse configuration, error %s", err)
	}
	return c, nil
}
