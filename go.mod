module github.com/dpf-mqtt-demo

go 1.16

replace (
	github.com/beeedge/beethings => /beethings/beethings
	github.com/beeedge/device-plugin-framework => /beethings/device-plugin-framework
)

require (
	github.com/beeedge/beethings v0.0.0-00010101000000-000000000000
	github.com/beeedge/device-plugin-framework v0.0.0-20221110072337-fee28457221e
	github.com/google/uuid v1.3.0
	github.com/hashicorp/go-hclog v1.3.1
	github.com/hashicorp/go-plugin v1.4.6
	gopkg.in/yaml.v2 v2.4.0
	k8s.io/klog/v2 v2.80.1 // indirect
)
