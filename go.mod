module bessarabov/mac2mqtt

go 1.24.0

toolchain go1.24.3

require (
	github.com/antonfisher/go-media-devices-state v0.2.0
	github.com/cloudfoundry/gosigar v1.3.112
	github.com/eclipse/paho.mqtt.golang v1.5.1
	github.com/shirou/gopsutil/v3 v3.24.5
	gopkg.in/yaml.v2 v2.4.0
)

require (
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/tklauser/go-sysconf v0.3.12 // indirect
	github.com/tklauser/numcpus v0.6.1 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	golang.org/x/net v0.44.0 // indirect
	golang.org/x/sync v0.17.0 // indirect
	golang.org/x/sys v0.36.0 // indirect
)

// using my fork until PR #9 is resolved in upstream
replace github.com/antonfisher/go-media-devices-state => github.com/johntdyer/go-media-devices-state v0.0.0-20251204145225-5b3592a6499f
