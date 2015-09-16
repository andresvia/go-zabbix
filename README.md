go-zabbix
==============================================================================
Golang package, implement zabbix sender protocol for send metrics to zabbix.

Example 1:
```go
package main

import (
    "time"
    . "github.com/blacked/go-zabbix"
)

const (
    defaultHost  = `localhost`
    defaultPort  = `10051`
)

func main() {
    var metrics []*Metric
    metrics = append(metrics, NewMetric("localhost", "cpu", "1.22", time.Now().Unix()))
    metrics = append(metrics, NewMetric("localhost", "status", "OK"))

    // Create instance of Packet class
    packet := NewPacket(metrics)

    // Send packet to zabbix
    z := NewSender(defaultHost, defaultPort)
    z.Send(packet)
}
```

Example 2:
```go
package main
import (
	"fmt"
	"github.com/andresvia/go-zabbix"
	"time"
)
func main() {
	z := zabbix.NewConfiguredSender()
	for {
		z.SendMetric("time_now_unix", fmt.Sprintf("%v", time.Now().Unix()))
		time.Sleep(time.Second * 1)
	}
}
```
