// Package implement zabbix sender protocol for send metrics to zabbix.

package zabbix

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	. "github.com/tj/go-debug"
	"gopkg.in/ini.v1"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
	"strings"
	"time"
)

var debug = Debug("zabbix")

// Metric class.
type Metric struct {
	Host  string `json:"host"`
	Key   string `json:"key"`
	Value string `json:"value"`
	Clock int64  `json:"clock"`
}

// Metric class constructor.
func NewMetric(host, key, value string, clock ...int64) *Metric {
	m := &Metric{Host: host, Key: key, Value: value}
	// use current time, if `clock` is not specified
	if m.Clock = time.Now().Unix(); len(clock) > 0 {
		m.Clock = int64(clock[0])
	}
	return m
}

// Packet class.
type Packet struct {
	Request string    `json:"request"`
	Data    []*Metric `json:"data"`
	Clock   int64     `json:"clock"`
}

// Packet class constructor.
func NewPacket(data []*Metric, clock ...int64) *Packet {
	p := &Packet{Request: `sender data`, Data: data}
	// use current time, if `clock` is not specified
	if p.Clock = time.Now().Unix(); len(clock) > 0 {
		p.Clock = int64(clock[0])
	}
	return p
}

// DataLen Packet class method, return 8 bytes with packet length in little endian order.
func (p *Packet) DataLen() []byte {
	dataLen := make([]byte, 8)
	JSONData, _ := json.Marshal(p)
	binary.LittleEndian.PutUint32(dataLen, uint32(len(JSONData)))
	return dataLen
}

// Sender class.
type Sender struct {
	Host                     string
	Port                     string
	sendingBuffer            chan *Metric
	bufferDuration           time.Duration
	defaultTechnicalHostname string
	lastError                string
	resolvedServer           *net.TCPAddr
}

// Sender class constructor.
func NewSender(host, port string) *Sender {
	s := &Sender{Host: host, Port: port}
	return s
}

// Creates a buffered Sender with sendBuffer started, is intended for continous sending
func NewBufferedSender(host, port, defaultTechnicalHostname string, bufferSend time.Duration, bufferSize int) *Sender {
	s := NewSender(host, port)
	s.sendingBuffer = make(chan *Metric, bufferSize)
	s.bufferDuration = bufferSend
	s.defaultTechnicalHostname = defaultTechnicalHostname
	go s.sendBuffer()
	return s
}

// Keeps sending what the buffered Sender have in memory.
// is never terminated
func (s *Sender) sendBuffer() {
	var metrics []*Metric
	sendNow := func() {
		if len(metrics) > 0 {
			packet := NewPacket(metrics)
			response, err := s.timeout_send(packet)
			if err != nil {
				if s.lastError != err.Error() {
					log.Printf("Error sending data to zabbix: '%s'\n", err.Error())
					s.lastError = err.Error()
				}
			}
			debug("Zabbix server responded: %v", string(response[:]))
			metrics = []*Metric{}
		}
	}
	for {
		// if the metrics to send exceed the
		// sending buffer capacity
		// then send metrics array to zabbix
		if len(metrics) > cap(s.sendingBuffer) {
			sendNow()
		} else {
			// if the buffer is not empty
			// acummulate in the metrics array
			// when is empty send and wait
			select {
			case m := <-s.sendingBuffer:
				metrics = append(metrics, m)
			default:
				sendNow()
				// buffer is empty, nothing to send
				// wait until buffer is filled again
				time.Sleep(s.bufferDuration)
			}
		}
	}
}

// Creates a Buffered sender using default configuration file, optionally the technical hostname can be set
// if technical hostname not given will try to load the FQDN from `hostname -f`
// will use technical name "localhost" if everything fails
// current defaults if configuration file is not found (default zabbix trapper port is 10051)
// ServerActive = zabbix
// BufferSend = 5 (seconds)
// BufferSize = 100 (metrics)
func NewConfiguredSender(technical_hostname ...string) *Sender {
	cfg, _ := ini.Load("/etc/zabbix/zabbix_agentd.conf")
	server_active := cfg.Section("").Key("ServerActive").MustString("zabbix")
	host, port, err := net.SplitHostPort(server_active)
	if err != nil {
		host = server_active
		port = "10051"
	}
	hostname := ""
	if len(technical_hostname) > 0 {
		hostname = technical_hostname[0]
	} else {
		hostname_out, err := exec.Command("hostname", "-f").Output()
		hostname_cmd := ""
		if err != nil {
			debug("Can't get the FQDN (using default 'localhost'): %v", err.Error())
			hostname_cmd = "localhost"
		} else {
			hostname_cmd = strings.TrimSpace(string(hostname_out))
		}
		hostname = hostname_cmd
	}
	bufferSend := time.Second * time.Duration(cfg.Section("").Key("BufferSend").MustInt(5))
	bufferSize := cfg.Section("").Key("BufferSize").MustInt(100)
	debug("Using Host: '%v'", host)
	debug("Using Port: '%v'", port)
	debug("Using Technical hostname: '%v'", hostname)
	debug("Using BufferSend: '%v'", bufferSend)
	debug("Using BufferSize: '%v'", bufferSize)
	return NewBufferedSender(host, port, hostname, bufferSend, bufferSize)
}

// Method Sender class, return zabbix header.
func (s *Sender) getHeader() []byte {
	return []byte("ZBXD\x01")
}

// Method Sender class, resolve uri by name:port.
func (s *Sender) getTCPAddr() *net.TCPAddr {
	// format: hostname:port
	addr := fmt.Sprintf("%s:%s", s.Host, s.Port)

	// Resolve hostname:port to ip:port
	iaddr, err := net.ResolveTCPAddr("tcp", addr)

	if err != nil {
		fmt.Printf("Connection failed: %s", err.Error())
		os.Exit(1)
	}

	return iaddr
}

// Method Sender class, make connection to uri.
func (s *Sender) connect() *net.TCPConn {
	// Open connection to zabbix host
	iaddr := s.getTCPAddr()
	conn, err := net.DialTCP("tcp", nil, iaddr)

	if err != nil {
		fmt.Printf("Connection failed: %s", err.Error())
		os.Exit(1)
	}

	return conn
}

// Resolves given server name and connects (Dials) to it
// with a timeout of 10 seconds per operation
func (s *Sender) timeout_connect() (*net.TCPConn, error) {
	// Open connection to zabbix host
	addr := fmt.Sprintf("%s:%s", s.Host, s.Port)

	ch := make(chan bool, 1)
	var conn *net.TCPConn
	var err error

	if s.resolvedServer == nil {
		go func() {
			s.resolvedServer, err = net.ResolveTCPAddr("tcp", addr)
			ch <- true
		}()

		select {
		case <-ch:
			if err != nil {
				return nil, err
			}
		case <-time.After(10 * time.Second):
			return nil, errors.New(fmt.Sprintf("Timeout resolving Zabbix address %v", addr))
		}
	}

	go func() {
		conn, err = net.DialTCP("tcp", nil, s.resolvedServer)
		ch <- true
	}()

	select {
	case <-ch:
		return conn, err
	case <-time.After(10 * time.Second):
		return nil, errors.New(fmt.Sprintf("Timeout connecting to Zabbix %v", s.resolvedServer))
	}

}

// Method Sender class, read data from connection.
func (s *Sender) read(conn *net.TCPConn) []byte {
	res := make([]byte, 1024)
	res, err := ioutil.ReadAll(conn)
	if err != nil {
		fmt.Printf("Error while receiving the data: %s", err.Error())
		os.Exit(1)
	}

	return res
}

// read packet, reads the zabbix server response and returns only the payload
func (s *Sender) read_packet(conn *net.TCPConn) ([]byte, error) {
	var header [5]byte
	_, err := conn.Read(header[:]) // ZBXD\x01
	var datalen [8]byte
	_, err = conn.Read(datalen[:])
	buf := bytes.NewReader(datalen[:])
	var packetSize uint32
	binary.Read(buf, binary.LittleEndian, &packetSize)
	packet := make([]byte, packetSize)
	_, err = conn.Read(packet)
	return packet, err
}

// Method Sender class, send packet to zabbix.
func (s *Sender) Send(packet *Packet) []byte {
	conn := s.connect()
	defer conn.Close()

	dataPacket, _ := json.Marshal(packet)

	/*
	   fmt.Printf("HEADER: % x (%s)\n", s.getHeader(), s.getHeader())
	   fmt.Printf("DATALEN: % x, %d byte\n", packet.DataLen(), len(packet.DataLen()))
	   fmt.Printf("BODY: %s\n", string(dataPacket))
	*/

	// Fill buffer
	buffer := append(s.getHeader(), packet.DataLen()...)
	buffer = append(buffer, dataPacket...)

	// Sent packet to zabbix
	_, err := conn.Write(buffer)
	if err != nil {
		fmt.Printf("Error while sending the data: %s", err.Error())
		os.Exit(1)
	}

	res := s.read(conn)
	/*
	   fmt.Printf("RESPONSE: %s\n", string(res))
	*/
	return res
}

// Will send the defined packets returning the server response
// or an empty byte slice if the sent fails, or timeouts
// default timeout per operation 10 seconds
func (s *Sender) timeout_send(packet *Packet) ([]byte, error) {
	var err error
	conn, err := s.timeout_connect()

	if err != nil {
		return []byte{}, err
	}

	defer conn.Close()

	dataPacket, _ := json.Marshal(packet)

	debug("HEADER: % x (%s)", s.getHeader(), s.getHeader())
	debug("DATALEN: % x, %d byte", packet.DataLen(), len(packet.DataLen()))
	debug("BODY: %s", string(dataPacket))

	// Fill buffer
	buffer := append(s.getHeader(), packet.DataLen()...)
	buffer = append(buffer, dataPacket...)

	ch := make(chan bool, 1)

	go func() {
		// Sent packet to zabbix
		_, err = conn.Write(buffer)
		ch <- true
	}()

	select {
	case <-ch:
		// connection may be broken in the middle of the write
		if err != nil {
			return []byte{}, err
		}
	case <-time.After(10 * time.Second):
		return []byte{}, errors.New("Timeout sending data to Zabbix")
	}

	var response []byte

	go func() {
		// Read data from Zabbix
		response, err = s.read_packet(conn)
		ch <- true
	}()

	select {
	case <-ch:
		return response, err
	case <-time.After(10 * time.Second):
		return []byte{}, errors.New("Timeout reading data from Zabbix")
	}
}

// SendMetric adds the metric defined by key, value and optionally the UNIX epoch
// using the default technical hostname, to the Buffered Sender
// will return error if the Sender is not a Buffered Sender or if the buffer
// is at maximum capacity, the function should return as soon as possible
func (s *Sender) SendMetric(key, value string, clock ...int64) error {
	if s.sendingBuffer == nil {
		return errors.New("SendMetric function is intended only for buffered senders")
	}
	select {
	case s.sendingBuffer <- NewMetric(s.defaultTechnicalHostname, key, value, clock...):
		return nil
	default:
		return errors.New("Buffered Sender is at maximum capacity")
	}
}
