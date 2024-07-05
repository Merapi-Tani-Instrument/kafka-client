package kafkaClient

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type Broker struct {
	conf *Config
	rack *string

	id            int32
	addr          string
	correlationID int32
	conn          net.Conn
	connErr       error
	lock          sync.Mutex
	opened        int32

	throttleTimer *time.Timer
}

type bufConn struct {
	net.Conn
	buf *bufio.Reader
}

func NewBroker(addr string) *Broker {
	return &Broker{id: 0, addr: addr, correlationID: 0}
}

func (b *Broker) Close() error {
	b.lock.Lock()
	defer b.lock.Unlock()

	atomic.StoreInt32(&b.opened, 0)
	if b.conn != nil {
		b.conn.Close()
	}

	b.conn = nil
	b.connErr = nil

	return nil
}

func newBufConn(conn net.Conn) *bufConn {
	return &bufConn{
		Conn: conn,
		buf:  bufio.NewReader(conn),
	}
}

func (b *Broker) Open(conf *Config) error {
	if atomic.LoadInt32(&b.opened) == 1 {
		return ErrAlreadyConnected
	}

	if conf == nil {
		conf = NewConfig()
	}

	err := conf.Validate()
	if err != nil {
		return err
	}

	b.lock.Lock()
	defer b.lock.Unlock()

	b.conn, b.connErr = conf.getDialer().Dial("tcp", b.addr)

	if b.connErr != nil {
		return b.connErr
	}

	b.conn = newBufConn(b.conn)
	b.conf = conf
	atomic.StoreInt32(&b.opened, 1)

	if b.id >= 0 {
		Logger.Printf("Connected to broker at %s (registered as #%d)\n", b.addr, b.id)
	} else {
		Logger.Printf("Connected to broker at %s (unregistered)\n", b.addr)
	}

	return nil
}

func (b *Broker) sendAndReceive(req protocolBody, res protocolBody) error {
	b.lock.Lock()
	defer b.lock.Unlock()
	if b.opened != 1 {
		return ErrClosedClient
	}
	if !b.conf.Version.IsAtLeast(req.requiredVersion()) {
		return ErrUnsupportedVersion
	}

	request := &request{correlationID: b.correlationID, clientID: b.conf.ClientID, body: req}
	requestBuffer, err := encode(request)
	if err != nil {
		return err
	}
	_, err = b.write(requestBuffer)
	if err != nil {
		return err
	}
	b.correlationID++

	headerLength := getHeaderLength(res.headerVersion())
	header := make([]byte, headerLength)

	_, err = b.readFull(header)
	if err != nil {
		return err
	}
	decodedHeader := responseHeader{}
	err = versionedDecode(header, &decodedHeader, res.headerVersion())
	fmt.Println("Error version decode ", err)
	if decodedHeader.correlationID != request.correlationID {
		b.correlationID = 0
		return PacketDecodingError{fmt.Sprintf("correlation ID didn't match, wanted %d, got %d", request.correlationID, decodedHeader.correlationID)}
	}
	responseBuffer := make([]byte, decodedHeader.length-int32(headerLength)+4)
	_, err = b.readFull(responseBuffer)
	if err != nil {
		return err
	}
	err = versionedDecode(responseBuffer, res, req.version())
	if err != nil {
		return err
	}
	return nil
}

func (b *Broker) sendAndReceiveWithRetry(req protocolBody, res protocolBody, retry bool) error {
	b.lock.Lock()
	defer b.lock.Unlock()
	err := b.sendAndReceive(req, res)
	if (err == io.EOF || err == ErrNotConnected) && retry {
		Logger.Println("Retry send request")
		err = b.sendAndReceive(req, res)
	}
	return err
}

func getHeaderLength(headerVersion int16) int8 {
	if headerVersion < 1 {
		return 8
	} else {
		return 9
	}
}

func (b *Broker) readFull(buf []byte) (n int, err error) {
	if b.conn == nil {
		return 0, ErrClosedClient
	}
	if err := b.conn.SetReadDeadline(time.Now().Add(b.conf.Net.ReadTimeout)); err != nil {
		return 0, err
	}
	return io.ReadFull(b.conn, buf)
}

func (b *Broker) write(buf []byte) (n int, err error) {
	if b.conn == nil {
		return 0, ErrClosedClient
	}
	if err := b.conn.SetWriteDeadline(time.Now().Add(b.conf.Net.WriteTimeout)); err != nil {
		return 0, err
	}

	return b.conn.Write(buf)
}

func (b *Broker) decode(pd packetDecoder, version int16) (err error) {
	b.id, err = pd.getInt32()
	if err != nil {
		return err
	}

	var host string
	if version < 9 {
		host, err = pd.getString()
	} else {
		host, err = pd.getCompactString()
	}
	if err != nil {
		return err
	}

	port, err := pd.getInt32()
	if err != nil {
		return err
	}

	if version >= 1 && version < 9 {
		b.rack, err = pd.getNullableString()
	} else if version >= 9 {
		b.rack, err = pd.getCompactNullableString()
	}
	if err != nil {
		return err
	}

	b.addr = net.JoinHostPort(host, fmt.Sprint(port))
	if _, _, err := net.SplitHostPort(b.addr); err != nil {
		return err
	}

	if version >= 9 {
		_, err := pd.getEmptyTaggedFieldArray()
		if err != nil {
			return err
		}
	}

	return nil
}

func (b *Broker) encode(pe packetEncoder, version int16) (err error) {
	host, portstr, err := net.SplitHostPort(b.addr)
	if err != nil {
		return err
	}

	port, err := strconv.ParseInt(portstr, 10, 32)
	if err != nil {
		return err
	}

	pe.putInt32(b.id)

	if version < 9 {
		err = pe.putString(host)
	} else {
		err = pe.putCompactString(host)
	}
	if err != nil {
		return err
	}

	pe.putInt32(int32(port))

	if version >= 1 {
		if version < 9 {
			err = pe.putNullableString(b.rack)
		} else {
			err = pe.putNullableCompactString(b.rack)
		}
		if err != nil {
			return err
		}
	}

	if version >= 9 {
		pe.putEmptyTaggedFieldArray()
	}

	return nil
}
