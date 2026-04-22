package coord

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
	"time"
)

const (
	respWriteTimeout = 3 * time.Second
	respReadTimeout  = 5 * time.Second
	respDialTimeout  = 3 * time.Second
)

func respDial(addr string) (net.Conn, error) {
	d := &net.Dialer{Timeout: respDialTimeout}
	return d.Dial("tcp", addr)
}

// respSend writes a RESP command to conn.
func respSend(c net.Conn, args ...string) error {
	_ = c.SetWriteDeadline(time.Now().Add(respWriteTimeout))
	var sb strings.Builder
	fmt.Fprintf(&sb, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&sb, "$%d\r\n%s\r\n", len(a), a)
	}
	_, err := io.WriteString(c, sb.String())
	return err
}

// respRead reads one RESP reply using a fresh bufio.Reader.
// Use this only for single-shot request/reply connections where no buffered
// data will be left unconsumed. For long-lived connections (pub/sub) keep the
// bufio.Reader alive across calls and use readRESP directly.
func respRead(c net.Conn) (any, error) {
	_ = c.SetReadDeadline(time.Now().Add(respReadTimeout))
	return readRESP(bufio.NewReader(c))
}

// respSendRead sends a command and reads one reply on a fresh connection.
func respSendRead(c net.Conn, args ...string) (any, error) {
	if err := respSend(c, args...); err != nil {
		return nil, err
	}
	return respRead(c)
}

// readRESP reads one RESP value from r.
// The caller is responsible for keeping r alive across multiple replies
// on the same connection (e.g. pub/sub streams).
func readRESP(r *bufio.Reader) (any, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if len(line) == 0 {
		return nil, fmt.Errorf("coord/resp: empty reply")
	}
	switch line[0] {
	case '+':
		return line[1:], nil
	case '-':
		return nil, fmt.Errorf("coord/resp: server error: %s", line[1:])
	case ':':
		var n int64
		_, err := fmt.Sscanf(line[1:], "%d", &n)
		return n, err
	case '$':
		var n int
		if _, err := fmt.Sscanf(line[1:], "%d", &n); err != nil {
			return nil, err
		}
		if n < 0 {
			return nil, nil // nil bulk
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	case '*':
		var n int
		if _, err := fmt.Sscanf(line[1:], "%d", &n); err != nil {
			return nil, err
		}
		if n < 0 {
			return nil, nil
		}
		arr := make([]any, n)
		for i := range arr {
			arr[i], err = readRESP(r)
			if err != nil {
				return nil, err
			}
		}
		return arr, nil
	}
	return nil, fmt.Errorf("coord/resp: unknown type %q", line[0])
}
