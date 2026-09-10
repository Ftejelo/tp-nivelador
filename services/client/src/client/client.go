package client

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/protocol"
)

const ConnectionAttemptsMax = 20
const ConnectionAttempsDelayMs = 250

type Config struct {
	ServerHost string
	ServerPort string
	AgencyId   string
	InputFile  string
	OutputFile string
	BatchSize  int
}

type Client struct {
	conn   net.Conn
	config Config
}

func NewClient(config Config) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}
	if config.BatchSize >= 8 {
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			tcpConn.SetNoDelay(true)
		}
	}

	client := &Client{conn: conn, config: config}
	return client, nil
}

func connectToServer(host, port string) (net.Conn, error) {
	const action = "connect-to-server"
	var err error
	var conn net.Conn

	logger.Info(action, logger.InProgress)
	for i := range ConnectionAttemptsMax {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			logger.Warn(action, logger.Fail, "attempt", i)
			time.Sleep(ConnectionAttempsDelayMs * time.Millisecond)
			continue
		}

		logger.Info(action, logger.Success)
		break
	}

	return conn, err
}

func (client *Client) Run() error {
	return client.RunWithContext(context.Background())
}

func (client *Client) recvMessageWithContext(ctx context.Context, conn *net.TCPConn) (protocol.MessageType, interface{}, error) {
	// Use a goroutine to receive message with context cancellation
	type result struct {
		msgType protocol.MessageType
		payload interface{}
		err     error
	}
	
	resultChan := make(chan result, 1)
	
	go func() {
		msgType, payload, err := protocol.RecvMessage(conn)
		resultChan <- result{msgType, payload, err}
	}()
	
	select {
	case <-ctx.Done():
		// Context cancelled, close connection to unblock the goroutine
		conn.Close()
		return 0, nil, ctx.Err()
	case res := <-resultChan:
		return res.msgType, res.payload, res.err
	}
}

func isContextCanceled(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func (client *Client) RunWithContext(ctx context.Context) error {
	const mainAction = "process-bets"
	defer client.conn.Close()
	if client.config.BatchSize <= 0 {
		return fmt.Errorf("invalid batch size: %d", client.config.BatchSize)
	}

	// Open input file
	inputFile, err := os.Open(client.config.InputFile)
	if err != nil {
		logger.Error("open-input-file", logger.Fail, "file", client.config.InputFile, "err", err)
		return err
	}
	defer inputFile.Close()

	// Parse agency ID
	agencyId, err := strconv.Atoi(client.config.AgencyId)
	if err != nil {
		logger.Error("parse-agency-id", logger.Fail, "agency-id", client.config.AgencyId, "err", err)
		return err
	}

	scanner := bufio.NewScanner(inputFile)
	batch := make([]protocol.Bet, 0, client.config.BatchSize)
	lineNumber := 0
	totalBets := 0
	flushBatch := func(start int) error {
		if len(batch) == 0 {
			return nil
		}
		betMsg := protocol.BetMessage{Bets: append([]protocol.Bet(nil), batch...)}
		messageArgs := []any{
			"agency-id", client.config.AgencyId,
			"bets-count", len(batch),
			"batch-start", start,
			"batch-end", start + len(batch),
		}
		logger.Info("send-bets", logger.InProgress, messageArgs...)
		if err := protocol.SendMessage(client.conn, protocol.BetMsg, betMsg); err != nil {
			logger.Error("send-bets", logger.Fail, messageArgs...)
			return err
		}
		logger.Info("send-bets", logger.Success, messageArgs...)
		batch = batch[:0]
		return nil
	}

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			logger.Info(mainAction, logger.Fail, "reason", "shutdown requested", "bets-parsed", totalBets)
			return ctx.Err()
		default:
		}

		line := strings.TrimSpace(scanner.Text())
		lineNumber++
		if line == "" {
			continue
		}

		fields := strings.Split(line, ",")
		if len(fields) != 5 {
			logger.Warn("parse-bet", logger.Fail, "line", lineNumber, "reason", "invalid format")
			continue
		}

		document, err := strconv.Atoi(fields[2])
		if err != nil {
			logger.Warn("parse-bet", logger.Fail, "line", lineNumber, "reason", "invalid document")
			continue
		}

		number, err := strconv.Atoi(fields[4])
		if err != nil {
			logger.Warn("parse-bet", logger.Fail, "line", lineNumber, "reason", "invalid number")
			continue
		}

		batch = append(batch, protocol.Bet{
			AgencyId:  agencyId,
			FirstName: fields[0],
			LastName:  fields[1],
			Document:  document,
			Birthdate: fields[3],
			Number:    number,
		})
		totalBets++

		if len(batch) >= client.config.BatchSize {
			if err := flushBatch(totalBets - len(batch)); err != nil {
				return err
			}
		}
	}

	if err := scanner.Err(); err != nil {
		logger.Error("scan-input-file", logger.Fail, "err", err)
		return err
	}
	if err := flushBatch(totalBets - len(batch)); err != nil {
		return err
	}

	// Send FINISH message to trigger winner calculation
	logger.Info("send-finish", logger.InProgress, "agency-id", client.config.AgencyId)
	if err := protocol.SendMessage(client.conn, protocol.Finish, protocol.FinishMessage{}); err != nil {
		logger.Error("send-finish", logger.Fail, "agency-id", client.config.AgencyId, "err", err)
		return err
	}
	logger.Info("send-finish", logger.Success, "agency-id", client.config.AgencyId)

	// Receive winners from server
	logger.Info("receive-winners", logger.InProgress, "agency-id", client.config.AgencyId)
	
	// Set read deadline to allow context cancellation
	conn := client.conn.(*net.TCPConn)
	msgType, payload, err := client.recvMessageWithContext(ctx, conn)
	if err != nil {
		if isContextCanceled(err) {
			logger.Info("receive-winners", logger.Fail, "agency-id", client.config.AgencyId, "reason", "shutdown requested")
			return err
		}
		logger.Error("receive-winners", logger.Fail, "agency-id", client.config.AgencyId, "err", err)
		return err
	}
	if msgType != protocol.Winners {
		logger.Error("receive-winners", logger.Fail, "agency-id", client.config.AgencyId, "reason", "unexpected message type")
		return fmt.Errorf("expected WINNERS message, got %s", msgType)
	}

	winnersMsg := payload.(protocol.WinnersMessage)
	logger.Info("receive-winners", logger.Success, "agency-id", client.config.AgencyId, "winners-count", len(winnersMsg.Winners))

	// Create output file
	outputFile, err := os.Create(client.config.OutputFile)
	if err != nil {
		logger.Error("create-output-file", logger.Fail, "file", client.config.OutputFile, "err", err)
		return err
	}
	defer outputFile.Close()

	// Write winners to output file
	for _, winner := range winnersMsg.Winners {
		line := fmt.Sprintf("%s,%s,%d,%s,%d",
			winner.FirstName,
			winner.LastName,
			winner.Document,
			winner.Birthdate,
			winner.Number,
		)
		if _, err := outputFile.WriteString(line + "\n"); err != nil {
			logger.Error("write-output", logger.Fail, "file", client.config.OutputFile, "err", err)
			return err
		}
	}

	logger.Info(mainAction, logger.Success, "agency-id", client.config.AgencyId, "bets-processed", totalBets, "winners-written", len(winnersMsg.Winners))

	return nil
}
