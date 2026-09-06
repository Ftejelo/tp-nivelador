package protocol

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

type MessageType byte

const (
	BetMsg  MessageType = 1
	Finish  MessageType = 2
	Winners MessageType = 3
)

type Bet struct {
	AgencyId  int
	FirstName string
	LastName  string
	Document  int
	Birthdate string
	Number    int
}

type BetMessage struct {
	Bets []Bet
}

type FinishMessage struct{}

type WinnersMessage struct {
	Winners []Bet
}

func encodeString(value string) []byte {
	raw := []byte(value)
	buf := make([]byte, 2+len(raw))
	binary.BigEndian.PutUint16(buf[:2], uint16(len(raw)))
	copy(buf[2:], raw)
	return buf
}

func decodeString(data []byte, offset int) (string, int, error) {
	if offset+2 > len(data) {
		return "", 0, fmt.Errorf("truncated string length")
	}
	length := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2
	if offset+length > len(data) {
		return "", 0, fmt.Errorf("truncated string data")
	}
	return string(data[offset : offset+length]), offset + length, nil
}

func serializeBet(bet Bet) []byte {
	payload := make([]byte, 0, 32)
	var buf [4]byte

	binary.BigEndian.PutUint32(buf[:], uint32(bet.AgencyId))
	payload = append(payload, buf[:]...)
	payload = append(payload, encodeString(bet.FirstName)...)
	payload = append(payload, encodeString(bet.LastName)...)
	binary.BigEndian.PutUint32(buf[:], uint32(bet.Document))
	payload = append(payload, buf[:]...)
	payload = append(payload, encodeString(bet.Birthdate)...)
	binary.BigEndian.PutUint32(buf[:], uint32(bet.Number))
	payload = append(payload, buf[:]...)
	return payload
}

func deserializeBet(data []byte, offset int) (Bet, int, error) {
	if offset+4 > len(data) {
		return Bet{}, 0, fmt.Errorf("truncated agency id")
	}
	agencyID := int(int32(binary.BigEndian.Uint32(data[offset : offset+4])))
	offset += 4

	firstName, next, err := decodeString(data, offset)
	if err != nil {
		return Bet{}, 0, fmt.Errorf("decode first_name: %w", err)
	}
	offset = next

	lastName, next, err := decodeString(data, offset)
	if err != nil {
		return Bet{}, 0, fmt.Errorf("decode last_name: %w", err)
	}
	offset = next

	if offset+4 > len(data) {
		return Bet{}, 0, fmt.Errorf("truncated document")
	}
	document := int(int32(binary.BigEndian.Uint32(data[offset : offset+4])))
	offset += 4

	birthdate, next, err := decodeString(data, offset)
	if err != nil {
		return Bet{}, 0, fmt.Errorf("decode birthdate: %w", err)
	}
	offset = next

	if offset+4 > len(data) {
		return Bet{}, 0, fmt.Errorf("truncated number")
	}
	number := int(int32(binary.BigEndian.Uint32(data[offset : offset+4])))
	offset += 4

	return Bet{
		AgencyId:  agencyID,
		FirstName: firstName,
		LastName:  lastName,
		Document:  document,
		Birthdate: birthdate,
		Number:    number,
	}, offset, nil
}

func serializeBets(bets []Bet) []byte {
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, uint32(len(bets)))
	for _, bet := range bets {
		payload = append(payload, serializeBet(bet)...)
	}
	return payload
}

func deserializeBets(data []byte, offset int) ([]Bet, int, error) {
	if offset+4 > len(data) {
		return nil, 0, fmt.Errorf("truncated bet count")
	}
	count := int(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	bets := make([]Bet, 0, count)
	for i := 0; i < count; i++ {
		bet, next, err := deserializeBet(data, offset)
		if err != nil {
			return nil, 0, fmt.Errorf("decode bet[%d]: %w", i, err)
		}
		bets = append(bets, bet)
		offset = next
	}
	return bets, offset, nil
}

func SerializeMessage(msgType MessageType, payload interface{}) ([]byte, error) {
	var payloadBytes []byte

	switch msgType {
	case BetMsg:
		betMsg, ok := payload.(BetMessage)
		if !ok {
			return nil, fmt.Errorf("invalid BET payload")
		}
		payloadBytes = serializeBets(betMsg.Bets)
	case Winners:
		winnersMsg, ok := payload.(WinnersMessage)
		if !ok {
			return nil, fmt.Errorf("invalid WINNERS payload")
		}
		payloadBytes = serializeBets(winnersMsg.Winners)
	case Finish:
		payloadBytes = nil
	default:
		return nil, fmt.Errorf("unknown message type: %v", msgType)
	}

	frame := make([]byte, 5+len(payloadBytes))
	frame[0] = byte(msgType)
	binary.BigEndian.PutUint32(frame[1:5], uint32(len(payloadBytes)))
	copy(frame[5:], payloadBytes)
	return frame, nil
}

func RecvMessage(socket io.Reader) (MessageType, interface{}, error) {
	header, err := safe_socket.RecvAll(socket, 5)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read message header: %w", err)
	}

	msgType := MessageType(header[0])
	payloadLength := int(binary.BigEndian.Uint32(header[1:5]))
	payloadBytes, err := safe_socket.RecvAll(socket, payloadLength)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read message payload: %w", err)
	}

	switch msgType {
	case BetMsg:
		bets, _, err := deserializeBets(payloadBytes, 0)
		if err != nil {
			return 0, nil, fmt.Errorf("failed to deserialize bets: %w", err)
		}
		return BetMsg, BetMessage{Bets: bets}, nil
	case Finish:
		return Finish, FinishMessage{}, nil
	case Winners:
		winners, _, err := deserializeBets(payloadBytes, 0)
		if err != nil {
			return 0, nil, fmt.Errorf("failed to deserialize winners: %w", err)
		}
		return Winners, WinnersMessage{Winners: winners}, nil
	default:
		return 0, nil, fmt.Errorf("unknown message type: %v", msgType)
	}
}

func SendMessage(socket io.Writer, msgType MessageType, payload interface{}) error {
	messageBytes, err := SerializeMessage(msgType, payload)
	if err != nil {
		return err
	}
	return safe_socket.SendAll(socket, messageBytes)
}
