package model

import (
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
)

// SyslogMessageDTO represents a Syslog message
type SyslogMessageDTO struct {
	Timestamp string `xml:"timestamp,attr"`
	Content   []byte `xml:",chardata"`
}

// GetContent returns the readable text of the syslog message
func (dto SyslogMessageDTO) GetContent() string {
	txt, err := base64.StdEncoding.DecodeString(string(dto.Content))
	if err == nil {
		return string(txt)
	}
	return "[unknown]"
}

// SyslogMessageLogDTO represents a collection of Syslog messages
type SyslogMessageLogDTO struct {
	XMLName       xml.Name           `xml:"syslog-message-log"`
	SystemID      string             `xml:"system-id,attr"`
	Location      string             `xml:"location,attr"`
	SourceAddress string             `xml:"source-address,attr"`
	SourcePort    int                `xml:"source-port,attr"`
	Messages      []SyslogMessageDTO `xml:"messages"`
}

// ToJSON converts the messages into an indented JSON string
func (dto SyslogMessageLogDTO) ToJSON() string {
	jsonmap := make(map[string]interface{})
	jsonmap["systemId"] = dto.SystemID
	jsonmap["location"] = dto.Location
	jsonmap["sourceAddress"] = dto.SourceAddress
	jsonmap["sourcePort"] = dto.SourcePort
	messages := make(map[string]string)
	for _, m := range dto.Messages {
		messages[m.Timestamp] = m.GetContent()
	}
	jsonmap["messages"] = messages
	bytes, _ := json.MarshalIndent(jsonmap, "  ", "")
	return string(bytes)
}
