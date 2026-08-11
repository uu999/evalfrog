package ir

import (
	"strconv"
	"strings"
)

func NodePath(id LogicalID, index int) string {
	if id == "" {
		return "/nodes/" + strconv.Itoa(index)
	}
	return "/nodes/" + pointerToken(string(id))
}

func InputPath(nodeID LogicalID, nodeIndex int, name PortName, inputIndex int) string {
	base := NodePath(nodeID, nodeIndex) + "/inputs/"
	if name == "" {
		return base + strconv.Itoa(inputIndex)
	}
	return base + pointerToken(string(name))
}

func OutputPath(nodeID LogicalID, nodeIndex int, name PortName, outputIndex int) string {
	base := NodePath(nodeID, nodeIndex) + "/outputs/"
	if name == "" {
		return base + strconv.Itoa(outputIndex)
	}
	return base + pointerToken(string(name))
}

func EdgePath(id LogicalID, index int) string {
	if id == "" {
		return "/edges/" + strconv.Itoa(index)
	}
	return "/edges/" + pointerToken(string(id))
}

func pointerToken(value string) string {
	value = strings.ReplaceAll(value, "~", "~0")
	return strings.ReplaceAll(value, "/", "~1")
}
