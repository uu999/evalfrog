package ir

import "regexp"

const (
	MaxLogicalIDBytes = 128
	MaxPortNameBytes  = 64
)

var (
	logicalIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)
	portNamePattern  = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,63}$`)
	nodeTypePattern  = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
)

func ValidLogicalID(value LogicalID) bool {
	return logicalIDPattern.MatchString(string(value))
}

func ValidPortName(value PortName) bool {
	return portNamePattern.MatchString(string(value))
}

func ValidRouteName(value RouteName) bool {
	return portNamePattern.MatchString(string(value))
}

func ValidNodeType(value NodeType) bool {
	return nodeTypePattern.MatchString(string(value))
}
