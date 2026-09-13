package rns

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"quad4/reticulum-go/pkg/common"
	"quad4/reticulum-go/pkg/interfaces"
	rnstransport "quad4/reticulum-go/pkg/transport"
)

type stack struct {
	transport  *rnstransport.Transport
	interfaces []interfaces.Interface
	started    []interfaces.Interface
}

func newStack(config *common.ReticulumConfig) (*stack, error) {
	result := &stack{transport: rnstransport.NewTransport(config)}
	for name, interfaceConfig := range config.Interfaces {
		if !interfaceConfig.Enabled {
			continue
		}
		normalized := normalizeInterfaceConfig(interfaceConfig)
		value, err := interfaces.NewFromConfig(name, normalized)
		if err != nil {
			return nil, fmt.Errorf("construct interface %q: %w", name, err)
		}
		result.interfaces = append(result.interfaces, value)
	}
	if len(result.interfaces) == 0 {
		return nil, fmt.Errorf("%w: at least one enabled Reticulum interface is required", ErrInvalidConfig)
	}
	return result, nil
}

func normalizeInterfaceConfig(config *common.InterfaceConfig) *common.InterfaceConfig {
	copy := *config
	if copy.Type != "UDPInterface" {
		return &copy
	}
	copy.Address = addressWithPort(copy.Address, copy.Port)
	target := copy.TargetAddress
	if target == "" {
		target = copy.TargetHost
	}
	copy.TargetHost = addressWithPort(target, copy.TargetPort)
	return &copy
}

func addressWithPort(address string, port int) string {
	address = strings.TrimSpace(address)
	if address == "" || port == 0 {
		return address
	}
	if _, _, err := net.SplitHostPort(address); err == nil {
		return address
	}
	return net.JoinHostPort(strings.Trim(address, "[]"), strconv.Itoa(port))
}

func (s *stack) Start() error {
	if err := s.transport.Start(); err != nil {
		return err
	}
	if err := s.transport.InitializePathRequestHandler(); err != nil {
		_ = s.transport.Close()
		return err
	}
	for _, value := range s.interfaces {
		if err := value.Start(); err != nil {
			_ = s.Close()
			return fmt.Errorf("start interface %q: %w", value.GetName(), err)
		}
		network, ok := value.(common.NetworkInterface)
		if !ok {
			_ = value.Stop()
			_ = s.Close()
			return fmt.Errorf("interface %q is not a network interface", value.GetName())
		}
		if err := s.transport.RegisterInterface(value.GetName(), network); err != nil {
			_ = value.Stop()
			_ = s.Close()
			return fmt.Errorf("register interface %q: %w", value.GetName(), err)
		}
		s.started = append(s.started, value)
	}
	return nil
}

func (s *stack) Close() error {
	var first error
	for index := len(s.started) - 1; index >= 0; index-- {
		if err := s.started[index].Stop(); err != nil && first == nil {
			first = err
		}
	}
	s.started = nil
	if err := s.transport.Close(); err != nil && first == nil {
		first = err
	}
	return first
}
