package containerd

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

func withDefaults(config Config) Config {
	if config.Address == "" {
		config.Address = DefaultAddress
	}
	if config.Namespace == "" {
		config.Namespace = DefaultNamespace
	}
	if config.CleanupTimeout == 0 {
		config.CleanupTimeout = defaultCleanupTimeout
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return config
}

func validateConfig(config Config) error {
	if strings.TrimSpace(config.Address) == "" || strings.TrimSpace(config.Namespace) == "" {
		return fmt.Errorf("%w: address and namespace are required", ErrInvalidConfig)
	}
	if config.Namespace == "version" {
		return fmt.Errorf("%w: namespace %q is reserved", ErrInvalidConfig, config.Namespace)
	}
	if config.Logs != nil && !filepath.IsAbs(config.LogBinary) {
		return fmt.Errorf("%w: log binary must be absolute", ErrInvalidConfig)
	}
	if config.CleanupTimeout <= 0 {
		return fmt.Errorf("%w: cleanup timeout must be positive", ErrInvalidConfig)
	}
	return nil
}
