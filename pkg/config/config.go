package config

import "github.com/i5heu/GoQueueBench/pkg/testbench"

// Config is an alias for testbench.Config. This allows other programs to import
// the queue configuration without pulling in the entire testbench package.
type Config = testbench.Config
