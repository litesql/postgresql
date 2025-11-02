package config

const (
	UseNamespace         = "use_namespace"          // Keep schema/namespace (otherwise always use main database)
	PositionTrackerTable = "position_tracker_table" // Table to store replication position checkpoints
	Timeout              = "timeout"                // timeout in milliseconds
	Logger               = "logger"                 // Log errors to "stdout, stderr or file:/path/to/log.txt"

	DefaultReplicationVTabName  = "pg"
	DefaultPositionTrackerTable = "_pg"
)
