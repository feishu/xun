// Package grammar provides database-specific SQL grammar implementations
package dameng

import (
	"net/url"
	"strings"
)

// Config represents the configuration parsed from a DSN string
// It contains all the properties supported by DM database connection
// https://eco.dameng.com/document/dm/zh-cn/pm/go-rogramming-guide.html
//nolint:golint,unused
// go:generate stringer -type=BatchType,LoginMode,CompressType
//nolint:golint,unused
// go:generate stringer -type=RWSeparate,DoSwitch
//nolint:golint,unused
// go:generate stringer -type=LobMode

// BatchType defines the batch processing type
//
//nolint:golint,unused
type BatchType int

const (
	// BatchTypeBind represents batch binding
	BatchTypeBind BatchType = 1
	// BatchTypeNoBind represents no batch binding
	BatchTypeNoBind BatchType = 2
)

// LoginMode defines the login mode
//
//nolint:golint,unused
type LoginMode int

const (
	// LoginModePrimaryNormalStandby represents priority order: PRIMARY, NORMAL, STANDBY
	LoginModePrimaryNormalStandby LoginMode = 0
	// LoginModePrimaryOnly represents only connect to primary
	LoginModePrimaryOnly LoginMode = 1
	// LoginModeStandbyOnly represents only connect to standby
	LoginModeStandbyOnly LoginMode = 2
	// LoginModeStandbyPrimaryNormal represents priority order: STANDBY, PRIMARY, NORMAL
	LoginModeStandbyPrimaryNormal LoginMode = 3
	// LoginModeNormalPrimaryStandby represents priority order: NORMAL, PRIMARY, STANDBY
	LoginModeNormalPrimaryStandby LoginMode = 4
)

// CompressType defines the compression type
//
//nolint:golint,unused
type CompressType int

const (
	// CompressTypeZlib represents ZLIB compression
	CompressTypeZlib CompressType = 0
	// CompressTypeSnappy represents SNAPPY compression
	CompressTypeSnappy CompressType = 1
)

// RWSeparate defines the read-write separation mode
//
//nolint:golint,unused
type RWSeparate int

const (
	// RWSeparateOff represents read-write separation is off
	RWSeparateOff RWSeparate = 0
	// RWSeparateAuto represents read-write separation is on, standby is auto-selected
	RWSeparateAuto RWSeparate = 1
	// RWSeparateClient represents read-write separation is on, standby is selected by client
	RWSeparateClient RWSeparate = 2
	// RWSeparateServer represents read-write separation is on, standby is informed by server
	RWSeparateServer RWSeparate = 3
	// RWSeparateClientConsistent represents read-write separation is on, only connect to consistent standby
	RWSeparateClientConsistent RWSeparate = 4
	// RWSeparateGateway represents read-write separation is on, connect to first available standby in epList
	RWSeparateGateway RWSeparate = 5
)

// DoSwitch defines the connection switch strategy
//
//nolint:golint,unused
type DoSwitch int

const (
	// DoSwitchClose represents close connection when exception occurs
	DoSwitchClose DoSwitch = 0
	// DoSwitchAuto represents auto switch to other libraries when exception occurs
	DoSwitchAuto DoSwitch = 1
	// DoSwitchRecover represents switch to previous nodes when they recover
	DoSwitchRecover DoSwitch = 2
)

// LobMode defines the LOB mode
//
//nolint:golint,unused
type LobMode int

const (
	// LobModeBatchLocal represents batch cache to local
	LobModeBatchLocal LobMode = 1
	// LobModeOnceLocal represents once cache all LOB data to local
	LobModeOnceLocal LobMode = 2
)

// Config represents the configuration parsed from a DSN string
//
//nolint:golint,unused
type DSNConfig struct {
	User         string            // Username
	Passwd       string            // Password
	Net          string            // Network type (default: tcp)
	Addr         string            // Network address (host:port)
	GroupName    string            // Group name for multiple hosts
	HostList     []string          // List of hosts (host1:port1,host2:port2,...)
	DatabaseName string            // Database name
	Params       map[string]string // Connection parameters

	// Connection properties
	SocketTimeout        int          // Socket timeout in seconds
	EscapeProcess        bool         // Whether to escape SQL
	AutoCommit           bool         // Whether to auto commit
	MaxRows              int          // Max rows in result set
	RowPrefetch          int          // Prefetch rows count
	LobMode              LobMode      // LOB mode
	AlwaysAllowCommit    bool         // Whether to allow commit when autoCommit is true
	BatchType            BatchType    // Batch processing type
	AppName              string       // Application name
	SessionTimeout       int          // Session timeout in seconds
	SSLCertPath          string       // SSL certificate path
	SSLKeyPath           string       // SSL key path
	MPPLocal             bool         // Whether to use MPP local connection
	RWSeparate           RWSeparate   // Read-write separation mode
	RWPercent            int          // Percent of transactions to distribute to primary
	IsBdtaRS             bool         // Whether to use column mode result set
	DoSwitch             DoSwitch     // Connection switch strategy
	ContinueBatchOnError bool         // Whether to continue batch execution on error
	BatchAllowMaxErrors  int          // Max allowable errors for batch execution
	ConnectTimeout       int          // Connect timeout in milliseconds
	ColumnNameUpperCase  bool         // Whether to convert column names to uppercase
	RWIgnoreSql          bool         // Whether to ignore SQL type for read-write separation
	CompatibleMode       string       // Compatible database mode (Oracle, Mysql)
	DBAliveCheckFreq     int          // Database alive check frequency in milliseconds
	LogDir               string       // Log directory
	LogLevel             string       // Log level (off, error, warn, sql, info, all)
	LogFlushFreq         int          // Log flush frequency in seconds
	LogBufferSize        int          // Log buffer size in bytes
	LogFlusherQueueSize  int          // Log flusher queue size
	StatEnable           bool         // Whether to enable statistics
	StatFlushFreq        int          // Statistics flush frequency in seconds
	StatSlowSqlCount     int          // Top slow SQL count
	StatHighFreqSqlCount int          // Top high frequency SQL count
	StatSqlMaxCount      int          // Max SQL count for statistics
	StatSqlRemoveMode    string       // SQL remove mode for statistics (latest, eldest)
	StatDir              string       // Statistics directory
	Schema               string       // Current schema
	CipherPath           string       // Third-party encryption algorithm library path
	Compress             int          // Compress mode (0: off, 1: full, 2: optimized)
	CompressId           CompressType // Compression algorithm
	SvcConfPath          string       // Custom client configuration file path
	BatchNotOnCall       bool         // Whether to disable batch execution for stored procedures
	Cluster              string       // Cluster type (DSC)
	ColumnNameCase       string       // Column name case (upper, lower)
	EpSelector           int          // Service name connection strategy (0: round-robin, 1: first-available)
	LoginDscCtrl         bool         // Whether to only connect to DSC control nodes
	OsName               string       // Operating system name
	AddressRemap         string       // Address remapping
	UserRemap            string       // User remapping
	SwitchInterval       int          // Switch interval in milliseconds
	SwitchTimes          int          // Switch times
	DialName             string       // Custom dial name
	LocalTimezone        int          // Local timezone in minutes
	KeyWords             string       // User keywords
	Language             string       // Language (CN, EN, CNT_HK)
	RsRefreshFreq        int          // Result set cache refresh frequency in seconds
	RsCacheSize          int          // Result set cache size in MB
	LoginMode            LoginMode    // Login mode
}

// NewConfig creates a new DSNConfig with default values
//
//nolint:golint,unused
func NewDSNConfig() *DSNConfig {
	return &DSNConfig{
		Net:                  "tcp",
		EscapeProcess:        false,
		AutoCommit:           true,
		MaxRows:              0,
		RowPrefetch:          10,
		LobMode:              LobModeBatchLocal,
		AlwaysAllowCommit:    true,
		BatchType:            BatchTypeBind,
		SessionTimeout:       0,
		MPPLocal:             false,
		RWSeparate:           RWSeparateOff,
		RWPercent:            25,
		IsBdtaRS:             false,
		DoSwitch:             DoSwitchClose,
		ContinueBatchOnError: false,
		BatchAllowMaxErrors:  0,
		ConnectTimeout:       5000,
		ColumnNameUpperCase:  false,
		RWIgnoreSql:          false,
		DBAliveCheckFreq:     0,
		LogLevel:             "off",
		LogFlushFreq:         10,
		LogBufferSize:        32768,
		LogFlusherQueueSize:  100,
		StatEnable:           false,
		StatFlushFreq:        3,
		StatSlowSqlCount:     100,
		StatHighFreqSqlCount: 100,
		StatSqlMaxCount:      100000,
		StatSqlRemoveMode:    "latest",
		Compress:             0,
		CompressId:           CompressTypeZlib,
		BatchNotOnCall:       false,
		EpSelector:           0,
		LoginDscCtrl:         false,
		SwitchInterval:       200,
		SwitchTimes:          1,
		LoginMode:            LoginModeNormalPrimaryStandby,
	}
}

// ParseDSN parses the DSN string into a DSNConfig structure
func ParseDSN(dsn string) (*DSNConfig, error) {
	// New config with default values
	cfg := NewDSNConfig()

	// Check if it's a dm:// URL
	if !strings.HasPrefix(dsn, "dm://") {
		// Not a URL format, use as simple connection string
		cfg.Addr = dsn
		return cfg, nil
	}

	// Extract the dm:// prefix
	dsn = dsn[5:] // Remove "dm://"

	// Parse user:password@host:port/path?query
	var userInfo, hostPathQuery string

	// Split on @ to separate user info from host/path/query
	if atIndex := strings.Index(dsn, "@"); atIndex != -1 {
		userInfo = dsn[:atIndex]
		hostPathQuery = dsn[atIndex+1:]
	} else {
		// No user info, use entire string as host/path/query
		hostPathQuery = dsn
	}

	// Parse user and password
	if userInfo != "" {
		if colonIndex := strings.Index(userInfo, ":"); colonIndex != -1 {
			cfg.User = userInfo[:colonIndex]
			cfg.Passwd = userInfo[colonIndex+1:]
		} else {
			// Only username, no password
			cfg.User = userInfo
		}
	}

	// Parse host, path and query
	var hostPath, query string
	if qIndex := strings.Index(hostPathQuery, "?"); qIndex != -1 {
		hostPath = hostPathQuery[:qIndex]
		query = hostPathQuery[qIndex+1:]
	} else {
		hostPath = hostPathQuery
	}

	// Parse host and path
	var host, path string
	if pIndex := strings.Index(hostPath, "/"); pIndex != -1 {
		host = hostPath[:pIndex]
		path = hostPath[pIndex:]
	} else {
		host = hostPath
	}

	// Set address
	cfg.Addr = host

	// Parse database name from path
	if len(path) > 1 {
		cfg.DatabaseName = path[1:] // Remove leading /
	}

	// Parse query parameters
	var params map[string][]string
	var err error
	if query != "" {
		params, err = url.ParseQuery(query)
		if err != nil {
			return nil, err
		}

		// Handle GroupName parameter which might contain host list
		if groupParam, ok := params["GroupName"]; ok && len(groupParam) > 0 {
			cfg.GroupName = cfg.Addr // Group name from host part
			groupValue := groupParam[0]
			// Check if GroupName parameter contains host list in format (host1:port1,host2:port2,...)
			if strings.HasPrefix(groupValue, "(") && strings.HasSuffix(groupValue, ")") {
				// Remove parentheses
				hostListStr := groupValue[1 : len(groupValue)-1]
				// Split by comma
				cfg.HostList = strings.Split(hostListStr, ",")
			} else {
				// GroupName parameter is just a name
				cfg.HostList = []string{groupValue}
			}
			// Remove GroupName from params
			delete(params, "GroupName")
		}

		// Set schema if provided
		if schemaParam, ok := params["schema"]; ok && len(schemaParam) > 0 {
			cfg.Schema = schemaParam[0]
			delete(params, "schema")
		}

		// Parse other known parameters
		if dialName, ok := params["dialName"]; ok && len(dialName) > 0 {
			cfg.DialName = dialName[0]
			delete(params, "dialName")
		}

		if appName, ok := params["appName"]; ok && len(appName) > 0 {
			cfg.AppName = appName[0]
			delete(params, "appName")
		}

		if compatibleMode, ok := params["compatibleMode"]; ok && len(compatibleMode) > 0 {
			cfg.CompatibleMode = compatibleMode[0]
			delete(params, "compatibleMode")
		}

		if logDir, ok := params["logDir"]; ok && len(logDir) > 0 {
			cfg.LogDir = logDir[0]
			delete(params, "logDir")
		}

		if logLevel, ok := params["logLevel"]; ok && len(logLevel) > 0 {
			cfg.LogLevel = logLevel[0]
			delete(params, "logLevel")
		}

		if statDir, ok := params["statDir"]; ok && len(statDir) > 0 {
			cfg.StatDir = statDir[0]
			delete(params, "statDir")
		}

		if cipherPath, ok := params["cipherPath"]; ok && len(cipherPath) > 0 {
			cfg.CipherPath = cipherPath[0]
			delete(params, "cipherPath")
		}

		if svcConfPath, ok := params["svcConfPath"]; ok && len(svcConfPath) > 0 {
			cfg.SvcConfPath = svcConfPath[0]
			delete(params, "svcConfPath")
		}

		if cluster, ok := params["cluster"]; ok && len(cluster) > 0 {
			cfg.Cluster = cluster[0]
			delete(params, "cluster")
		}

		if columnNameCase, ok := params["columnNameCase"]; ok && len(columnNameCase) > 0 {
			cfg.ColumnNameCase = columnNameCase[0]
			delete(params, "columnNameCase")
		}

		if osName, ok := params["osName"]; ok && len(osName) > 0 {
			cfg.OsName = osName[0]
			delete(params, "osName")
		}

		if addressRemap, ok := params["addressRemap"]; ok && len(addressRemap) > 0 {
			cfg.AddressRemap = addressRemap[0]
			delete(params, "addressRemap")
		}

		if userRemap, ok := params["userRemap"]; ok && len(userRemap) > 0 {
			cfg.UserRemap = userRemap[0]
			delete(params, "userRemap")
		}

		if keyWords, ok := params["keyWords"]; ok && len(keyWords) > 0 {
			cfg.KeyWords = keyWords[0]
			delete(params, "keyWords")
		}

		if language, ok := params["language"]; ok && len(language) > 0 {
			cfg.Language = language[0]
			delete(params, "language")
		}

		// Store remaining parameters
		if len(params) > 0 {
			cfg.Params = make(map[string]string)
			for key, values := range params {
				if len(values) > 0 {
					cfg.Params[key] = values[0]
				}
			}
		}
	}

	return cfg, nil
}
