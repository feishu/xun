package dameng

import (
	"testing"
)

// TestParseDSN tests the DSN parsing functionality for DM database
func TestParseDSN(t *testing.T) {
	tests := []struct {
		name     string
		dsn      string
		expected *DSNConfig
		wantErr  bool
	}{
		{
			name: "simple dm URL with schema",
			dsn:  "dm://user:password@host:5236/mydb?schema=SC",
			expected: &DSNConfig{
				User:         "user",
				Passwd:       "password",
				Addr:         "host:5236",
				DatabaseName: "mydb",
				Schema:       "SC",
				Net:          "tcp",
				// Default values
				EscapeProcess:  false,
				AutoCommit:     true,
				RowPrefetch:    10,
				LobMode:        LobModeBatchLocal,
				BatchType:      BatchTypeBind,
				ConnectTimeout: 5000,
				// ... other default values
			},
			wantErr: false,
		},
		{
			name: "dm URL with GroupName and host list",
			dsn:  "dm://user:password@mygroup?GroupName=(host1:5236,host2:5236)&schema=SC",
			expected: &DSNConfig{
				User:      "user",
				Passwd:    "password",
				Addr:      "mygroup",
				GroupName: "mygroup",
				HostList:  []string{"host1:5236", "host2:5236"},
				Schema:    "SC",
				Net:       "tcp",
				// Default values from NewConfig()
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
			},
			wantErr: false,
		},
		{
			name: "dm URL with dialName",
			dsn:  "dm://user:password@host:5236?dialName=myDial&schema=SC",
			expected: &DSNConfig{
				User:   "user",
				Passwd: "password",
				Addr:   "host:5236",
				Schema: "SC",
				Net:    "tcp",
				// Default values from NewConfig()
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
				DialName:             "myDial", // This should be parsed from query param
				LoginMode:            LoginModeNormalPrimaryStandby,
			},
			wantErr: false,
		},
		{
			name: "simple connection string",
			dsn:  "host:5236",
			expected: &DSNConfig{
				Addr: "host:5236",
				Net:  "tcp",
				// Default values from NewConfig()
				EscapeProcess: false,
				AutoCommit:    true,
				MaxRows:       0,
				RowPrefetch:   10,
				LobMode:       LobModeBatchLocal,
				AlwaysAllowCommit: true,
				BatchType:     BatchTypeBind,
				SessionTimeout: 0,
				MPPLocal:      false,
				RWSeparate:    RWSeparateOff,
				RWPercent:     25,
				IsBdtaRS:      false,
				DoSwitch:      DoSwitchClose,
				ContinueBatchOnError: false,
				BatchAllowMaxErrors: 0,
				ConnectTimeout: 5000,
				ColumnNameUpperCase: false,
				RWIgnoreSql:   false,
				DBAliveCheckFreq: 0,
				LogLevel:      "off",
				LogFlushFreq:  10,
				LogBufferSize: 32768,
				LogFlusherQueueSize: 100,
				StatEnable:    false,
				StatFlushFreq: 3,
				StatSlowSqlCount: 100,
				StatHighFreqSqlCount: 100,
				StatSqlMaxCount: 100000,
				StatSqlRemoveMode: "latest",
				Compress:      0,
				CompressId:    CompressTypeZlib,
				BatchNotOnCall: false,
				EpSelector:    0,
				LoginDscCtrl:  false,
				SwitchInterval: 200,
				SwitchTimes:   1,
				LoginMode:     LoginModeNormalPrimaryStandby,
			},
			wantErr: false,
		},
		{
			name: "dm URL with special characters in password",
			dsn:  "dm://root:ZBuT6PzNyP6^y3kP@39.101.71.171:3306?dialName=suneed",
			expected: &DSNConfig{
				User:         "root",
				Passwd:       "ZBuT6PzNyP6^y3kP",
				Addr:         "39.101.71.171:3306",
				DialName:     "suneed",
				Net:          "tcp",
				// Default values from NewConfig()
				EscapeProcess: false,
				AutoCommit:    true,
				MaxRows:       0,
				RowPrefetch:   10,
				LobMode:       LobModeBatchLocal,
				AlwaysAllowCommit: true,
				BatchType:     BatchTypeBind,
				SessionTimeout: 0,
				MPPLocal:      false,
				RWSeparate:    RWSeparateOff,
				RWPercent:     25,
				IsBdtaRS:      false,
				DoSwitch:      DoSwitchClose,
				ContinueBatchOnError: false,
				BatchAllowMaxErrors: 0,
				ConnectTimeout: 5000,
				ColumnNameUpperCase: false,
				RWIgnoreSql:   false,
				DBAliveCheckFreq: 0,
				LogLevel:      "off",
				LogFlushFreq:  10,
				LogBufferSize: 32768,
				LogFlusherQueueSize: 100,
				StatEnable:    false,
				StatFlushFreq: 3,
				StatSlowSqlCount: 100,
				StatHighFreqSqlCount: 100,
				StatSqlMaxCount: 100000,
				StatSqlRemoveMode: "latest",
				Compress:      0,
				CompressId:    CompressTypeZlib,
				BatchNotOnCall: false,
				EpSelector:    0,
				LoginDscCtrl:  false,
				SwitchInterval: 200,
				SwitchTimes:   1,
				LoginMode:     LoginModeNormalPrimaryStandby,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseDSN(tt.dsn)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDSN() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// Check basic fields
			if got.User != tt.expected.User {
				t.Errorf("ParseDSN() User = %v, want %v", got.User, tt.expected.User)
			}
			if got.Passwd != tt.expected.Passwd {
				t.Errorf("ParseDSN() Passwd = %v, want %v", got.Passwd, tt.expected.Passwd)
			}
			if got.Addr != tt.expected.Addr {
				t.Errorf("ParseDSN() Addr = %v, want %v", got.Addr, tt.expected.Addr)
			}
			if got.DatabaseName != tt.expected.DatabaseName {
				t.Errorf("ParseDSN() DatabaseName = %v, want %v", got.DatabaseName, tt.expected.DatabaseName)
			}
			if got.Schema != tt.expected.Schema {
				t.Errorf("ParseDSN() Schema = %v, want %v", got.Schema, tt.expected.Schema)
			}
			if got.GroupName != tt.expected.GroupName {
				t.Errorf("ParseDSN() GroupName = %v, want %v", got.GroupName, tt.expected.GroupName)
			}
			if got.DialName != tt.expected.DialName {
				t.Errorf("ParseDSN() DialName = %v, want %v", got.DialName, tt.expected.DialName)
			}

			// Check host list
			if len(got.HostList) != len(tt.expected.HostList) {
				t.Errorf("ParseDSN() HostList length = %d, want %d", len(got.HostList), len(tt.expected.HostList))
			} else {
				for i, host := range got.HostList {
					if host != tt.expected.HostList[i] {
						t.Errorf("ParseDSN() HostList[%d] = %v, want %v", i, host, tt.expected.HostList[i])
					}
				}
			}

			// Check default values
			if got.EscapeProcess != tt.expected.EscapeProcess {
				t.Errorf("ParseDSN() EscapeProcess = %v, want %v", got.EscapeProcess, tt.expected.EscapeProcess)
			}
			if got.AutoCommit != tt.expected.AutoCommit {
				t.Errorf("ParseDSN() AutoCommit = %v, want %v", got.AutoCommit, tt.expected.AutoCommit)
			}
			if got.RowPrefetch != tt.expected.RowPrefetch {
				t.Errorf("ParseDSN() RowPrefetch = %v, want %v", got.RowPrefetch, tt.expected.RowPrefetch)
			}
			if got.LobMode != tt.expected.LobMode {
				t.Errorf("ParseDSN() LobMode = %v, want %v", got.LobMode, tt.expected.LobMode)
			}
			if got.BatchType != tt.expected.BatchType {
				t.Errorf("ParseDSN() BatchType = %v, want %v", got.BatchType, tt.expected.BatchType)
			}
			if got.ConnectTimeout != tt.expected.ConnectTimeout {
				t.Errorf("ParseDSN() ConnectTimeout = %v, want %v", got.ConnectTimeout, tt.expected.ConnectTimeout)
			}
		})
	}
}
