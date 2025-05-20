package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetLeastTrafficServer(t *testing.T) {
	tests := []struct {
		name           string
		stats          map[string]*BackendServer
		expectedOneOf  []string // Очікуваний адрес (або кілька можливих, якщо порядок не гарантується)
		expectNil      bool
	}{
		{
			name: "basic healthy choice",
			stats: map[string]*BackendServer{
				"srv1": {Address: "srv1", Traffic: 100, Healthy: true},
				"srv2": {Address: "srv2", Traffic: 50, Healthy: true}, // очікуваний
				"srv3": {Address: "srv3", Traffic: 20, Healthy: false},
			},
			expectedOneOf: []string{"srv2"},
		},
		{
			name: "all servers unhealthy",
			stats: map[string]*BackendServer{
				"srv1": {Address: "srv1", Traffic: 10, Healthy: false},
				"srv2": {Address: "srv2", Traffic: 20, Healthy: false},
			},
			expectNil: true,
		},
		{
			name: "only one healthy",
			stats: map[string]*BackendServer{
				"srv1": {Address: "srv1", Traffic: 200, Healthy: false},
				"srv2": {Address: "srv2", Traffic: 5, Healthy: true}, // єдиний живий
				"srv3": {Address: "srv3", Traffic: 100, Healthy: false},
			},
			expectedOneOf: []string{"srv2"},
		},
		{
			name: "equal traffic multiple healthy",
			stats: map[string]*BackendServer{
				"srv1": {Address: "srv1", Traffic: 10, Healthy: true},
				"srv2": {Address: "srv2", Traffic: 10, Healthy: true},
				"srv3": {Address: "srv3", Traffic: 10, Healthy: false},
			},
			expectedOneOf: []string{"srv1", "srv2"}, // будь-який з них може бути вибраний
		},
		{
			name: "best server becomes unhealthy",
			stats: map[string]*BackendServer{
				"srv1": {Address: "srv1", Traffic: 1, Healthy: false},  // був найкращий, але недоступний
				"srv2": {Address: "srv2", Traffic: 2, Healthy: true},   // новий мінімум
				"srv3": {Address: "srv3", Traffic: 5, Healthy: true},
			},
			expectedOneOf: []string{"srv2"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// підміна глобальної карти бекендів
			backendStats = test.stats

			selected := getLeastTrafficServer()

			if test.expectNil {
				require.Nil(t, selected)
			} else {
				require.NotNil(t, selected)
				require.Contains(t, test.expectedOneOf, selected.Address)
			}
		})
	}
}
