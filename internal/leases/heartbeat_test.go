package leases_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kyuff/es-postgres/internal/assert"
	"github.com/kyuff/es-postgres/internal/database"
	"github.com/kyuff/es-postgres/internal/dbtx"
	"github.com/kyuff/es-postgres/internal/leases"
)

func TestHeartbeat(t *testing.T) {
	var (
		newSUT = func(t *testing.T, schema leases.Schema, opts ...leases.Option) *leases.Heartbeat {
			t.Helper()
			testOptions := append([]leases.Option{
				leases.WithHeartbeatInterval(10 * time.Millisecond),
				leases.WithRange(leases.Range{From: 0, To: 100}),
			}, opts...)
			cfg := leases.DefaultOptions()
			for _, opt := range testOptions {
				opt(cfg)
			}

			return leases.NewHeartbeat(cfg, schema)
		}

		newSchema = func(t *testing.T) (*pgxpool.Pool, *database.Schema) {
			t.Helper()
			dsn := database.DSNTest(t)
			pool, err := database.Connect(t.Context(), dsn)
			if !assert.NoError(t, err) {
				t.FailNow()
			}
			schema, err := database.NewSchema("es")
			if !assert.NoError(t, err) {
				t.FailNow()
			}
			err = database.Migrate(t.Context(), pool, schema)
			if !assert.NoError(t, err) {
				t.FailNow()
			}
			return pool, schema
		}
	)

	var tests = []struct {
		name             string
		opts             []leases.Option
		afterFirstSelect func(db dbtx.DBTX, schema *database.Schema)
		ring             leases.Ring
		want             leases.Ring
	}{
		{
			name: "test",
			opts: []leases.Option{
				leases.WithNodeName("node1"),
				leases.WithVNodeCount(2),
				leases.WithRange(leases.Range{From: 0, To: 10}),
			},
			ring: leases.Ring{
				{VNode: 3, NodeName: "node1", Valid: true, Status: leases.Leased},
				{VNode: 7, NodeName: "node1", Valid: true, Status: leases.Leased},
			},
			want: leases.Ring{
				{VNode: 3, NodeName: "node1", Valid: true, Status: leases.Leased},
				{VNode: 7, NodeName: "node1", Valid: true, Status: leases.Leased},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// arrange
			var db, schema = newSchema(t)
			var schemaMock = &SchemaMock{
				ApproveLeaseFunc: schema.ApproveLease,
				InsertLeaseFunc:  schema.InsertLease,
			}
			schemaMock.SelectLeasesFunc = func(ctx context.Context, db dbtx.DBTX) (leases.Ring, error) {
				ring, err := schema.SelectLeases(ctx, db)
				if len(schemaMock.SelectLeasesCalls()) == 1 && tt.afterFirstSelect != nil {
					tt.afterFirstSelect(db, schema)
				}

				return ring, err

			}
			var sut = newSUT(t, schema, tt.opts...)

			for _, info := range tt.ring {
				ttl := time.Duration(0)
				if info.Valid {
					ttl = time.Second
				}
				err := schema.InsertLease(t.Context(), db, info.VNode, info.NodeName, ttl, info.Status.String())
				if !assert.NoError(t, err) {
					t.FailNow()
				}
			}

			// act
			err := sut.Heartbeat(t.Context(), db)

			// assert
			assert.NoError(t, err)
			got, err := schema.SelectLeases(t.Context(), db)
			if !assert.NoError(t, err) {
				t.FailNow()
			}
			assert.EqualSlice(t, tt.want, got)
		})
	}
}
