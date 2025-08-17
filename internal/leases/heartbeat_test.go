package leases_test

import (
	"context"
	"math/rand/v2"
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
				leases.WithRand(rand.New(rand.NewPCG(1, 2))),
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
			t.Cleanup(pool.Close)
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
		name          string
		opts          []leases.Option
		onFirstSelect func(db dbtx.DBTX, schema *database.Schema)
		ring          leases.Ring
		want          leases.Ring
		values        []uint32
		assert        func(t *testing.T, schema *SchemaMock)
	}{
		{
			name: "keep a single node ring stable",
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
			values: []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			assert: func(t *testing.T, schema *SchemaMock) {
				assert.Equal(t, 1, len(schema.RefreshLeasesCalls()))
				assert.Equal(t, 0, len(schema.ApproveLeaseCalls()))
				assert.Equal(t, 0, len(schema.InsertLeaseCalls()))
			},
		},

		{
			name: "approve another node",
			opts: []leases.Option{
				leases.WithNodeName("node1"),
				leases.WithVNodeCount(2),
				leases.WithRange(leases.Range{From: 0, To: 10}),
			},
			ring: leases.Ring{
				{VNode: 1, NodeName: "node1", Valid: true, Status: leases.Leased},
				{VNode: 3, NodeName: "node1", Valid: true, Status: leases.Leased},
				{VNode: 5, NodeName: "new", Valid: true, Status: leases.Pending},
				{VNode: 7, NodeName: "new", Valid: true, Status: leases.Pending},
			},
			want: leases.Ring{
				{VNode: 1, NodeName: "node1", Valid: true, Status: leases.Leased},
				{VNode: 3, NodeName: "node1", Valid: true, Status: leases.Leased},
				{VNode: 5, NodeName: "new", Valid: true, Status: leases.Leased},
				{VNode: 7, NodeName: "new", Valid: true, Status: leases.Leased},
			},
			values: []uint32{1, 2, 3, 4},
			assert: func(t *testing.T, schema *SchemaMock) {
				assert.Equal(t, 1, len(schema.RefreshLeasesCalls()))
				assert.Equal(t, 1, len(schema.ApproveLeaseCalls()))
				assert.Equal(t, 0, len(schema.InsertLeaseCalls()))
			},
		},

		{
			name: "approve new node af looping the circle",
			opts: []leases.Option{
				leases.WithNodeName("node1"),
				leases.WithVNodeCount(1),
				leases.WithRange(leases.Range{From: 0, To: 10}),
			},
			ring: leases.Ring{
				{VNode: 3, NodeName: "new", Valid: true, Status: leases.Pending},
				{VNode: 7, NodeName: "node1", Valid: true, Status: leases.Leased},
			},
			want: leases.Ring{
				{VNode: 3, NodeName: "new", Valid: true, Status: leases.Leased},
				{VNode: 7, NodeName: "node1", Valid: true, Status: leases.Leased},
			},
			values: []uint32{0, 1, 2, 7, 8, 9},
			assert: func(t *testing.T, schema *SchemaMock) {
				assert.Equalf(t, 1, len(schema.RefreshLeasesCalls()), "RefreshLeases")
				assert.Equalf(t, 1, len(schema.ApproveLeaseCalls()), "ApproveLease")
				assert.Equalf(t, 0, len(schema.InsertLeaseCalls()), "InsertLease")
			},
		},

		{
			name: "wait for approval by other node",
			opts: []leases.Option{
				leases.WithNodeName("node1"),
				leases.WithVNodeCount(2),
				leases.WithRange(leases.Range{From: 0, To: 10}),
			},
			ring: leases.Ring{
				{VNode: 0, NodeName: "other", Valid: true, Status: leases.Leased},
				{VNode: 2, NodeName: "other", Valid: true, Status: leases.Leased},
				{VNode: 6, NodeName: "node1", Valid: true, Status: leases.Pending},
				{VNode: 8, NodeName: "node1", Valid: true, Status: leases.Pending},
			},
			want: leases.Ring{
				{VNode: 0, NodeName: "other", Valid: true, Status: leases.Leased},
				{VNode: 2, NodeName: "other", Valid: true, Status: leases.Leased},
				{VNode: 6, NodeName: "node1", Valid: true, Status: leases.Pending},
				{VNode: 8, NodeName: "node1", Valid: true, Status: leases.Pending},
			},
			values: []uint32{},
			assert: func(t *testing.T, schema *SchemaMock) {
				assert.Equalf(t, 1, len(schema.RefreshLeasesCalls()), "RefreshLeases")
				assert.Equalf(t, 0, len(schema.ApproveLeaseCalls()), "ApproveLease")
				assert.Equalf(t, 0, len(schema.InsertLeaseCalls()), "InsertLease")
			},
		},

		{
			name: "approve own node",
			opts: []leases.Option{
				leases.WithNodeName("node1"),
				leases.WithVNodeCount(3),
				leases.WithRange(leases.Range{From: 0, To: 10}),
			},
			ring: leases.Ring{
				{VNode: 3, NodeName: "node1", Valid: true, Status: leases.Leased},
				{VNode: 5, NodeName: "node1", Valid: true, Status: leases.Pending},
				{VNode: 7, NodeName: "node1", Valid: true, Status: leases.Leased},
			},
			want: leases.Ring{
				{VNode: 3, NodeName: "node1", Valid: true, Status: leases.Leased},
				{VNode: 5, NodeName: "node1", Valid: true, Status: leases.Leased},
				{VNode: 7, NodeName: "node1", Valid: true, Status: leases.Leased},
			},
			values: []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			assert: func(t *testing.T, schema *SchemaMock) {
				assert.Equalf(t, 1, len(schema.RefreshLeasesCalls()), "RefreshLeases")
				assert.Equalf(t, 1, len(schema.ApproveLeaseCalls()), "ApproveLease")
				assert.Equalf(t, 0, len(schema.InsertLeaseCalls()), "InsertLease")
			},
		},

		{
			name: "lease a new vnode",
			opts: []leases.Option{
				leases.WithNodeName("node1"),
				leases.WithVNodeCount(3),
				leases.WithRange(leases.Range{From: 0, To: 10}),
			},
			ring: leases.Ring{
				{VNode: 3, NodeName: "node1", Valid: true, Status: leases.Leased},
				{VNode: 7, NodeName: "node1", Valid: true, Status: leases.Leased},
			},
			want: leases.Ring{
				{VNode: 3, NodeName: "node1", Valid: true, Status: leases.Leased},
				{VNode: 6, NodeName: "node1", Valid: true, Status: leases.Pending},
				{VNode: 7, NodeName: "node1", Valid: true, Status: leases.Leased},
			},
			values: []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			assert: func(t *testing.T, schema *SchemaMock) {
				assert.Equal(t, 1, len(schema.RefreshLeasesCalls()))
				assert.Equal(t, 0, len(schema.ApproveLeaseCalls()))
				assert.Equal(t, 1, len(schema.InsertLeaseCalls()))
			},
		},

		{
			name: "lease new nodes on first run",
			opts: []leases.Option{
				leases.WithNodeName("node1"),
				leases.WithVNodeCount(3),
				leases.WithRange(leases.Range{From: 0, To: 10}),
			},
			ring: leases.Ring{},
			want: leases.Ring{
				{VNode: 2, NodeName: "node1", Valid: true, Status: leases.Pending},
				{VNode: 6, NodeName: "node1", Valid: true, Status: leases.Pending},
				{VNode: 7, NodeName: "node1", Valid: true, Status: leases.Pending},
			},
			values: []uint32{},
			assert: func(t *testing.T, schema *SchemaMock) {
				assert.Equalf(t, 1, len(schema.RefreshLeasesCalls()), "RefreshLeases")
				assert.Equalf(t, 0, len(schema.ApproveLeaseCalls()), "ApproveLease")
				assert.Equalf(t, 3, len(schema.InsertLeaseCalls()), "InsertLease")
			},
		},

		{
			name: "approve next vnode when none is leased",
			opts: []leases.Option{
				leases.WithNodeName("node1"),
				leases.WithVNodeCount(3),
				leases.WithRange(leases.Range{From: 0, To: 10}),
			},
			ring: leases.Ring{
				{VNode: 2, NodeName: "node1", Valid: true, Status: leases.Pending},
				{VNode: 6, NodeName: "node1", Valid: true, Status: leases.Pending},
				{VNode: 7, NodeName: "node1", Valid: true, Status: leases.Pending},
			},
			want: leases.Ring{
				{VNode: 2, NodeName: "node1", Valid: true, Status: leases.Leased},
				{VNode: 6, NodeName: "node1", Valid: true, Status: leases.Leased},
				{VNode: 7, NodeName: "node1", Valid: true, Status: leases.Leased},
			},
			values: []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			assert: func(t *testing.T, schema *SchemaMock) {
				assert.Equalf(t, 1, len(schema.RefreshLeasesCalls()), "RefreshLeases")
				assert.Equalf(t, 1, len(schema.ApproveLeaseCalls()), "ApproveLease")
				assert.Equalf(t, 0, len(schema.InsertLeaseCalls()), "InsertLease")
			},
		},

		{
			name: "approve other when self is pending",
			opts: []leases.Option{
				leases.WithNodeName("node1"),
				leases.WithVNodeCount(2),
				leases.WithRange(leases.Range{From: 0, To: 10}),
			},
			ring: leases.Ring{
				{VNode: 2, NodeName: "node1", Valid: true, Status: leases.Pending},
				{VNode: 4, NodeName: "other", Valid: true, Status: leases.Pending},
				{VNode: 6, NodeName: "node1", Valid: true, Status: leases.Pending},
				{VNode: 8, NodeName: "other", Valid: true, Status: leases.Pending},
			},
			want: leases.Ring{
				{VNode: 2, NodeName: "node1", Valid: true, Status: leases.Pending},
				{VNode: 4, NodeName: "other", Valid: true, Status: leases.Leased},
				{VNode: 6, NodeName: "node1", Valid: true, Status: leases.Pending},
				{VNode: 8, NodeName: "other", Valid: true, Status: leases.Leased},
			},
			values: []uint32{},
			assert: func(t *testing.T, schema *SchemaMock) {
				assert.Equalf(t, 1, len(schema.RefreshLeasesCalls()), "RefreshLeases")
				assert.Equalf(t, 1, len(schema.ApproveLeaseCalls()), "ApproveLease")
				assert.Equalf(t, 0, len(schema.InsertLeaseCalls()), "InsertLease")
			},
		},

		{
			name: "evacuate old leases",
			opts: []leases.Option{
				leases.WithNodeName("node1"),
				leases.WithVNodeCount(2),
				leases.WithRange(leases.Range{From: 0, To: 10}),
			},
			ring: leases.Ring{
				{VNode: 2, NodeName: "node1", Valid: true, Status: leases.Leased},
				{VNode: 4, NodeName: "old", Valid: false, Status: leases.Leased},
				{VNode: 6, NodeName: "node1", Valid: true, Status: leases.Leased},
				{VNode: 8, NodeName: "old", Valid: false, Status: leases.Leased},
			},
			want: leases.Ring{
				{VNode: 2, NodeName: "node1", Valid: true, Status: leases.Leased},
				{VNode: 6, NodeName: "node1", Valid: true, Status: leases.Leased},
			},
			values: []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			assert: func(t *testing.T, schema *SchemaMock) {
				assert.Equalf(t, 1, len(schema.RefreshLeasesCalls()), "RefreshLeases")
				assert.Equalf(t, 0, len(schema.ApproveLeaseCalls()), "ApproveLease")
				assert.Equalf(t, 0, len(schema.InsertLeaseCalls()), "InsertLease")
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
			var nodeName string
			schemaMock.RefreshLeasesFunc = func(ctx context.Context, db dbtx.DBTX, name string, ttl time.Duration) (leases.Ring, error) {
				nodeName = name
				ring, err := schema.RefreshLeases(ctx, db, name, ttl)
				if len(schemaMock.RefreshLeasesCalls()) == 1 && tt.onFirstSelect != nil {
					tt.onFirstSelect(db, schema)
				}

				return ring, err

			}
			var sut = newSUT(t, schemaMock, tt.opts...)

			for _, info := range tt.ring {
				ttl := -time.Hour
				if info.Valid {
					ttl = time.Second
				}
				err := schema.InsertLease(t.Context(), db, info.VNode, info.NodeName, ttl, info.Status.String())
				if !assert.NoError(t, err) {
					t.FailNow()
				}
			}

			if true {
				return
			}

			// act
			values, err := sut.Heartbeat(t.Context(), db)

			// assert
			assert.NoError(t, err)
			got, err := schema.RefreshLeases(t.Context(), db, nodeName, time.Hour)
			if !assert.NoError(t, err) {
				t.FailNow()
			}
			assert.EqualSlice(t, tt.want, got)
			if !assert.EqualSlice(t, tt.values, values) {
				t.Logf("got : %v", values)
				t.Logf("want: %v", tt.values)
			}
			tt.assert(t, schemaMock)
		})
	}
}
