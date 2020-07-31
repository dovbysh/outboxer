package publisher

import (
	"encoding/json"
	"github.com/dovbysh/go-outboxer/event"
	"github.com/dovbysh/go-utils/testing/tlog"
	"github.com/dovbysh/tests_common/v3"
	"github.com/go-pg/pg/v9"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

var (
	sc stan.Conn
	db *pg.DB
)

func TestNats(t *testing.T) {
	var wg sync.WaitGroup
	var pgCloser func()
	o, pgCloser, _, _ := tests_common.PostgreSQLContainer(&wg)
	defer pgCloser()

	natsOpt, natsCloser, _, _ := tests_common.NatsStreamingContainer(&wg)
	defer natsCloser()

	wg.Wait()

	opts := []nats.Option{nats.Name("NATS Streaming outboxer test")}
	nc, err := nats.Connect(natsOpt.Url, opts...)
	if err != nil {
		panic(err)
	}
	defer nc.Close()
	sc, err = stan.Connect(natsOpt.ClusterId, "outboxer-test-ClientId", stan.NatsConn(nc))
	if err != nil {
		panic(err)
	}
	defer sc.Close()

	db = pg.Connect(&pg.Options{
		Addr:         o.Addr,
		User:         o.User,
		Password:     o.Password,
		Database:     o.Database,
		PoolSize:     o.PoolSize,
		MinIdleConns: o.MinIdleConns,
	})
	db.AddQueryHook(tlog.NewShowQuery(t))
	t.Run("simple", simple)
}

type User struct {
	Id    uint64
	Login string
}

const simpleSubj = "simple"

func simple(t *testing.T) {
	db.Model((*event.Outbox)(nil)).CreateTable(nil)
	db.Model((*User)(nil)).CreateTable(nil)
	n := NewNats(db.Model((*event.Outbox)(nil)).TableModel(), sc, db, 1)
	defer n.Close()

	user := User{
		Login: "lll",
	}
	ch := make(chan User, 1)
	subsc, err := sc.Subscribe(simpleSubj, func(msg *stan.Msg) {
		var u User
		err := json.Unmarshal(msg.Data, &u)
		if err != nil {
			t.Error(err)
			ch <- User{}
		}
		ch <- u
	})
	assert.NoError(t, err)
	defer subsc.Close()
	go func(ech chan error) {
		err := <-ech
		assert.NoError(t, err)
		if err != nil {
			ch <- User{}
		}
	}(n.ErrCh)

	var dbEventId uint64
	if err := db.RunInTransaction(func(tx *pg.Tx) error {
		if _, err := tx.Model(&user).Returning("*").Insert(); err != nil {
			return err
		}
		b, err := json.Marshal(user)
		if err != nil {
			return err
		}
		out := event.Outbox{
			Subject: simpleSubj,
			Data:    b,
		}
		if _, err := tx.Model(&out).Returning("*").Insert(); err != nil {
			return err
		}

		dbEventId = out.ID
		return nil
	}); err != nil {
		t.Error(err)
		return
	}

	n.PubCh <- dbEventId
	u := <-ch
	assert.Equal(t, user, u)
}
