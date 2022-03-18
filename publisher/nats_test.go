package publisher

import (
	"encoding/json"
	"github.com/dovbysh/go-utils/testing/tlog"
	"github.com/dovbysh/outboxer/events"
	"github.com/dovbysh/tests_common"
	"github.com/go-pg/pg/v9/orm"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

var (
	sc stan.Conn
	db *pg.DB
)

type User struct {
	Id    uint64
	Login string
}

const simpleSubj = "simple"
const chanLenSub = "chanLenSub"

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
	t.Run("publishUnpublished", publishUnpublished)
	t.Run("chanLen", chanLen)
}

func simple(t *testing.T) {
	db.Model((*events.Outbox)(nil)).Table("_natss_outbox").CreateTable(&orm.CreateTableOptions{IfNotExists: true})
	db.Model((*User)(nil)).CreateTable(nil)
	n := NewNats("_natss_outbox", sc, db, 1, 1)
	defer n.Close()

	user := User{
		Login: "lll",
	}
	ch := make(chan User, 1)
	var evId uint64
	subsc, err := sc.Subscribe(simpleSubj, func(msg *stan.Msg) {
		var u User
		evId = msg.Sequence
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
		out := events.Outbox{
			Subject: simpleSubj,
			Data:    b,
		}
		if _, err := tx.Model(&out).Table("_natss_outbox").Returning("*").Insert(); err != nil {
			return err
		}

		dbEventId = out.ID
		return nil
	}); err != nil {
		t.Error(err)
		return
	}

	n.PubCh <- dbEventId
	assert.NotEmpty(t, dbEventId)
	u := <-ch

	assert.Equal(t, user, u)
	assert.NotEmpty(t, evId)
}

func publishUnpublished(t *testing.T) {
	db.Model((*events.Outbox)(nil)).Table("_natss_outbox").CreateTable(&orm.CreateTableOptions{IfNotExists: true})
	db.Query(nil, "INSERT INTO _natss_outbox (\"id\", \"published\", \"published_nuid\", \"created_at\", \"published_at\", \"subject\", \"data\") VALUES (DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, 'simple', '\\x7b224964223a312c224c6f67696e223a226c6c6c227d')")
	n := NewNats("_natss_outbox", sc, db, 1, 1)
	defer n.Close()

	user := User{
		Id:    1,
		Login: "lll",
	}
	ch := make(chan User, 1)
	var evId uint64
	subsc, err := sc.Subscribe(simpleSubj, func(msg *stan.Msg) {
		var u User
		evId = msg.Sequence
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

	_, done := n.PublishUnPublished()
	<-done
	u := <-ch

	assert.Equal(t, user, u)
	assert.NotEmpty(t, evId)
}

func chanLen(t *testing.T) {
	db.Model((*events.Outbox)(nil)).Table("_natss_outbox").CreateTable(&orm.CreateTableOptions{IfNotExists: true})
	db.Model((*User)(nil)).CreateTable(nil)
	const chanLength = 2
	n := NewNats("_natss_outbox", sc, db, 0, chanLength)
	defer n.Close()

	user := User{
		Login: "lll",
	}

	var wg sync.WaitGroup
	var maxId uint64
	subsc, err := sc.Subscribe(chanLenSub, func(msg *stan.Msg) {
		defer wg.Done()
		var u User
		err := json.Unmarshal(msg.Data, &u)
		assert.NoError(t, err)
		if maxId < u.Id {
			maxId = u.Id
		}
	})
	assert.NoError(t, err)
	defer subsc.Close()
	go func(ech chan error) {
		err := <-ech
		assert.NoError(t, err)
	}(n.ErrCh)

	wg.Add(chanLength)
	for i := 0; i < chanLength; i++ {
		var dbEventId uint64
		if err := db.RunInTransaction(func(tx *pg.Tx) error {
			user.Id = uint64(i + 1)
			b, err := json.Marshal(user)
			if err != nil {
				return err
			}
			out := events.Outbox{
				Subject: chanLenSub,
				Data:    b,
			}
			if _, err := tx.Model(&out).Table("_natss_outbox").Returning("*").Insert(); err != nil {
				return err
			}

			dbEventId = out.ID
			return nil
		}); err != nil {
			t.Error(err)
			return
		}
		n.PubCh <- dbEventId
		assert.NotEmpty(t, dbEventId)
	}
	assert.Equal(t, chanLength, len(n.PubCh))
	assert.Equal(t, uint64(0), maxId)

	go n.Publish(n.PubCh, n.ErrCh)

	wg.Wait()
	assert.Equal(t, 0, len(n.PubCh))
	assert.Equal(t, uint64(chanLength), maxId)
}
