package publisher

import (
	"fmt"
	"github.com/dovbysh/outboxer/events"
	"github.com/go-pg/pg/v9"
	"github.com/nats-io/stan.go"
	"sync"
	"time"
)

type Nats struct {
	tableName     string
	sc            stan.Conn
	db            *pg.DB
	PubCh         chan uint64
	ErrCh         chan error
	wg            sync.WaitGroup
	numPublishers int
}

func NewNats(tableName string, sc stan.Conn, db *pg.DB, numPublishers int) *Nats {
	p := &Nats{
		tableName:     tableName,
		sc:            sc,
		db:            db,
		PubCh:         make(chan uint64, numPublishers),
		ErrCh:         make(chan error, numPublishers),
		wg:            sync.WaitGroup{},
		numPublishers: numPublishers,
	}
	p.wg.Add(numPublishers)
	for i := 0; i < numPublishers; i++ {
		go func(p *Nats) {
			defer p.wg.Done()
			p.Publish(p.PubCh, p.ErrCh)
		}(p)
	}

	return p
}

func (p *Nats) Publish(ch <-chan uint64, ech chan<- error) {
	for ID := range ch {
		err := p.db.RunInTransaction(func(tx *pg.Tx) error {
			var out events.Outbox
			if err := tx.Model(&out).
				Table(p.tableName).
				For("UPDATE").
				Where("id = ? and published = false", ID).
				Select(); err != nil {
				return err
			}
			var wg sync.WaitGroup
			var accError error
			wg.Add(1)
			if _, err := p.sc.PublishAsync(out.Subject, out.Data, func(nuid string, err error) {
				defer wg.Done()
				if err != nil {
					accError = err
					return
				}
				r, err := tx.Model(&out).
					Table(p.tableName).
					Set("published = true").
					Set("published_at = ?", time.Now()).
					Set("published_nuid = ?", nuid).
					Where("id = ? and published = false", ID).
					Update()
				if err != nil {
					accError = err
					return
				}
				if r.RowsAffected() != 1 {
					accError = fmt.Errorf("row affected !=1, RowsAffected: %d, out: %#v", r.RowsAffected(), out)
					return
				}

			}); err != nil {
				wg.Done()
				return err
			}

			wg.Wait()
			if accError != nil {
				return accError
			}
			return nil
		})
		if err != nil {
			ech <- err
			continue
		}

	}
}

func (p *Nats) Close() {
	close(p.PubCh)
	p.wg.Wait()
	close(p.ErrCh)
}

func (p *Nats) PublishUnPublished() (<-chan error, <-chan struct{}) {
	var events []events.Outbox
	PubCh := p.PubCh
	errCh := p.ErrCh
	var wg sync.WaitGroup
	if p.numPublishers < 1 {
		PubCh = make(chan uint64)
		errCh = make(chan error)
		wg.Add(1)
		go func(ch <-chan uint64, ech chan<- error) {
			wg.Done()
			p.Publish(ch, ech)
		}(PubCh, errCh)
	}

	done := make(chan struct{})

	go func() {
		defer func() { done <- struct{}{} }()
		var maxId uint64
		var err error
		for {
			err = p.db.Model(&events).
				Table(p.tableName).
				Where("published=false and id > ?", maxId).
				Order("id", "created_at").
				Limit(p.numPublishers + 1).
				Select()
			if err != nil && err != pg.ErrNoRows {
				break
			}
			if len(events) == 0 {
				break
			}
			for _, ev := range events {
				if maxId < ev.ID {
					maxId = ev.ID
				}
				p.PubCh <- ev.ID
			}
		}

		if p.numPublishers < 1 {
			close(PubCh)
			wg.Wait()
			close(errCh)
		}
	}()

	return errCh, done
}
