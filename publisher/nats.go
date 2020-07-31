package publisher

import (
	"fmt"
	"github.com/dovbysh/go-outboxer/event"
	"github.com/go-pg/pg/v9"
	"github.com/go-pg/pg/v9/orm"
	"github.com/nats-io/stan.go"
	"sync"
	"time"
)

type Nats struct {
	outBoxModel orm.TableModel
	sc          stan.Conn
	db          *pg.DB
	PubCh       chan uint64
	ErrCh       chan error
	wg          sync.WaitGroup
}

func NewNats(outBoxModel orm.TableModel, sc stan.Conn, db *pg.DB, numPublishers int) *Nats {
	p := &Nats{
		outBoxModel: outBoxModel,
		sc:          sc,
		db:          db,
		PubCh:       make(chan uint64, numPublishers),
		ErrCh:       make(chan error, numPublishers),
		wg:          sync.WaitGroup{},
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
			var out event.Outbox
			m := p.outBoxModel
			if err := tx.Model(m).
				For("UPDATE").
				Where("id = ? and published = false", ID).
				Select(&out); err != nil {
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
				m := p.outBoxModel
				r, err := tx.Model(m).
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
