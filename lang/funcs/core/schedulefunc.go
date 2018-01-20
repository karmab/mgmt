// Mgmt
// Copyright (C) 2013-2018+ James Shubin and the project contributors
// Written by James Shubin <james@shubin.ca> and the project contributors
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

// XXX: test with:

// standalone etcd:
// etcd # run a standalone server...
// time ./mgmt run --lang examples/lang/schedule0.mcl --hostname h1 --seeds http://127.0.0.1:2379 --tmp-prefix --no-pgp
// time ./mgmt run --lang examples/lang/schedule0.mcl --hostname h2 --seeds http://127.0.0.1:2379 --tmp-prefix --no-pgp
// time ./mgmt run --lang examples/lang/schedule0.mcl --hostname h3 --seeds http://127.0.0.1:2379 --tmp-prefix --no-pgp

// built-in etcd:
// time ./mgmt run --lang examples/lang/schedule0.mcl --hostname h1 --ideal-cluster-size 1 --tmp-prefix --no-pgp
// time ./mgmt run --lang examples/lang/schedule0.mcl --hostname h2 --seeds http://127.0.0.1:2379 --client-urls http://127.0.0.1:2381 --server-urls http://127.0.0.1:2382 --tmp-prefix --no-pgp
// time ./mgmt run --lang examples/lang/schedule0.mcl --hostname h3 --seeds http://127.0.0.1:2379 --client-urls http://127.0.0.1:2383 --server-urls http://127.0.0.1:2384 --tmp-prefix --no-pgp

package core // TODO: should this be in its own individual package?

import (
	"context"
	"fmt"

	"github.com/purpleidea/mgmt/etcd/scheduler" // TODO: is it okay to import this without abstraction?
	"github.com/purpleidea/mgmt/lang/funcs"
	"github.com/purpleidea/mgmt/lang/interfaces"
	"github.com/purpleidea/mgmt/lang/types"

	errwrap "github.com/pkg/errors"
)

func init() {
	funcs.Register("schedule", func() interfaces.Func { return &ScheduleFunc{} }) // must register the func and name
}

// ScheduleFunc is special function which determines where code should run in
// the cluster.
type ScheduleFunc struct {
	init *interfaces.Init

	namespace string
	scheduler *scheduler.Result

	last   types.Value
	result types.Value // last calculated output

	watchChan chan *schedulerResult
	closeChan chan struct{}
}

// Validate makes sure we've built our struct properly. It is usually unused for
// normal functions that users can use directly.
func (obj *ScheduleFunc) Validate() error {
	return nil
}

// Info returns some static info about itself.
func (obj *ScheduleFunc) Info() *interfaces.Info {
	return &interfaces.Info{
		Pure: false, // definitely false
		Memo: false,
		// output is list of hostnames chosen
		// TODO: we could make this polymorphic so that it would allow a
		// struct with whichever opts parameters are missing be defaults
		Sig: types.NewType("func(namespace str, opts struct{strategy str; max int}) []str"),
		Err: obj.Validate(),
	}
}

// Init runs some startup code for this fact.
func (obj *ScheduleFunc) Init(init *interfaces.Init) error {
	obj.init = init
	obj.watchChan = make(chan *schedulerResult)
	obj.closeChan = make(chan struct{})
	return nil
}

// Stream returns the changing values that this func has over time.
func (obj *ScheduleFunc) Stream() error {
	defer close(obj.init.Output) // the sender closes
	for {
		select {
		// TODO: should this first chan be run as a priority channel to
		// avoid some sort of glitch? is that even possible? can our
		// hostname check with reality (below) fix that?
		case input, ok := <-obj.init.Input:
			if !ok {
				obj.init.Input = nil // don't infinite loop back
				continue             // no more inputs, but don't return!
			}
			//if err := input.Type().Cmp(obj.Info().Sig.Input); err != nil {
			//	return errwrap.Wrapf(err, "wrong function input")
			//}

			if obj.last != nil && input.Cmp(obj.last) == nil {
				continue // value didn't change, skip it
			}
			obj.last = input // store for next

			namespace := input.Struct()["namespace"].Str()
			if namespace == "" {
				return fmt.Errorf("can't use an empty namespace")
			}
			opts := input.Struct()["opts"].Struct()

			if obj.init.Debug {
				obj.init.Logf("namespace: %s", namespace)
			}

			schedulerOpts := []scheduler.Option{}
			// don't add bad or zero-value options

			strategy := opts["strategy"].Str()
			if strategy != "" {
				if obj.init.Debug {
					obj.init.Logf("opts: strategy: %s", strategy)
				}
				schedulerOpts = append(schedulerOpts, scheduler.StrategyKind(strategy))
			}
			max := int(opts["max"].Int()) // TODO: check for overflow
			if max > 0 {
				if obj.init.Debug {
					obj.init.Logf("opts: max: %d", max)
				}
				schedulerOpts = append(schedulerOpts, scheduler.MaxCount(max))
			}

			// TODO: support changing the namespace over time...
			// TODO: possibly removing our stored value there first!
			if obj.namespace == "" {
				obj.namespace = namespace // store it

				if obj.init.Debug {
					obj.init.Logf("starting scheduler...")
				}
				var err error
				obj.scheduler, err = obj.init.World.Scheduler(obj.namespace, schedulerOpts...)
				if err != nil {
					return errwrap.Wrapf(err, "can't create scheduler")
				}

				// process the stream of scheduling output...
				go func() {
					defer close(obj.watchChan)
					ctx, cancel := context.WithCancel(context.Background())
					go func() {
						defer cancel() // unblock Next()
						defer obj.scheduler.Shutdown()
						select {
						case <-obj.closeChan:
							return
						}
					}()
					for {
						hosts, err := obj.scheduler.Next(ctx)
						select {
						case obj.watchChan <- &schedulerResult{
							hosts: hosts,
							err:   err,
						}:

						case <-obj.closeChan:
							return
						}
					}
				}()

			} else if obj.namespace != namespace {
				return fmt.Errorf("can't change namespace, previously: `%s`", obj.namespace)
			}

			continue // we send values on the watch chan, not here!

		case schedulerResult, ok := <-obj.watchChan:
			if !ok { // closed
				// XXX: maybe etcd reconnected? (fix etcd implementation)

				// XXX: if we close, perhaps the engine is
				// switching etcd hosts and we should retry?
				// maybe instead we should get an "etcd
				// reconnect" signal, and the lang will restart?
				return nil
			}
			if err := schedulerResult.err; err != nil {
				if err == scheduler.ErrEndOfResults {
					//return nil // TODO: we should probably fix the reconnect issue and use this here
					return fmt.Errorf("scheduler shutdown, reconnect bug?") // XXX: fix etcd reconnects
				}
				return errwrap.Wrapf(err, "channel watch failed on `%s`", obj.namespace)
			}

			if obj.init.Debug {
				obj.init.Logf("got hosts: %+v", schedulerResult.hosts)
			}

			var result types.Value
			l := types.NewList(obj.Info().Sig.Out)
			for _, val := range schedulerResult.hosts {
				if err := l.Add(&types.StrValue{V: val}); err != nil {
					return errwrap.Wrapf(err, "list could not add val: `%s`", val)
				}
			}
			result = l // set list as result

			if obj.init.Debug {
				obj.init.Logf("result: %+v", result)
			}

			// if the result is still the same, skip sending an update...
			if obj.result != nil && result.Cmp(obj.result) == nil {
				continue // result didn't change
			}
			obj.result = result // store new result

		case <-obj.closeChan:
			return nil
		}

		select {
		case obj.init.Output <- obj.result: // send
			// pass
		case <-obj.closeChan:
			return nil
		}
	}
}

// Close runs some shutdown code for this fact and turns off the stream.
func (obj *ScheduleFunc) Close() error {
	close(obj.closeChan)
	return nil
}

// schedulerResult combines our internal events into a single message packet.
type schedulerResult struct {
	hosts []string
	err   error
}
