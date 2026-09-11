package skein

import (
	"sync"
	"time"
)

// EngineState describes this process's startup and shutdown lifecycle.
type EngineState string

const (
	EngineNew      EngineState = "new"
	EngineStarting EngineState = "starting"
	EngineRunning  EngineState = "running"
	EngineStopping EngineState = "stopping"
	EngineStopped  EngineState = "stopped"
)

// LoopHealth distinguishes loop progress from successful database work. Idle
// rounds advance LastTick only; successful database rounds clear the error fields.
type LoopHealth struct {
	LastTick          time.Time
	LastSuccess       time.Time
	ConsecutiveErrors int
	LastError         string
}

// Health describes this process, independently of the shared queue in Stats.
// Its fields are sampled separately, not as one atomic snapshot of the engine.
type Health struct {
	Instance         string
	State            EngineState
	WorkerEnabled    bool
	SchedulerEnabled bool
	Concurrency      int
	SlotsUsed        int // includes preparation, settlement and Observer callbacks
	Leases           int // still tracked for renewal; may be less than SlotsUsed
	Claim            LoopHealth
	Heartbeat        LoopHealth
	Scan             LoopHealth
}

// Health does no I/O and does not wait for startup or shutdown. EngineStopped
// means Shutdown finished; an executor ignoring cancellation may still hold a slot.
func (e *Engine) Health() Health {
	state := EngineNew
	switch {
	case e.shutting.Load():
		state = EngineStopping
		select {
		case <-e.shutDone:
			state = EngineStopped
		default:
		}
	case e.ready.Load():
		state = EngineRunning
	case e.started.Load():
		state = EngineStarting
	}
	e.inflight.mu.Lock()
	leases := len(e.inflight.m)
	e.inflight.mu.Unlock()
	return Health{
		Instance: e.owner, State: state,
		WorkerEnabled: !e.cfg.DisableWorker, SchedulerEnabled: !e.cfg.DisableScheduler,
		Concurrency: cap(e.slots), SlotsUsed: len(e.slots), Leases: leases,
		Claim: e.claimHealth.snapshot(), Heartbeat: e.heartbeatHealth.snapshot(), Scan: e.scanHealth.snapshot(),
	}
}

// Health uses independent short locks: a host may read it from an Observer while
// Shutdown holds lifecycle waiting for that same worker. Never nest these locks.
type loopHealth struct {
	mu    sync.Mutex
	value LoopHealth
}

func (h *loopHealth) tick() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.value.LastTick = time.Now()
}

func (h *loopHealth) finish(err error) {
	message := ""
	if err != nil {
		message = cleanMessage(err.Error())
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if err != nil {
		h.value.ConsecutiveErrors++
		h.value.LastError = message
		return
	}
	h.value.LastSuccess = time.Now()
	h.value.ConsecutiveErrors = 0
	h.value.LastError = ""
}

func (h *loopHealth) snapshot() LoopHealth {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.value
}
