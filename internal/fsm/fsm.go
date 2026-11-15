package fsm

import (
	"fmt"
	"sync"
	"time"

	"github.com/yoohooshantanu/vocalflow/internal/logger"
	"github.com/yoohooshantanu/vocalflow/internal/model"
)

type StateChangeHook func(from, to model.ConversationState)

// TurnFSM manages the conversational turn state machine for an active voice session
type TurnFSM struct {
	mu           sync.RWMutex
	sessionID    string
	current      model.ConversationState
	lastChange   time.Time
	stateHooks   []StateChangeHook
	allowedTrans map[model.ConversationState]map[model.ConversationState]bool
}

func NewTurnFSM(sessionID string) *TurnFSM {
	fsm := &TurnFSM{
		sessionID:  sessionID,
		current:    model.StateIdle,
		lastChange: time.Now(),
		stateHooks: make([]StateChangeHook, 0),
		allowedTrans: map[model.ConversationState]map[model.ConversationState]bool{
			model.StateIdle: {
				model.StateListening: true,
			},
			model.StateListening: {
				model.StateThinking: true,
				model.StateIdle:     true,
			},
			model.StateThinking: {
				model.StateSpeaking:    true,
				model.StateInterrupted: true,
				model.StateListening:   true,
				model.StateIdle:        true,
			},
			model.StateSpeaking: {
				model.StateInterrupted: true,
				model.StateDraining:    true,
				model.StateListening:   true,
				model.StateIdle:        true,
			},
			model.StateDraining: {
				model.StateListening:   true,
				model.StateInterrupted: true,
				model.StateIdle:        true,
			},
			model.StateInterrupted: {
				model.StateListening: true,
				model.StateIdle:      true,
			},
		},
	}
	return fsm
}

func (f *TurnFSM) OnStateChange(hook StateChangeHook) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stateHooks = append(f.stateHooks, hook)
}

func (f *TurnFSM) Current() model.ConversationState {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.current
}

func (f *TurnFSM) Is(s model.ConversationState) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.current == s
}

// Transition attempts to advance the conversation state atomically
func (f *TurnFSM) Transition(target model.ConversationState) error {
	f.mu.Lock()
	from := f.current

	if from == target {
		f.mu.Unlock()
		return nil
	}

	allowed := f.allowedTrans[from][target]
	if !allowed {
		f.mu.Unlock()
		return fmt.Errorf("%w: cannot transition from %s to %s", model.ErrInvalidStateTransition, from, target)
	}

	f.current = target
	f.lastChange = time.Now()
	hooks := make([]StateChangeHook, len(f.stateHooks))
	copy(hooks, f.stateHooks)
	f.mu.Unlock()

	logger.L.Debug("fsm_state_transition",
		"session_id", f.sessionID,
		"from", string(from),
		"to", string(target),
	)

	for _, h := range hooks {
		h(from, target)
	}

	return nil
}
