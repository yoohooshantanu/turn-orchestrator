package pipeline

import (
	"sync"
	"time"

	"github.com/yoohooshantanu/vocalflow/internal/logger"
)

// SessionManager maintains multi-tenant active voice conversation sessions
type SessionManager struct {
	mu       sync.RWMutex
	sessions map[string]*VoiceSession
	ttl      time.Duration
	stopCh   chan struct{}
	stopOnce sync.Once
}

func NewSessionManager(ttl time.Duration) *SessionManager {
	if ttl <= 0 {
		ttl = 30 * time.Minute
	}

	sm := &SessionManager{
		sessions: make(map[string]*VoiceSession),
		ttl:      ttl,
		stopCh:   make(chan struct{}),
	}

	go sm.cleanupLoop()
	return sm
}

func (sm *SessionManager) GetOrCreate(sessionID string) *VoiceSession {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	s, ok := sm.sessions[sessionID]
	if !ok {
		s = NewVoiceSession(sessionID)
		sm.sessions[sessionID] = s
	}
	return s
}

func (sm *SessionManager) Get(sessionID string) (*VoiceSession, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	s, ok := sm.sessions[sessionID]
	return s, ok
}

func (sm *SessionManager) Delete(sessionID string) {
	sm.mu.Lock()
	s, ok := sm.sessions[sessionID]
	if ok {
		delete(sm.sessions, sessionID)
		s.Close()
	}
	sm.mu.Unlock()
}

func (sm *SessionManager) ActiveCount() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.sessions)
}

func (sm *SessionManager) cleanupLoop() {
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-sm.stopCh:
			return
		case <-ticker.C:
			sm.mu.Lock()
			// Bound sessions table to 10,000 active sessions
			if len(sm.sessions) > 10000 {
				count := 0
				for id, s := range sm.sessions {
					if s.isClosed {
						delete(sm.sessions, id)
						count++
						if count >= 2000 {
							break
						}
					}
				}
			}
			sm.mu.Unlock()
		}
	}
}

func (sm *SessionManager) Stop() {
	sm.stopOnce.Do(func() {
		close(sm.stopCh)
		sm.mu.Lock()
		for _, s := range sm.sessions {
			s.Close()
		}
		sm.sessions = make(map[string]*VoiceSession)
		sm.mu.Unlock()
		logger.L.Info("session_manager_stopped")
	})
}
