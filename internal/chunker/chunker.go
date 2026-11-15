package chunker

import (
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/yoohooshantanu/vocalflow/internal/logger"
	"github.com/yoohooshantanu/vocalflow/internal/metrics"
	"github.com/yoohooshantanu/vocalflow/internal/model"
)

type SentenceChunkCallback func(chunk *model.SentenceChunk)

// SentenceChunker buffers streaming LLM tokens and yields synthesizable sentence chunks
type SentenceChunker struct {
	mu           sync.Mutex
	sessionID    string
	turnID       string
	buffer       strings.Builder
	wordCount    int
	chunkIndex   int
	isFirstChunk bool
	lastChunk    *model.SentenceChunk
	callback     SentenceChunkCallback
}

func NewSentenceChunker(sessionID, turnID string, cb SentenceChunkCallback) *SentenceChunker {
	return &SentenceChunker{
		sessionID:    sessionID,
		turnID:       turnID,
		isFirstChunk: true,
		callback:     cb,
	}
}

// IngestToken appends an LLM token, checks for sentence/clause boundaries, and flushes chunks
func (c *SentenceChunker) IngestToken(token string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	metrics.DefaultMetrics.AddTokens(1)
	c.buffer.WriteString(token)

	// Count words roughly by spaces
	if strings.Contains(token, " ") {
		c.wordCount++
	}

	currentText := c.buffer.String()
	trimmed := strings.TrimSpace(currentText)
	if len(trimmed) == 0 {
		return
	}

	// First chunk optimization: split aggressively on clause boundary (comma, semicolon, colon)
	// after at least 3 words to achieve ultra-low Time-to-First-Audio (<250ms)
	if c.isFirstChunk && c.wordCount >= 3 {
		if idx := findClauseBoundary(currentText); idx != -1 {
			chunkText := strings.TrimSpace(currentText[:idx+1])
			remaining := currentText[idx+1:]
			c.emitChunk(chunkText, false)
			c.buffer.Reset()
			c.buffer.WriteString(remaining)
			c.wordCount = countWords(remaining)
			c.isFirstChunk = false
			return
		}
	}

	// Normal sentence boundary detection: '.', '!', '?', '\n'
	if idx := findSentenceBoundary(currentText); idx != -1 {
		chunkText := strings.TrimSpace(currentText[:idx+1])
		remaining := currentText[idx+1:]
		c.emitChunk(chunkText, false)
		c.buffer.Reset()
		c.buffer.WriteString(remaining)
		c.wordCount = countWords(remaining)
		c.isFirstChunk = false
		return
	}

	// Safety cap: if an LLM generates a runaway sentence with > 25 words without punctuation,
	// force a chunk at the latest word boundary to avoid high latency
	if c.wordCount >= 25 {
		if lastSpace := strings.LastIndex(currentText, " "); lastSpace != -1 {
			chunkText := strings.TrimSpace(currentText[:lastSpace])
			remaining := currentText[lastSpace+1:]
			c.emitChunk(chunkText, false)
			c.buffer.Reset()
			c.buffer.WriteString(remaining)
			c.wordCount = countWords(remaining)
			c.isFirstChunk = false
		}
	}
}

// Flush emits any remaining buffered text when the LLM stream concludes, or marks the last emitted chunk as final
func (c *SentenceChunker) Flush() {
	c.mu.Lock()
	defer c.mu.Unlock()

	remaining := strings.TrimSpace(c.buffer.String())
	if len(remaining) > 0 {
		c.emitChunk(remaining, true)
		c.buffer.Reset()
		c.wordCount = 0
	} else if c.lastChunk != nil {
		c.lastChunk.IsFinal = true
	}
}

func (c *SentenceChunker) emitChunk(text string, isFinal bool) {
	if len(text) == 0 {
		return
	}

	chunk := &model.SentenceChunk{
		SessionID:    c.sessionID,
		TurnID:       c.turnID,
		Index:        c.chunkIndex,
		Text:         text,
		IsFirstChunk: c.isFirstChunk,
		IsFinal:      isFinal,
		CreatedAt:    time.Now().UTC(),
	}

	c.lastChunk = chunk
	c.chunkIndex++
	metrics.DefaultMetrics.IncSentence()

	logger.L.Debug("sentence_chunk_emitted",
		"session_id", c.sessionID,
		"turn_id", c.turnID,
		"index", chunk.Index,
		"is_first", chunk.IsFirstChunk,
		"text", chunk.Text,
	)

	if c.callback != nil {
		c.callback(chunk)
	}
}

func findSentenceBoundary(s string) int {
	for i, r := range s {
		if r == '.' || r == '!' || r == '?' || r == '\n' {
			// Ensure it's not a common decimal number or abbreviation (e.g. "3.14" or "Dr.")
			if i+1 < len(s) && unicode.IsDigit(rune(s[i+1])) {
				continue
			}
			return i
		}
	}
	return -1
}

func findClauseBoundary(s string) int {
	for i, r := range s {
		if r == ',' || r == ';' || r == ':' || r == '—' {
			return i
		}
	}
	return -1
}

func countWords(s string) int {
	fields := strings.Fields(s)
	return len(fields)
}
