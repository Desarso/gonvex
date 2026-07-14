package server

import (
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type appAuthRateWindow struct {
	Started   time.Time
	ExpiresAt time.Time
	Count     int
}

type appAuthRateLimiter struct {
	mu      sync.Mutex
	windows map[string]appAuthRateWindow
}

func (limiter *appAuthRateLimiter) allow(key string, limit int, duration time.Duration) (bool, time.Duration) {
	now := time.Now()
	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	if limiter.windows == nil {
		limiter.windows = map[string]appAuthRateWindow{}
	}
	window, exists := limiter.windows[key]
	if !exists || !now.Before(window.ExpiresAt) {
		if !exists && len(limiter.windows) >= 10_000 {
			for existingKey, existing := range limiter.windows {
				if !now.Before(existing.ExpiresAt) {
					delete(limiter.windows, existingKey)
				}
			}
			if len(limiter.windows) >= 10_000 {
				// Bound memory even under a large distributed source-IP flood.
				for existingKey := range limiter.windows {
					delete(limiter.windows, existingKey)
					break
				}
			}
		}
		limiter.windows[key] = appAuthRateWindow{Started: now, ExpiresAt: now.Add(duration), Count: 1}
		return true, 0
	}
	if window.Count >= limit {
		return false, time.Until(window.ExpiresAt)
	}
	window.Count++
	limiter.windows[key] = window
	return true, 0
}

func (s *Server) allowAppAuthRequest(w http.ResponseWriter, r *http.Request, scope string, limit int, duration time.Duration) bool {
	key := scope + ":" + s.requestRemoteIP(r)
	allowed, retryAfter := s.authRateLimiter.allow(key, limit, duration)
	if allowed {
		return true
	}
	seconds := int(retryAfter.Seconds()) + 1
	w.Header().Set("retry-after", strconv.Itoa(seconds))
	writeJSON(w, http.StatusTooManyRequests, map[string]any{
		"error": "too many authentication requests; try again shortly", "retryAfter": seconds,
	})
	return false
}

func (s *Server) requestRemoteIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	if err != nil || host == "" {
		host = strings.TrimSpace(r.RemoteAddr)
	}
	remoteIP := net.ParseIP(host)
	if remoteIP == nil {
		if host != "" {
			return host
		}
		return "unknown"
	}
	if !s.isTrustedProxy(remoteIP) {
		return remoteIP.String()
	}
	forwarded := strings.Split(r.Header.Get("x-forwarded-for"), ",")
	for index := len(forwarded) - 1; index >= 0; index-- {
		candidate := net.ParseIP(strings.TrimSpace(forwarded[index]))
		if candidate != nil && !s.isTrustedProxy(candidate) {
			return candidate.String()
		}
	}
	return remoteIP.String()
}

func (s *Server) isTrustedProxy(ip net.IP) bool {
	for _, raw := range s.config.TrustedProxyCIDRs {
		raw = strings.TrimSpace(raw)
		if networkIP := net.ParseIP(raw); networkIP != nil {
			if networkIP.Equal(ip) {
				return true
			}
			continue
		}
		_, network, err := net.ParseCIDR(raw)
		if err == nil && network.Contains(ip) {
			return true
		}
	}
	return false
}
