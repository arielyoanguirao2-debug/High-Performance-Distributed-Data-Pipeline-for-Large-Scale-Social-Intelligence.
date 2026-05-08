package auth

// ============================================================
// Package auth — Module 1 : Authentification & Rate-Limiting
// ============================================================
// Responsabilités :
//   - Injection du Bearer Token sur chaque requête HTTP
//   - Lecture des headers x-rate-limit-* retournés par l'API X
//   - Back-off automatique avant épuisement du quota
//   - Retry transparent sur HTTP 429
// ============================================================

import (
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// AuthClient wraps http.Client avec gestion du Bearer token et du rate-limit.
// Toutes les requêtes vers l'API X doivent passer par DoRequest.
type AuthClient struct {
	httpClient     *http.Client
	bearerToken    string

	mu             sync.Mutex
	remainingCalls int       // quota restant dans la fenêtre courante
	resetAt        time.Time // instant de réinitialisation du quota
}

// NewAuthClient crée un AuthClient prêt à l'emploi.
// bearerToken : Bearer Token X API v2 (obligatoire).
func NewAuthClient(bearerToken string) *AuthClient {
	return &AuthClient{
		httpClient:     &http.Client{Timeout: 15 * time.Second},
		bearerToken:    bearerToken,
		remainingCalls: 450, // quota par défaut : 450 req / 15 min (search endpoint)
		resetAt:        time.Now().Add(15 * time.Minute),
	}
}

// DoRequest exécute une requête HTTP en injectant le header Authorization,
// en respectant les rate-limits et en réessayant automatiquement sur 429.
//
// Comportement :
//  1. Si la fenêtre de 15 min est expirée → réinitialise le compteur
//  2. Si budget ≤ 5 → dort jusqu'à la fin de la fenêtre (+2s de marge)
//  3. Décrémente le compteur local AVANT la requête
//  4. Après réponse → met à jour le compteur depuis les headers HTTP
//  5. Sur 429 → lit retry-after et réessaie (un niveau de récursion)
func (a *AuthClient) DoRequest(req *http.Request) (*http.Response, error) {
	a.mu.Lock()

	// Réinitialisation de la fenêtre si elle est expirée
	if time.Now().After(a.resetAt) {
		a.remainingCalls = 450
		a.resetAt = time.Now().Add(15 * time.Minute)
	}

	// Back-off préventif : on s'arrête avant d'atteindre 0
	if a.remainingCalls <= 5 {
		waitDuration := time.Until(a.resetAt) + 2*time.Second
		log.Printf("[RateLimit] Budget épuisé (%d restants). Pause de %s jusqu'à la réinitialisation.\n",
			a.remainingCalls, waitDuration.Round(time.Second))
		a.mu.Unlock()
		time.Sleep(waitDuration)
		a.mu.Lock()
		a.remainingCalls = 450
		a.resetAt = time.Now().Add(15 * time.Minute)
	}

	a.remainingCalls--
	a.mu.Unlock()

	// Injection du Bearer Token et du User-Agent
	req.Header.Set("Authorization", "Bearer "+a.bearerToken)
	req.Header.Set("User-Agent", "XGlobalScanner-Go/2.0")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http.Do: %w", err)
	}

	// Mise à jour du compteur depuis les headers de la réponse API
	a.mu.Lock()
	if v := resp.Header.Get("x-rate-limit-remaining"); v != "" {
		if n, err2 := strconv.Atoi(v); err2 == nil {
			a.remainingCalls = n
		}
	}
	if v := resp.Header.Get("x-rate-limit-reset"); v != "" {
		if ts, err2 := strconv.ParseInt(v, 10, 64); err2 == nil {
			a.resetAt = time.Unix(ts, 0)
		}
	}
	a.mu.Unlock()

	// Gestion explicite du 429 Too Many Requests
	if resp.StatusCode == http.StatusTooManyRequests {
		resp.Body.Close()
		retryAfter := 60 * time.Second
		if v := resp.Header.Get("retry-after"); v != "" {
			if secs, err2 := strconv.Atoi(v); err2 == nil {
				retryAfter = time.Duration(secs) * time.Second
			}
		}
		log.Printf("[RateLimit] HTTP 429 reçu. Retry dans %s.\n", retryAfter)
		time.Sleep(retryAfter)
		return a.DoRequest(req) // retry récursif (un seul niveau)
	}

	return resp, nil
}
