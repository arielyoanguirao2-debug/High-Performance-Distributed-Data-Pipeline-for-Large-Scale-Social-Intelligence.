package extractor

// ============================================================
// Package extractor — transform.go
// ============================================================
// Responsabilités :
//   - Définition des types API bruts (APITweetRaw, SearchResponse…)
//   - Définition du type RawTweet (tweet enrichi, prêt pour le disque)
//   - Transformer : convertit une SearchResponse en []RawTweet
//     • Détection du type de contenu (Texte / Image / Vidéo / GIF)
//     • Extraction des hashtags depuis le bloc entities
//     • Classification du style de post
//     • Calcul du taux d'engagement à la volée
//     • Résolution géographique via GeoMapper
// ============================================================

import (
	"math"
	"strings"
	"time"
)

// ── Types API bruts ───────────────────────────────────────────────────────────

// APIPublicMetrics reflète le bloc "public_metrics" retourné par l'API X v2.
type APIPublicMetrics struct {
	RetweetCount    int64 `json:"retweet_count"`
	ReplyCount      int64 `json:"reply_count"`
	LikeCount       int64 `json:"like_count"`
	QuoteCount      int64 `json:"quote_count"`
	BookmarkCount   int64 `json:"bookmark_count"`
	ImpressionCount int64 `json:"impression_count"`
}

// APIAttachments liste les media_keys attachés à un tweet.
type APIAttachments struct {
	MediaKeys []string `json:"media_keys"`
}

// APIEntities porte les entités structurées (hashtags, URLs, mentions).
type APIEntities struct {
	Hashtags []struct {
		Tag string `json:"tag"`
	} `json:"hashtags"`
}

// APIGeo représente le bloc géo retourné par l'API X (place_id + coordonnées).
type APIGeo struct {
	PlaceID     string `json:"place_id"`
	Coordinates *struct {
		Type        string    `json:"type"`
		Coordinates []float64 `json:"coordinates"`
	} `json:"coordinates"`
}

// APIPlace est l'expansion "place" retournée dans le bloc includes.
type APIPlace struct {
	ID          string `json:"id"`
	FullName    string `json:"full_name"`
	CountryCode string `json:"country_code"`
	Country     string `json:"country"`
	PlaceType   string `json:"place_type"`
}

// APITweetRaw est le tweet brut tel que retourné par l'API X v2,
// avant toute transformation.
type APITweetRaw struct {
	ID             string           `json:"id"`
	Text           string           `json:"text"`
	AuthorID       string           `json:"author_id"`
	CreatedAt      string           `json:"created_at"`
	ConversationID string           `json:"conversation_id"`
	Lang           string           `json:"lang"`
	PublicMetrics  APIPublicMetrics `json:"public_metrics"`
	Attachments    *APIAttachments  `json:"attachments,omitempty"`
	Entities       *APIEntities     `json:"entities,omitempty"`
	Geo            *APIGeo          `json:"geo,omitempty"`
	ReferencedTweets []struct {
		Type string `json:"type"`
		ID   string `json:"id"`
	} `json:"referenced_tweets,omitempty"`
}

// APIMedia décrit un objet media (photo, video, animated_gif).
type APIMedia struct {
	MediaKey      string `json:"media_key"`
	Type          string `json:"type"` // "photo" | "video" | "animated_gif"
	PublicMetrics struct {
		ViewCount int64 `json:"view_count"`
	} `json:"public_metrics"`
}

// SearchResponse est la structure de premier niveau d'une réponse
// /2/tweets/search/recent de l'API X v2.
type SearchResponse struct {
	Data     []APITweetRaw `json:"data"`
	Includes struct {
		Media  []APIMedia `json:"media"`
		Places []APIPlace `json:"places"`
	} `json:"includes"`
	Meta struct {
		NewestID    string `json:"newest_id"`
		OldestID    string `json:"oldest_id"`
		ResultCount int    `json:"result_count"`
		NextToken   string `json:"next_token"`
	} `json:"meta"`
}

// ── Type de sortie enrichi ────────────────────────────────────────────────────

// RawTweet est le tweet entièrement enrichi, prêt pour l'écriture sur disque.
type RawTweet struct {
	ID             string    `json:"id"`
	Text           string    `json:"text"`
	AuthorID       string    `json:"author_id"`
	CreatedAt      time.Time `json:"created_at"`
	ConversationID string    `json:"conversation_id"`
	Lang           string    `json:"lang"`

	// Métriques publiques brutes
	LikeCount     int64 `json:"like_count"`
	RetweetCount  int64 `json:"retweet_count"`
	ReplyCount    int64 `json:"reply_count"`
	QuoteCount    int64 `json:"quote_count"`
	ViewCount     int64 `json:"view_count"`
	BookmarkCount int64 `json:"bookmark_count"`

	// Champs dérivés calculés à la volée
	ContentType    string   `json:"content_type"`   // "Text" | "Image" | "Video" | "GIF" | "Mixed"
	PostStyle      string   `json:"post_style"`     // "Short Video" | "Thread" | "Quote Tweet" …
	Hashtags       []string `json:"hashtags"`
	EngagementRate float64  `json:"engagement_rate"` // (likes+RT+replies+quotes)/vues × 100

	// Géolocalisation résolue
	Geo GeoInfo `json:"geo"`
}

// ── Transformer ───────────────────────────────────────────────────────────────

// Transformer convertit les réponses brutes de l'API X en []RawTweet enrichis.
type Transformer struct {
	geo *GeoMapper
}

// NewTransformer retourne un Transformer initialisé avec son GeoMapper.
func NewTransformer() *Transformer {
	return &Transformer{geo: NewGeoMapper()}
}

// Transform convertit une SearchResponse en slice de RawTweet.
// Pour chaque tweet :
//  1. Construit les métriques publiques
//  2. Résout la géolocalisation (place_id → fallback langue)
//  3. Détecte le type de contenu via les media_keys
//  4. Extrait les hashtags depuis le bloc entities
//  5. Classifie le style de post
//  6. Calcule le taux d'engagement
func (t *Transformer) Transform(sr *SearchResponse) []RawTweet {
	// Index media_key → APIMedia pour lookup O(1)
	mediaMap := make(map[string]APIMedia, len(sr.Includes.Media))
	for _, m := range sr.Includes.Media {
		mediaMap[m.MediaKey] = m
	}
	// Index place_id → APIPlace pour lookup O(1)
	placeMap := make(map[string]APIPlace, len(sr.Includes.Places))
	for _, pl := range sr.Includes.Places {
		placeMap[pl.ID] = pl
	}

	results := make([]RawTweet, 0, len(sr.Data))
	for _, raw := range sr.Data {
		tweet := RawTweet{
			ID:             raw.ID,
			Text:           raw.Text,
			AuthorID:       raw.AuthorID,
			ConversationID: raw.ConversationID,
			Lang:           raw.Lang,
			LikeCount:      raw.PublicMetrics.LikeCount,
			RetweetCount:   raw.PublicMetrics.RetweetCount,
			ReplyCount:     raw.PublicMetrics.ReplyCount,
			QuoteCount:     raw.PublicMetrics.QuoteCount,
			BookmarkCount:  raw.PublicMetrics.BookmarkCount,
		}

		// Parsing du timestamp RFC3339
		if ts, err := time.Parse(time.RFC3339, raw.CreatedAt); err == nil {
			tweet.CreatedAt = ts
		}

		// ── Géolocalisation ──────────────────────────────────────────
		tweet.Geo = t.resolveGeo(raw, placeMap)

		// ── Type de contenu + comptage des vues ──────────────────────
		ct, views := t.detectContentType(raw, mediaMap)
		tweet.ContentType = ct
		tweet.ViewCount = views

		// ── Hashtags ─────────────────────────────────────────────────
		tweet.Hashtags = t.extractHashtags(raw)

		// ── Style de post ────────────────────────────────────────────
		tweet.PostStyle = t.classifyStyle(raw, ct)

		// ── Taux d'engagement : (likes+RT+replies+quotes) / vues × 100
		if tweet.ViewCount > 0 {
			interactions := float64(tweet.LikeCount + tweet.RetweetCount +
				tweet.ReplyCount + tweet.QuoteCount)
			tweet.EngagementRate = math.Round(
				(interactions/float64(tweet.ViewCount)*100)*100,
			) / 100
		}

		results = append(results, tweet)
	}
	return results
}

// resolveGeo détermine le GeoInfo d'un tweet selon l'ordre de priorité :
//  1. Expansion place (place_id dans includes) — le plus précis
//  2. Heuristique sur la langue du tweet
func (t *Transformer) resolveGeo(raw APITweetRaw, placeMap map[string]APIPlace) GeoInfo {
	if raw.Geo != nil && raw.Geo.PlaceID != "" {
		if pl, ok := placeMap[raw.Geo.PlaceID]; ok && pl.CountryCode != "" {
			return t.geo.Lookup(pl.CountryCode)
		}
	}
	// Fallback : heuristique langue
	return t.geo.LookupByLang(raw.Lang)
}

// detectContentType infère le type de média et agrège les vues.
// Retourne ("Text"|"Image"|"Video"|"GIF"|"Mixed", totalViews).
func (t *Transformer) detectContentType(
	raw APITweetRaw,
	mediaMap map[string]APIMedia,
) (string, int64) {
	if raw.Attachments == nil || len(raw.Attachments.MediaKeys) == 0 {
		return "Text", raw.PublicMetrics.ImpressionCount
	}

	hasVideo, hasPhoto, hasGIF := false, false, false
	var totalViews int64

	for _, mk := range raw.Attachments.MediaKeys {
		m, ok := mediaMap[mk]
		if !ok {
			continue
		}
		totalViews += m.PublicMetrics.ViewCount
		switch m.Type {
		case "video":
			hasVideo = true
		case "photo":
			hasPhoto = true
		case "animated_gif":
			hasGIF = true
		}
	}

	// Si le nombre de vues vidéo est nul, on replie sur les impressions
	if totalViews == 0 {
		totalViews = raw.PublicMetrics.ImpressionCount
	}

	switch {
	case hasVideo && hasPhoto:
		return "Mixed", totalViews
	case hasVideo:
		return "Video", totalViews
	case hasGIF:
		return "GIF", totalViews
	case hasPhoto:
		return "Image", totalViews
	default:
		return "Text", totalViews
	}
}

// extractHashtags extrait les hashtags depuis le bloc entities du tweet.
func (t *Transformer) extractHashtags(raw APITweetRaw) []string {
	if raw.Entities == nil {
		return nil
	}
	tags := make([]string, 0, len(raw.Entities.Hashtags))
	for _, h := range raw.Entities.Hashtags {
		tags = append(tags, strings.ToLower(h.Tag))
	}
	return tags
}

// classifyStyle assigne un label lisible au style du tweet :
// Retweet, Quote Tweet, Reply/Thread, Short Video, Long Video,
// GIF Post, Photo Post, Media Carousel, Thread/Long Text, Short Text.
func (t *Transformer) classifyStyle(raw APITweetRaw, contentType string) string {
	isReply := raw.ConversationID != "" && raw.ConversationID != raw.ID
	isQuote := false
	isRetweet := false

	for _, ref := range raw.ReferencedTweets {
		switch ref.Type {
		case "quoted":
			isQuote = true
		case "retweeted":
			isRetweet = true
		case "replied_to":
			isReply = true
		}
	}

	switch {
	case isRetweet:
		return "Retweet"
	case isQuote:
		return "Quote Tweet"
	case isReply:
		return "Reply / Thread"
	case contentType == "Video":
		if len(raw.Text) < 80 {
			return "Short Video"
		}
		return "Long Video"
	case contentType == "GIF":
		return "GIF Post"
	case contentType == "Image":
		return "Photo Post"
	case contentType == "Mixed":
		return "Media Carousel"
	default:
		// Texte pur
		if strings.Count(raw.Text, "\n") >= 3 || len(raw.Text) > 250 {
			return "Thread / Long Text"
		}
		return "Short Text"
	}
}
