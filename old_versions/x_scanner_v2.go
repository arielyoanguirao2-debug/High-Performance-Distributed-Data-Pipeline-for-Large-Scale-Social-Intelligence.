package main

// ============================================================
// X (Twitter) Global Data Scraper v2 — Go Implementation
// ============================================================
// Architecture :
//   Module 1  — AuthClient         (Bearer token + rate-limit guard)
//   Module 2  — Scanner            (Fan-out goroutines, global queries)
//   Module 3  — Extractor          (Géo-routing Pays→Continent + métriques)
//   Module 4  — Organizer          (Filesystem tree + flush périodique)
// ============================================================

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ╔══════════════════════════════════════════════════════════════╗
// ║                  TYPES & STRUCTURES                         ║
// ╚══════════════════════════════════════════════════════════════╝

// Config — paramètres CLI + nouvelles options géo/flush
type Config struct {
	BearerToken  string
	Keywords     []string
	Hashtags     []string
	MaxResults   int
	Interval     int    // secondes entre cycles de polling
	OutputFormat string // "json" | "csv"
	RootDir      string // racine de l'arborescence (défaut: data_scraped)
	Workers      int
	FlushEvery   int // flush buffer vers disque tous les N tweets (0 = à la fin du cycle)
	FlushMinutes int // flush buffer vers disque toutes les N minutes
}

// GeoInfo — informations géographiques résolues d'un tweet
type GeoInfo struct {
	CountryCode string `json:"country_code"` // ISO 3166-1 alpha-2  (ex: "CI")
	CountryName string `json:"country_name"`  // Nom normalisé       (ex: "Cote_d_Ivoire")
	Continent   string `json:"continent"`     // Nom du continent    (ex: "Afrique")
}

// RawTweet — tweet enrichi prêt pour l'écriture sur disque
type RawTweet struct {
	ID             string    `json:"id"`
	Text           string    `json:"text"`
	AuthorID       string    `json:"author_id"`
	CreatedAt      time.Time `json:"created_at"`
	ConversationID string    `json:"conversation_id"`
	Lang           string    `json:"lang"`

	// Métriques publiques
	LikeCount     int64 `json:"like_count"`
	RetweetCount  int64 `json:"retweet_count"`
	ReplyCount    int64 `json:"reply_count"`
	QuoteCount    int64 `json:"quote_count"`
	ViewCount     int64 `json:"view_count"`
	BookmarkCount int64 `json:"bookmark_count"`

	// Dérivés
	ContentType    string   `json:"content_type"`
	PostStyle      string   `json:"post_style"`
	Hashtags       []string `json:"hashtags"`
	EngagementRate float64  `json:"engagement_rate"`

	// Géolocalisation
	Geo GeoInfo `json:"geo"`
}

// HashtagStat — occurrence + engagement d'un hashtag
type HashtagStat struct {
	Tag         string  `json:"tag"`
	Occurrences int     `json:"occurrences"`
	TotalLikes  int64   `json:"total_likes"`
	AvgLikes    float64 `json:"avg_likes"`
}

// StyleStat — performance par style de post
type StyleStat struct {
	Style      string  `json:"style"`
	Count      int     `json:"count"`
	TotalLikes int64   `json:"total_likes"`
	AvgLikes   float64 `json:"avg_likes"`
	TotalViews int64   `json:"total_views"`
	AvgViews   float64 `json:"avg_views"`
}

// CountrySummary — données consolidées d'un pays
type CountrySummary struct {
	GeneratedAt      time.Time     `json:"generated_at"`
	CountryCode      string        `json:"country_code"`
	CountryName      string        `json:"country_name"`
	Continent        string        `json:"continent"`
	TotalTweets      int           `json:"total_tweets"`
	TopHashtags      []HashtagStat `json:"top_hashtags"`
	StylePerformance []StyleStat   `json:"style_performance"`
}

// ContinentSummary — données consolidées d'un continent
type ContinentSummary struct {
	GeneratedAt      time.Time     `json:"generated_at"`
	Continent        string        `json:"continent"`
	TotalTweets      int           `json:"total_tweets"`
	Countries        []string      `json:"countries"`
	TopHashtags      []HashtagStat `json:"top_hashtags"`
	StylePerformance []StyleStat   `json:"style_performance"`
}

// ── Structures API brutes ──────────────────────────────────────

type APIPublicMetrics struct {
	RetweetCount    int64 `json:"retweet_count"`
	ReplyCount      int64 `json:"reply_count"`
	LikeCount       int64 `json:"like_count"`
	QuoteCount      int64 `json:"quote_count"`
	BookmarkCount   int64 `json:"bookmark_count"`
	ImpressionCount int64 `json:"impression_count"`
}

type APIAttachments struct {
	MediaKeys []string `json:"media_keys"`
}

type APIEntities struct {
	Hashtags []struct {
		Tag string `json:"tag"`
	} `json:"hashtags"`
}

// APIGeo — bloc géo retourné par l'API X (place_id + coordonnées)
type APIGeo struct {
	PlaceID     string `json:"place_id"`
	Coordinates *struct {
		Type        string    `json:"type"`
		Coordinates []float64 `json:"coordinates"`
	} `json:"coordinates"`
}

// APIPlace — expansion place retournée dans includes
type APIPlace struct {
	ID          string `json:"id"`
	FullName    string `json:"full_name"`
	CountryCode string `json:"country_code"`
	Country     string `json:"country"`
	PlaceType   string `json:"place_type"`
}

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

type APIMedia struct {
	MediaKey      string `json:"media_key"`
	Type          string `json:"type"`
	PublicMetrics struct {
		ViewCount int64 `json:"view_count"`
	} `json:"public_metrics"`
}

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

// ╔══════════════════════════════════════════════════════════════╗
// ║         TABLE DE CORRESPONDANCE PAYS → CONTINENT            ║
// ╚══════════════════════════════════════════════════════════════╝

// GeoMapper résout (country_code ISO-2) → (nom_normalisé, continent)
type GeoMapper struct{}

func NewGeoMapper() *GeoMapper { return &GeoMapper{} }

// countryTable — table exhaustive ISO-3166-1 alpha-2
// Format : code → [NomFichier, Continent]
var countryTable = map[string][2]string{
	// ── Afrique ────────────────────────────────────────────────
	"DZ": {"Algerie", "Afrique"},
	"AO": {"Angola", "Afrique"},
	"BJ": {"Benin", "Afrique"},
	"BW": {"Botswana", "Afrique"},
	"BF": {"Burkina_Faso", "Afrique"},
	"BI": {"Burundi", "Afrique"},
	"CV": {"Cap_Vert", "Afrique"},
	"CM": {"Cameroun", "Afrique"},
	"CF": {"Republique_Centrafricaine", "Afrique"},
	"TD": {"Tchad", "Afrique"},
	"KM": {"Comores", "Afrique"},
	"CG": {"Congo_Brazzaville", "Afrique"},
	"CD": {"Congo_RDC", "Afrique"},
	"CI": {"Cote_d_Ivoire", "Afrique"},
	"DJ": {"Djibouti", "Afrique"},
	"EG": {"Egypte", "Afrique"},
	"GQ": {"Guinee_Equatoriale", "Afrique"},
	"ER": {"Erythree", "Afrique"},
	"SZ": {"Eswatini", "Afrique"},
	"ET": {"Ethiopie", "Afrique"},
	"GA": {"Gabon", "Afrique"},
	"GM": {"Gambie", "Afrique"},
	"GH": {"Ghana", "Afrique"},
	"GN": {"Guinee", "Afrique"},
	"GW": {"Guinee_Bissau", "Afrique"},
	"KE": {"Kenya", "Afrique"},
	"LS": {"Lesotho", "Afrique"},
	"LR": {"Liberia", "Afrique"},
	"LY": {"Libye", "Afrique"},
	"MG": {"Madagascar", "Afrique"},
	"MW": {"Malawi", "Afrique"},
	"ML": {"Mali", "Afrique"},
	"MR": {"Mauritanie", "Afrique"},
	"MU": {"Maurice", "Afrique"},
	"MA": {"Maroc", "Afrique"},
	"MZ": {"Mozambique", "Afrique"},
	"NA": {"Namibie", "Afrique"},
	"NE": {"Niger", "Afrique"},
	"NG": {"Nigeria", "Afrique"},
	"RW": {"Rwanda", "Afrique"},
	"ST": {"Sao_Tome_et_Principe", "Afrique"},
	"SN": {"Senegal", "Afrique"},
	"SC": {"Seychelles", "Afrique"},
	"SL": {"Sierra_Leone", "Afrique"},
	"SO": {"Somalie", "Afrique"},
	"ZA": {"Afrique_du_Sud", "Afrique"},
	"SS": {"Soudan_du_Sud", "Afrique"},
	"SD": {"Soudan", "Afrique"},
	"TZ": {"Tanzanie", "Afrique"},
	"TG": {"Togo", "Afrique"},
	"TN": {"Tunisie", "Afrique"},
	"UG": {"Ouganda", "Afrique"},
	"ZM": {"Zambie", "Afrique"},
	"ZW": {"Zimbabwe", "Afrique"},
	// ── Europe ─────────────────────────────────────────────────
	"AL": {"Albanie", "Europe"},
	"AD": {"Andorre", "Europe"},
	"AT": {"Autriche", "Europe"},
	"BY": {"Bielorussie", "Europe"},
	"BE": {"Belgique", "Europe"},
	"BA": {"Bosnie_Herzegovine", "Europe"},
	"BG": {"Bulgarie", "Europe"},
	"HR": {"Croatie", "Europe"},
	"CY": {"Chypre", "Europe"},
	"CZ": {"Republique_Tcheque", "Europe"},
	"DK": {"Danemark", "Europe"},
	"EE": {"Estonie", "Europe"},
	"FI": {"Finlande", "Europe"},
	"FR": {"France", "Europe"},
	"DE": {"Allemagne", "Europe"},
	"GR": {"Grece", "Europe"},
	"HU": {"Hongrie", "Europe"},
	"IS": {"Islande", "Europe"},
	"IE": {"Irlande", "Europe"},
	"IT": {"Italie", "Europe"},
	"XK": {"Kosovo", "Europe"},
	"LV": {"Lettonie", "Europe"},
	"LI": {"Liechtenstein", "Europe"},
	"LT": {"Lituanie", "Europe"},
	"LU": {"Luxembourg", "Europe"},
	"MT": {"Malte", "Europe"},
	"MD": {"Moldavie", "Europe"},
	"MC": {"Monaco", "Europe"},
	"ME": {"Montenegro", "Europe"},
	"NL": {"Pays_Bas", "Europe"},
	"MK": {"Macedoine_du_Nord", "Europe"},
	"NO": {"Norvege", "Europe"},
	"PL": {"Pologne", "Europe"},
	"PT": {"Portugal", "Europe"},
	"RO": {"Roumanie", "Europe"},
	"RU": {"Russie", "Europe"},
	"SM": {"Saint_Marin", "Europe"},
	"RS": {"Serbie", "Europe"},
	"SK": {"Slovaquie", "Europe"},
	"SI": {"Slovenie", "Europe"},
	"ES": {"Espagne", "Europe"},
	"SE": {"Suede", "Europe"},
	"CH": {"Suisse", "Europe"},
	"UA": {"Ukraine", "Europe"},
	"GB": {"Royaume_Uni", "Europe"},
	"VA": {"Vatican", "Europe"},
	// ── Asie ───────────────────────────────────────────────────
	"AF": {"Afghanistan", "Asie"},
	"AM": {"Armenie", "Asie"},
	"AZ": {"Azerbaidjan", "Asie"},
	"BH": {"Bahrein", "Asie"},
	"BD": {"Bangladesh", "Asie"},
	"BT": {"Bhoutan", "Asie"},
	"BN": {"Brunei", "Asie"},
	"KH": {"Cambodge", "Asie"},
	"CN": {"Chine", "Asie"},
	"GE": {"Georgie", "Asie"},
	"IN": {"Inde", "Asie"},
	"ID": {"Indonesie", "Asie"},
	"IR": {"Iran", "Asie"},
	"IQ": {"Irak", "Asie"},
	"IL": {"Israel", "Asie"},
	"JP": {"Japon", "Asie"},
	"JO": {"Jordanie", "Asie"},
	"KZ": {"Kazakhstan", "Asie"},
	"KW": {"Koweit", "Asie"},
	"KG": {"Kirghizistan", "Asie"},
	"LA": {"Laos", "Asie"},
	"LB": {"Liban", "Asie"},
	"MY": {"Malaisie", "Asie"},
	"MV": {"Maldives", "Asie"},
	"MN": {"Mongolie", "Asie"},
	"MM": {"Myanmar", "Asie"},
	"NP": {"Nepal", "Asie"},
	"KP": {"Coree_du_Nord", "Asie"},
	"OM": {"Oman", "Asie"},
	"PK": {"Pakistan", "Asie"},
	"PS": {"Palestine", "Asie"},
	"PH": {"Philippines", "Asie"},
	"QA": {"Qatar", "Asie"},
	"SA": {"Arabie_Saoudite", "Asie"},
	"SG": {"Singapour", "Asie"},
	"KR": {"Coree_du_Sud", "Asie"},
	"LK": {"Sri_Lanka", "Asie"},
	"SY": {"Syrie", "Asie"},
	"TW": {"Taiwan", "Asie"},
	"TJ": {"Tadjikistan", "Asie"},
	"TH": {"Thailande", "Asie"},
	"TL": {"Timor_Oriental", "Asie"},
	"TR": {"Turquie", "Asie"},
	"TM": {"Turkmenistan", "Asie"},
	"AE": {"Emirats_Arabes_Unis", "Asie"},
	"UZ": {"Ouzbekistan", "Asie"},
	"VN": {"Vietnam", "Asie"},
	"YE": {"Yemen", "Asie"},
	// ── Amériques ──────────────────────────────────────────────
	"AG": {"Antigua_et_Barbuda", "Ameriques"},
	"AR": {"Argentine", "Ameriques"},
	"BS": {"Bahamas", "Ameriques"},
	"BB": {"Barbade", "Ameriques"},
	"BZ": {"Belize", "Ameriques"},
	"BO": {"Bolivie", "Ameriques"},
	"BR": {"Bresil", "Ameriques"},
	"CA": {"Canada", "Ameriques"},
	"CL": {"Chili", "Ameriques"},
	"CO": {"Colombie", "Ameriques"},
	"CR": {"Costa_Rica", "Ameriques"},
	"CU": {"Cuba", "Ameriques"},
	"DM": {"Dominique", "Ameriques"},
	"DO": {"Republique_Dominicaine", "Ameriques"},
	"EC": {"Equateur", "Ameriques"},
	"SV": {"Salvador", "Ameriques"},
	"GD": {"Grenade", "Ameriques"},
	"GT": {"Guatemala", "Ameriques"},
	"GY": {"Guyana", "Ameriques"},
	"HT": {"Haiti", "Ameriques"},
	"HN": {"Honduras", "Ameriques"},
	"JM": {"Jamaique", "Ameriques"},
	"MX": {"Mexique", "Ameriques"},
	"NI": {"Nicaragua", "Ameriques"},
	"PA": {"Panama", "Ameriques"},
	"PY": {"Paraguay", "Ameriques"},
	"PE": {"Perou", "Ameriques"},
	"KN": {"Saint_Christophe_et_Nievès", "Ameriques"},
	"LC": {"Sainte_Lucie", "Ameriques"},
	"VC": {"Saint_Vincent", "Ameriques"},
	"SR": {"Suriname", "Ameriques"},
	"TT": {"Trinite_et_Tobago", "Ameriques"},
	"US": {"Etats_Unis", "Ameriques"},
	"UY": {"Uruguay", "Ameriques"},
	"VE": {"Venezuela", "Ameriques"},
	// ── Océanie ────────────────────────────────────────────────
	"AU": {"Australie", "Oceanie"},
	"FJ": {"Fidji", "Oceanie"},
	"KI": {"Kiribati", "Oceanie"},
	"MH": {"Iles_Marshall", "Oceanie"},
	"FM": {"Micronesie", "Oceanie"},
	"NR": {"Nauru", "Oceanie"},
	"NZ": {"Nouvelle_Zelande", "Oceanie"},
	"PW": {"Palaos", "Oceanie"},
	"PG": {"Papouasie_Nouvelle_Guinee", "Oceanie"},
	"WS": {"Samoa", "Oceanie"},
	"SB": {"Iles_Salomon", "Oceanie"},
	"TO": {"Tonga", "Oceanie"},
	"TV": {"Tuvalu", "Oceanie"},
	"VU": {"Vanuatu", "Oceanie"},
}

// Lookup retourne GeoInfo à partir d'un country_code ISO-2
// Si le code est inconnu, on retourne continent "Unknown"
func (g *GeoMapper) Lookup(countryCode string) GeoInfo {
	code := strings.ToUpper(strings.TrimSpace(countryCode))
	if entry, ok := countryTable[code]; ok {
		return GeoInfo{
			CountryCode: code,
			CountryName: entry[0],
			Continent:   entry[1],
		}
	}
	// Fallback : on conserve le code brut
	name := "Unknown_" + code
	if code == "" {
		name = "Unknown"
	}
	return GeoInfo{
		CountryCode: code,
		CountryName: name,
		Continent:   "Unknown",
	}
}

// LookupByLang — heuristique de secours quand il n'y a pas de géo dans le tweet :
// on infère le pays le plus probable depuis la langue déclarée.
func (g *GeoMapper) LookupByLang(lang string) GeoInfo {
	langToCountry := map[string]string{
		"fr": "CI", // défaut francophone → Afrique (Côte d'Ivoire en priorité éditoriale)
		"en": "US",
		"es": "ES",
		"pt": "BR",
		"ar": "EG",
		"zh": "CN",
		"ja": "JP",
		"ko": "KR",
		"de": "DE",
		"it": "IT",
		"ru": "RU",
		"tr": "TR",
		"id": "ID",
		"hi": "IN",
		"ur": "PK",
		"vi": "VN",
		"th": "TH",
		"nl": "NL",
		"pl": "PL",
		"sv": "SE",
		"fi": "FI",
	}
	if code, ok := langToCountry[strings.ToLower(lang)]; ok {
		return g.Lookup(code)
	}
	return GeoInfo{CountryCode: "", CountryName: "Unknown", Continent: "Unknown"}
}

// ╔══════════════════════════════════════════════════════════════╗
// ║            MODULE 1 — AUTHENTIFICATION                      ║
// ╚══════════════════════════════════════════════════════════════╝

type AuthClient struct {
	httpClient     *http.Client
	bearerToken    string
	mu             sync.Mutex
	remainingCalls int
	resetAt        time.Time
}

func NewAuthClient(token string) *AuthClient {
	return &AuthClient{
		httpClient:     &http.Client{Timeout: 15 * time.Second},
		bearerToken:    token,
		remainingCalls: 450,
		resetAt:        time.Now().Add(15 * time.Minute),
	}
}

func (a *AuthClient) Do(req *http.Request) (*http.Response, error) {
	a.mu.Lock()
	if time.Now().After(a.resetAt) {
		a.remainingCalls = 450
		a.resetAt = time.Now().Add(15 * time.Minute)
	}
	if a.remainingCalls <= 5 {
		wait := time.Until(a.resetAt) + 2*time.Second
		log.Printf("[RateLimit] Budget épuisé — pause de %s\n", wait.Round(time.Second))
		a.mu.Unlock()
		time.Sleep(wait)
		a.mu.Lock()
		a.remainingCalls = 450
		a.resetAt = time.Now().Add(15 * time.Minute)
	}
	a.remainingCalls--
	a.mu.Unlock()

	req.Header.Set("Authorization", "Bearer "+a.bearerToken)
	req.Header.Set("User-Agent", "XGlobalScanner-Go/2.0")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http.Do: %w", err)
	}

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

	if resp.StatusCode == http.StatusTooManyRequests {
		resp.Body.Close()
		retry := 60 * time.Second
		if v := resp.Header.Get("retry-after"); v != "" {
			if s, err2 := strconv.Atoi(v); err2 == nil {
				retry = time.Duration(s) * time.Second
			}
		}
		log.Printf("[RateLimit] 429 — retry dans %s\n", retry)
		time.Sleep(retry)
		return a.Do(req)
	}
	return resp, nil
}

// ╔══════════════════════════════════════════════════════════════╗
// ║             MODULE 2 — SCANNER CONCURRENT                   ║
// ╚══════════════════════════════════════════════════════════════╝

type ScanJob struct {
	Query      string
	MaxResults int
}

type Scanner struct {
	auth    *AuthClient
	workers int
}

func NewScanner(auth *AuthClient, workers int) *Scanner {
	return &Scanner{auth: auth, workers: workers}
}

// buildSearchURL — construit l'URL de recherche SANS filtre géographique obligatoire
// On ajoute geo_fields et place.fields pour récupérer la localisation
func buildSearchURL(query string, maxResults int, sinceID string) string {
	base := "https://api.twitter.com/2/tweets/search/recent"
	p := url.Values{}
	p.Set("query", query)
	p.Set("max_results", strconv.Itoa(clamp(maxResults, 10, 100)))
	p.Set("tweet.fields",
		"created_at,author_id,conversation_id,lang,public_metrics,"+
			"attachments,entities,referenced_tweets,geo")
	p.Set("expansions", "attachments.media_keys,geo.place_id")
	p.Set("media.fields", "type,public_metrics")
	p.Set("place.fields", "full_name,country_code,country,place_type")
	if sinceID != "" {
		p.Set("since_id", sinceID)
	}
	return base + "?" + p.Encode()
}

func (s *Scanner) fetchPage(ctx context.Context, rawURL string) (*SearchResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, fmt.Errorf("newRequest: %w", err)
	}
	resp, err := s.auth.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d pour %s", resp.StatusCode, rawURL)
	}
	var sr SearchResponse
	if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
		return nil, fmt.Errorf("json.Decode: %w", err)
	}
	return &sr, nil
}

func (s *Scanner) ScanAll(
	ctx context.Context,
	jobs []ScanJob,
	sinceIDs map[string]string,
) <-chan *SearchResponse {

	out := make(chan *SearchResponse, len(jobs)*4)
	jobCh := make(chan ScanJob, len(jobs))
	for _, j := range jobs {
		jobCh <- j
	}
	close(jobCh)

	var wg sync.WaitGroup
	for i := 0; i < s.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobCh {
				select {
				case <-ctx.Done():
					return
				default:
				}
				pageURL := buildSearchURL(job.Query, job.MaxResults, sinceIDs[job.Query])
				for pageURL != "" {
					sr, err := s.fetchPage(ctx, pageURL)
					if err != nil {
						log.Printf("[Scanner] Erreur query=%q : %v\n", job.Query, err)
						break
					}
					out <- sr
					if sr.Meta.NextToken != "" {
						pageURL += "&next_token=" + sr.Meta.NextToken
					} else {
						pageURL = ""
					}
				}
			}
		}()
	}
	go func() { wg.Wait(); close(out) }()
	return out
}

// ╔══════════════════════════════════════════════════════════════╗
// ║      MODULE 3 — EXTRACTEUR + ROUTING GÉO                    ║
// ╚══════════════════════════════════════════════════════════════╝

type Extractor struct {
	geo *GeoMapper
}

func NewExtractor() *Extractor {
	return &Extractor{geo: NewGeoMapper()}
}

// Transform convertit une SearchResponse en slice de RawTweet enrichis + géo-routés
func (e *Extractor) Transform(sr *SearchResponse) []RawTweet {
	// Index media_key → APIMedia
	mediaMap := make(map[string]APIMedia, len(sr.Includes.Media))
	for _, m := range sr.Includes.Media {
		mediaMap[m.MediaKey] = m
	}
	// Index place_id → APIPlace
	placeMap := make(map[string]APIPlace, len(sr.Includes.Places))
	for _, pl := range sr.Includes.Places {
		placeMap[pl.ID] = pl
	}

	results := make([]RawTweet, 0, len(sr.Data))
	for _, raw := range sr.Data {
		t := RawTweet{
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
		if ts, err := time.Parse(time.RFC3339, raw.CreatedAt); err == nil {
			t.CreatedAt = ts
		}

		// ── Géolocalisation ──────────────────────────────────────
		t.Geo = e.resolveGeo(raw, placeMap)

		// ── Contenu & métriques ──────────────────────────────────
		ct, views := e.detectContentType(raw, mediaMap)
		t.ContentType = ct
		t.ViewCount = views
		t.Hashtags = e.extractHashtags(raw)
		t.PostStyle = e.classifyStyle(raw, ct)

		if t.ViewCount > 0 {
			interactions := float64(t.LikeCount + t.RetweetCount + t.ReplyCount + t.QuoteCount)
			t.EngagementRate = math.Round(interactions/float64(t.ViewCount)*100*100) / 100
		}

		results = append(results, t)
	}
	return results
}

// resolveGeo détermine le GeoInfo d'un tweet par ordre de priorité :
//  1. Expansion place (place_id dans includes)
//  2. Champ geo.place_id directement
//  3. Heuristique sur la langue
func (e *Extractor) resolveGeo(raw APITweetRaw, placeMap map[string]APIPlace) GeoInfo {
	if raw.Geo != nil && raw.Geo.PlaceID != "" {
		if pl, ok := placeMap[raw.Geo.PlaceID]; ok && pl.CountryCode != "" {
			return e.geo.Lookup(pl.CountryCode)
		}
	}
	// Fallback langue
	return e.geo.LookupByLang(raw.Lang)
}

func (e *Extractor) detectContentType(raw APITweetRaw, mediaMap map[string]APIMedia) (string, int64) {
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

func (e *Extractor) extractHashtags(raw APITweetRaw) []string {
	if raw.Entities == nil {
		return nil
	}
	tags := make([]string, 0, len(raw.Entities.Hashtags))
	for _, h := range raw.Entities.Hashtags {
		tags = append(tags, strings.ToLower(h.Tag))
	}
	return tags
}

func (e *Extractor) classifyStyle(raw APITweetRaw, ct string) string {
	isReply, isQuote, isRetweet := raw.ConversationID != "" && raw.ConversationID != raw.ID, false, false
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
	case ct == "Video":
		if len(raw.Text) < 80 {
			return "Short Video"
		}
		return "Long Video"
	case ct == "GIF":
		return "GIF Post"
	case ct == "Image":
		return "Photo Post"
	case ct == "Mixed":
		return "Media Carousel"
	default:
		if strings.Count(raw.Text, "\n") >= 3 || len(raw.Text) > 250 {
			return "Thread / Long Text"
		}
		return "Short Text"
	}
}

// ╔══════════════════════════════════════════════════════════════╗
// ║    MODULE 4 — ORGANIZER (Filesystem + Flush périodique)     ║
// ╚══════════════════════════════════════════════════════════════╝

// countryBuffer accumule les tweets d'un pays avant le flush sur disque
type countryBuffer struct {
	mu     sync.Mutex
	tweets []RawTweet
	seen   map[string]struct{} // déduplique par tweet ID
}

func newCountryBuffer() *countryBuffer {
	return &countryBuffer{seen: make(map[string]struct{})}
}

func (b *countryBuffer) add(t RawTweet) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.seen[t.ID]; ok {
		return false
	}
	b.seen[t.ID] = struct{}{}
	b.tweets = append(b.tweets, t)
	return true
}

func (b *countryBuffer) drain() []RawTweet {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := b.tweets
	b.tweets = nil
	return out
}

func (b *countryBuffer) size() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.tweets)
}

// Organizer gère l'arborescence data_scraped/ et les flush
type Organizer struct {
	cfg     *Config
	mu      sync.RWMutex
	buffers map[string]*countryBuffer // clé : "Continent/Pays"
}

func NewOrganizer(cfg *Config) *Organizer {
	return &Organizer{
		cfg:     cfg,
		buffers: make(map[string]*countryBuffer),
	}
}

// bufferKey retourne la clé canonique continent/pays
func bufferKey(geo GeoInfo) string {
	return geo.Continent + "/" + geo.CountryName
}

// getOrCreateBuffer retourne (ou crée) le buffer d'un pays
func (o *Organizer) getOrCreateBuffer(key string) *countryBuffer {
	o.mu.RLock()
	buf, ok := o.buffers[key]
	o.mu.RUnlock()
	if ok {
		return buf
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if buf, ok = o.buffers[key]; ok {
		return buf
	}
	buf = newCountryBuffer()
	o.buffers[key] = buf
	return buf
}

// Ingest enregistre un tweet dans le bon buffer pays
// Retourne true si le tweet était nouveau (non-dupliqué)
func (o *Organizer) Ingest(t RawTweet) bool {
	key := bufferKey(t.Geo)
	buf := o.getOrCreateBuffer(key)
	return buf.add(t)
}

// TotalBuffered retourne le nombre total de tweets en attente de flush
func (o *Organizer) TotalBuffered() int {
	o.mu.RLock()
	defer o.mu.RUnlock()
	total := 0
	for _, b := range o.buffers {
		total += b.size()
	}
	return total
}

// FlushAll vide TOUS les buffers sur disque
func (o *Organizer) FlushAll() {
	o.mu.RLock()
	keys := make([]string, 0, len(o.buffers))
	for k := range o.buffers {
		keys = append(keys, k)
	}
	o.mu.RUnlock()

	for _, key := range keys {
		o.mu.RLock()
		buf := o.buffers[key]
		o.mu.RUnlock()

		tweets := buf.drain()
		if len(tweets) == 0 {
			continue
		}
		if err := o.flushCountry(key, tweets); err != nil {
			log.Printf("[Organizer] Erreur flush %s : %v\n", key, err)
		}
	}
	// Regénérer les summaries par continent
	o.rebuildContinentSummaries()
}

// flushCountry écrit les tweets d'un pays dans son fichier CSV/JSON
// en mode APPEND (os.O_APPEND) pour préserver les données précédentes
func (o *Organizer) flushCountry(key string, tweets []RawTweet) error {
	if len(tweets) == 0 {
		return nil
	}
	// key = "Continent/Pays"
	parts := strings.SplitN(key, "/", 2)
	if len(parts) != 2 {
		return fmt.Errorf("clé de buffer invalide : %s", key)
	}
	continent, country := parts[0], parts[1]
	geo := tweets[0].Geo

	// ── Créer le répertoire si nécessaire ─────────────────────
	dir := filepath.Join(o.cfg.RootDir, continent, country)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("MkdirAll %s : %w", dir, err)
	}

	// ── Chemin du fichier de données brutes ───────────────────
	var dataPath string
	switch strings.ToLower(o.cfg.OutputFormat) {
	case "json":
		dataPath = filepath.Join(dir, "data_"+strings.ToLower(geo.CountryCode)+".json")
		if err := o.appendJSON(dataPath, tweets); err != nil {
			return err
		}
	default: // csv
		dataPath = filepath.Join(dir, "data_"+strings.ToLower(geo.CountryCode)+".csv")
		if err := o.appendCSV(dataPath, tweets); err != nil {
			return err
		}
	}

	// ── Fichier stats_[pays].json (recalculé à chaque flush) ──
	statsPath := filepath.Join(dir, "stats_"+strings.ToLower(geo.CountryCode)+".json")
	if err := o.writeCountryStats(statsPath, geo, tweets); err != nil {
		return err
	}

	log.Printf("[Organizer] Flush → %s (%d tweets) → %s\n", key, len(tweets), dataPath)
	return nil
}

// appendCSV ajoute des lignes à un fichier CSV existant (crée l'en-tête si nouveau)
func (o *Organizer) appendCSV(path string, tweets []RawTweet) error {
	isNew := false
	if _, err := os.Stat(path); os.IsNotExist(err) {
		isNew = true
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open CSV %s : %w", path, err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	if isNew {
		header := []string{
			"id", "created_at", "author_id", "lang",
			"country_code", "country_name", "continent",
			"content_type", "post_style",
			"like_count", "retweet_count", "reply_count", "quote_count",
			"view_count", "bookmark_count", "engagement_rate",
			"hashtags", "text",
		}
		if err := w.Write(header); err != nil {
			return err
		}
	}

	for _, t := range tweets {
		row := []string{
			t.ID,
			t.CreatedAt.Format(time.RFC3339),
			t.AuthorID,
			t.Lang,
			t.Geo.CountryCode,
			t.Geo.CountryName,
			t.Geo.Continent,
			t.ContentType,
			t.PostStyle,
			strconv.FormatInt(t.LikeCount, 10),
			strconv.FormatInt(t.RetweetCount, 10),
			strconv.FormatInt(t.ReplyCount, 10),
			strconv.FormatInt(t.QuoteCount, 10),
			strconv.FormatInt(t.ViewCount, 10),
			strconv.FormatInt(t.BookmarkCount, 10),
			strconv.FormatFloat(t.EngagementRate, 'f', 2, 64),
			strings.Join(t.Hashtags, "|"),
			strings.ReplaceAll(t.Text, "\n", " "),
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	return nil
}

// appendJSON ajoute des objets à un fichier JSON newline-delimited (NDJSON)
// Format : un objet JSON par ligne → compatible avec BigQuery, ClickHouse, etc.
func (o *Organizer) appendJSON(path string, tweets []RawTweet) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open JSON %s : %w", path, err)
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	for _, t := range tweets {
		if err := enc.Encode(t); err != nil {
			return fmt.Errorf("json.Encode tweet %s : %w", t.ID, err)
		}
	}
	return nil
}

// writeCountryStats recalcule et écrit le fichier stats_[pays].json
func (o *Organizer) writeCountryStats(path string, geo GeoInfo, tweets []RawTweet) error {
	cs := buildCountrySummary(geo, tweets)
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create stats %s : %w", path, err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(cs)
}

// rebuildContinentSummaries relit les stats JSON de chaque pays pour composer
// le summary_[continent].json à la racine de chaque continent
func (o *Organizer) rebuildContinentSummaries() {
	// Collecter les continents présents dans o.buffers (même vides désormais)
	continents := make(map[string]struct{})
	o.mu.RLock()
	for key := range o.buffers {
		parts := strings.SplitN(key, "/", 2)
		if len(parts) == 2 {
			continents[parts[0]] = struct{}{}
		}
	}
	o.mu.RUnlock()

	for continent := range continents {
		if err := o.writeContinentSummary(continent); err != nil {
			log.Printf("[Organizer] Erreur summary continent %s : %v\n", continent, err)
		}
	}
}

// writeContinentSummary scanne tous les sous-dossiers pays et agrège les stats
func (o *Organizer) writeContinentSummary(continent string) error {
	continentDir := filepath.Join(o.cfg.RootDir, continent)
	entries, err := os.ReadDir(continentDir)
	if err != nil {
		return fmt.Errorf("ReadDir %s : %w", continentDir, err)
	}

	var (
		totalTweets int
		countries   []string
		allHashtags = make(map[string]*HashtagStat)
		allStyles   = make(map[string]*StyleStat)
	)

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		countryDir := filepath.Join(continentDir, e.Name())
		// Chercher le fichier stats_*.json dans ce dossier
		statsFiles, _ := filepath.Glob(filepath.Join(countryDir, "stats_*.json"))
		for _, sf := range statsFiles {
			cs, err := readCountrySummary(sf)
			if err != nil {
				log.Printf("[Organizer] Impossible de lire %s : %v\n", sf, err)
				continue
			}
			totalTweets += cs.TotalTweets
			countries = append(countries, cs.CountryName)
			for _, hs := range cs.TopHashtags {
				if existing, ok := allHashtags[hs.Tag]; ok {
					existing.Occurrences += hs.Occurrences
					existing.TotalLikes += hs.TotalLikes
				} else {
					clone := hs
					allHashtags[hs.Tag] = &clone
				}
			}
			for _, ss := range cs.StylePerformance {
				if existing, ok := allStyles[ss.Style]; ok {
					existing.Count += ss.Count
					existing.TotalLikes += ss.TotalLikes
					existing.TotalViews += ss.TotalViews
				} else {
					clone := ss
					allStyles[ss.Style] = &clone
				}
			}
		}
	}

	// Calculer les moyennes finales
	topHashtags := sortedHashtags(allHashtags, 50)
	stylePerf := sortedStyles(allStyles)

	summary := ContinentSummary{
		GeneratedAt:      time.Now().UTC(),
		Continent:        continent,
		TotalTweets:      totalTweets,
		Countries:        countries,
		TopHashtags:      topHashtags,
		StylePerformance: stylePerf,
	}

	outPath := filepath.Join(continentDir, "summary_"+strings.ToLower(continent)+".json")
	f, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("create summary %s : %w", outPath, err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(summary); err != nil {
		return err
	}
	log.Printf("[Organizer] Summary continent → %s (%d tweets, %d pays)\n",
		outPath, totalTweets, len(countries))
	return nil
}

// RunFlushDaemon lance un goroutine qui flush toutes les flushMinutes minutes
// ET surveille le seuil flushEvery tweets
func (o *Organizer) RunFlushDaemon(ctx context.Context, flushMinutes, flushEvery int) {
	tickerTime := time.NewTicker(time.Duration(flushMinutes) * time.Minute)
	tickerCount := time.NewTicker(5 * time.Second) // vérifier le seuil toutes les 5s
	defer tickerTime.Stop()
	defer tickerCount.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[FlushDaemon] Flush final avant arrêt...")
			o.FlushAll()
			return
		case <-tickerTime.C:
			log.Printf("[FlushDaemon] Flush temporel (%dm)\n", flushMinutes)
			o.FlushAll()
		case <-tickerCount.C:
			if flushEvery > 0 && o.TotalBuffered() >= flushEvery {
				log.Printf("[FlushDaemon] Seuil %d tweets atteint — flush\n", flushEvery)
				o.FlushAll()
			}
		}
	}
}

// ╔══════════════════════════════════════════════════════════════╗
// ║                FONCTIONS UTILITAIRES                        ║
// ╚══════════════════════════════════════════════════════════════╝

func buildCountrySummary(geo GeoInfo, tweets []RawTweet) CountrySummary {
	allHashtags := make(map[string]*HashtagStat)
	allStyles := make(map[string]*StyleStat)

	for _, t := range tweets {
		for _, tag := range t.Hashtags {
			if hs, ok := allHashtags[tag]; ok {
				hs.Occurrences++
				hs.TotalLikes += t.LikeCount
			} else {
				allHashtags[tag] = &HashtagStat{Tag: tag, Occurrences: 1, TotalLikes: t.LikeCount}
			}
		}
		style := t.PostStyle
		if ss, ok := allStyles[style]; ok {
			ss.Count++
			ss.TotalLikes += t.LikeCount
			ss.TotalViews += t.ViewCount
		} else {
			allStyles[style] = &StyleStat{
				Style:      style,
				Count:      1,
				TotalLikes: t.LikeCount,
				TotalViews: t.ViewCount,
			}
		}
	}
	return CountrySummary{
		GeneratedAt:      time.Now().UTC(),
		CountryCode:      geo.CountryCode,
		CountryName:      geo.CountryName,
		Continent:        geo.Continent,
		TotalTweets:      len(tweets),
		TopHashtags:      sortedHashtags(allHashtags, 20),
		StylePerformance: sortedStyles(allStyles),
	}
}

func sortedHashtags(m map[string]*HashtagStat, limit int) []HashtagStat {
	out := make([]HashtagStat, 0, len(m))
	for _, hs := range m {
		if hs.Occurrences > 0 {
			hs.AvgLikes = math.Round(float64(hs.TotalLikes)/float64(hs.Occurrences)*100) / 100
		}
		out = append(out, *hs)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Occurrences != out[j].Occurrences {
			return out[i].Occurrences > out[j].Occurrences
		}
		return out[i].AvgLikes > out[j].AvgLikes
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

func sortedStyles(m map[string]*StyleStat) []StyleStat {
	out := make([]StyleStat, 0, len(m))
	for _, ss := range m {
		if ss.Count > 0 {
			ss.AvgLikes = math.Round(float64(ss.TotalLikes)/float64(ss.Count)*100) / 100
			ss.AvgViews = math.Round(float64(ss.TotalViews)/float64(ss.Count)*100) / 100
		}
		out = append(out, *ss)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].AvgLikes > out[j].AvgLikes
	})
	return out
}

func readCountrySummary(path string) (*CountrySummary, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var cs CountrySummary
	if err := json.NewDecoder(f).Decode(&cs); err != nil {
		return nil, err
	}
	return &cs, nil
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func splitTrim(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

// buildQueries — SANS filtre géographique, on scrape le flux global
func buildQueries(keywords, hashtags []string) []string {
	var queries []string

	// Requêtes par mots-clés (sans filtre pays)
	for _, kw := range keywords {
		queries = append(queries, kw+" -is:retweet")
	}

	// Requêtes par hashtags (batch de 5)
	batch := make([]string, 0, 5)
	for _, ht := range hashtags {
		tag := ht
		if !strings.HasPrefix(tag, "#") {
			tag = "#" + tag
		}
		batch = append(batch, tag)
		if len(batch) == 5 {
			queries = append(queries, "("+strings.Join(batch, " OR ")+") -is:retweet")
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		queries = append(queries, "("+strings.Join(batch, " OR ")+") -is:retweet")
	}
	return queries
}

// ╔══════════════════════════════════════════════════════════════╗
// ║                        MAIN                                 ║
// ╚══════════════════════════════════════════════════════════════╝

func main() {
	// ── Flags CLI ────────────────────────────────────────────────
	bearerToken  := flag.String("token",        "",              "Bearer Token X API v2 (ou env X_BEARER_TOKEN)")
	keywordsArg  := flag.String("keywords",     "Afrique,Africa,Côte d_Ivoire,Abidjan", "Mots-clés globaux, séparés par virgule")
	hashtagsArg  := flag.String("hashtags",     "CIV,Afrique,Africa,Politique,Tech,Sport", "Hashtags sans #, séparés par virgule")
	maxResults   := flag.Int("max",             100,             "Résultats par page (10–100)")
	interval     := flag.Int("interval",        300,             "Intervalle de polling en secondes (0 = run unique)")
	outputFmt    := flag.String("format",       "csv",           "Format de sortie : csv | json")
	rootDir      := flag.String("root",         "data_scraped",  "Répertoire racine de l'arborescence")
	workers      := flag.Int("workers",         6,               "Goroutines de scan concurrentes")
	flushEvery   := flag.Int("flush-every",     500,             "Flush automatique tous les N tweets bufferisés")
	flushMinutes := flag.Int("flush-minutes",   10,              "Flush automatique toutes les N minutes")
	flag.Parse()

	// Token d'authentification
	if *bearerToken == "" {
		*bearerToken = os.Getenv("X_BEARER_TOKEN")
	}
	if *bearerToken == "" {
		log.Fatal("[Auth] Aucun token fourni. Utilisez -token ou la variable X_BEARER_TOKEN.")
	}

	cfg := &Config{
		BearerToken:  *bearerToken,
		Keywords:     splitTrim(*keywordsArg),
		Hashtags:     splitTrim(*hashtagsArg),
		MaxResults:   *maxResults,
		Interval:     *interval,
		OutputFormat: *outputFmt,
		RootDir:      *rootDir,
		Workers:      *workers,
		FlushEvery:   *flushEvery,
		FlushMinutes: *flushMinutes,
	}

	// ── Instanciation des modules ────────────────────────────────
	auth      := NewAuthClient(cfg.BearerToken)
	scanner   := NewScanner(auth, cfg.Workers)
	extractor := NewExtractor()
	organizer := NewOrganizer(cfg)

	// Construire les requêtes de scan global
	queries := buildQueries(cfg.Keywords, cfg.Hashtags)
	log.Printf("[Config] %d requêtes | %d workers | interval=%ds | flush≥%d tweets ou ≥%dmin\n",
		len(queries), cfg.Workers, cfg.Interval, cfg.FlushEvery, cfg.FlushMinutes)
	log.Printf("[Config] Arborescence racine → %s/\n", cfg.RootDir)
	for i, q := range queries {
		log.Printf("  [Requête %02d] %s\n", i+1, q)
	}

	// Créer le répertoire racine
	if err := os.MkdirAll(cfg.RootDir, 0o755); err != nil {
		log.Fatalf("[Init] Impossible de créer %s : %v", cfg.RootDir, err)
	}

	// Gestion du signal d'arrêt
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("[Signal] Arrêt demandé — flush final en cours...")
		cancel()
	}()

	// Lancer le daemon de flush en arrière-plan
	go organizer.RunFlushDaemon(ctx, cfg.FlushMinutes, cfg.FlushEvery)

	// sinceIDs — mémorise le dernier tweet_id par requête (polling incrémental)
	sinceIDs := make(map[string]string)

	// ── Cycle de scan ────────────────────────────────────────────
	runCycle := func() {
		jobs := make([]ScanJob, len(queries))
		for i, q := range queries {
			jobs[i] = ScanJob{Query: q, MaxResults: cfg.MaxResults}
		}

		responseCh := scanner.ScanAll(ctx, jobs, sinceIDs)

		var totalNew, totalDup int
		for sr := range responseCh {
			// Mettre à jour le curseur incremental
			if sr.Meta.NewestID != "" {
				for _, j := range jobs {
					if cur, ok := sinceIDs[j.Query]; !ok || sr.Meta.NewestID > cur {
						sinceIDs[j.Query] = sr.Meta.NewestID
					}
				}
			}
			// Extraire + géo-router + ingérer
			tweets := extractor.Transform(sr)
			for _, t := range tweets {
				if organizer.Ingest(t) {
					totalNew++
				} else {
					totalDup++
				}
			}
		}
		log.Printf("[Cycle] +%d nouveaux tweets ingérés (%d doublons ignorés) | Buffer total : %d\n",
			totalNew, totalDup, organizer.TotalBuffered())
	}

	// Premier cycle immédiat
	runCycle()

	// Boucle de polling
	if cfg.Interval > 0 {
		ticker := time.NewTicker(time.Duration(cfg.Interval) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				// Le daemon de flush s'occupe du flush final
				// On attend un instant pour lui laisser le temps de terminer
				time.Sleep(3 * time.Second)
				log.Println("[Main] Arrêt propre.")
				return
			case <-ticker.C:
				log.Printf("[Poll] Nouveau cycle — %s\n", time.Now().UTC().Format(time.RFC3339))
				runCycle()
			}
		}
	} else {
		// Mode run unique : flush immédiat
		organizer.FlushAll()
		log.Println("[Main] Run unique terminé.")
	}
}
