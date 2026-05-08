package extractor

// ============================================================
// Package extractor — geo.go
// ============================================================
// Responsabilités :
//   - Table de correspondance exhaustive ISO-3166-1 alpha-2
//     (≈ 180 pays → NomNormalisé + Continent)
//   - GeoMapper : résolution country_code → GeoInfo
//   - LookupByLang : heuristique de secours via la langue du tweet
// ============================================================

import "strings"

// GeoInfo contient les informations géographiques résolues d'un tweet.
type GeoInfo struct {
	CountryCode string `json:"country_code"` // ISO 3166-1 alpha-2  (ex: "CI")
	CountryName string `json:"country_name"`  // Nom normalisé       (ex: "Cote_d_Ivoire")
	Continent   string `json:"continent"`     // Nom du continent    (ex: "Afrique")
}

// GeoMapper résout (country_code ISO-2) → (nom_normalisé, continent).
type GeoMapper struct{}

// NewGeoMapper retourne un GeoMapper initialisé.
func NewGeoMapper() *GeoMapper { return &GeoMapper{} }

// CountryTable est la table exhaustive ISO-3166-1 alpha-2.
// Format : code → [NomFichier, Continent]
// Exportée pour permettre des extensions extérieures au package.
var CountryTable = map[string][2]string{
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
	"KN": {"Saint_Christophe_et_Nieves", "Ameriques"},
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

// Lookup retourne GeoInfo à partir d'un country_code ISO-2.
// Si le code est inconnu, retourne continent "Unknown".
func (g *GeoMapper) Lookup(countryCode string) GeoInfo {
	code := strings.ToUpper(strings.TrimSpace(countryCode))
	if entry, ok := CountryTable[code]; ok {
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

// LookupByLang est une heuristique de secours quand il n'y a pas de géo
// dans le tweet : infère le pays le plus probable depuis la langue déclarée.
func (g *GeoMapper) LookupByLang(lang string) GeoInfo {
	langToCountry := map[string]string{
		"fr": "CI", // défaut francophone → Côte d'Ivoire (priorité éditoriale)
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
		"am": "ET",
		"sw": "KE",
		"ha": "NG",
		"yo": "NG",
	}
	if code, ok := langToCountry[strings.ToLower(lang)]; ok {
		return g.Lookup(code)
	}
	return GeoInfo{CountryCode: "", CountryName: "Unknown", Continent: "Unknown"}
}
