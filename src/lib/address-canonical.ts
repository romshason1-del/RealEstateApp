/**
 * Official Israeli address canonicalization for exact building matching.
 * Converts Google-style English addresses and Hebrew official dataset values
 * to a comparable canonical form. Internal use only - no Hebrew in API output.
 */

// ---------------------------------------------------------------------------
// City canonicalization: English/Hebrew variants -> canonical match key
// ---------------------------------------------------------------------------

const CITY_ALIASES: Record<string, string> = {
  // Tel Aviv variants
  "tel aviv": "tel_aviv",
  "tel aviv-yafo": "tel_aviv",
  "tel aviv yafo": "tel_aviv",
  "tel-aviv": "tel_aviv",
  "tel-aviv-yafo": "tel_aviv",
  "תל אביב": "tel_aviv",
  "תל אביב - יפו": "tel_aviv",
  "תל אביב-יפו": "tel_aviv",
  "תל-אביב": "tel_aviv",
  "תל אביב יפו": "tel_aviv",
  // Jerusalem
  "jerusalem": "jerusalem",
  "ירושלים": "jerusalem",
  "yerushalayim": "jerusalem",
  // Haifa
  "haifa": "haifa",
  "חיפה": "haifa",
  "חיפה - כרמל": "haifa",
  // Rishon LeZion
  "rishon lezion": "rishon_lezion",
  "rishon leziyon": "rishon_lezion",
  "ראשון לציון": "rishon_lezion",
  // Petah Tikva
  "petah tikva": "petah_tikva",
  "petach tikva": "petah_tikva",
  "פתח תקווה": "petah_tikva",
  "פתח תקוה": "petah_tikva",
  // Netanya
  "netanya": "netanya",
  "נתניה": "netanya",
  // Holon
  "holon": "holon",
  "חולון": "holon",
  // Bnei Brak
  "bnei brak": "bnei_brak",
  "bnei braq": "bnei_brak",
  "בני ברק": "bnei_brak",
  // Ramat Gan
  "ramat gan": "ramat_gan",
  "רמת גן": "ramat_gan",
  // Ashdod
  "ashdod": "ashdod",
  "אשדוד": "ashdod",
  // Be'er Sheva
  "beer sheva": "beer_sheva",
  "be'er sheva": "beer_sheva",
  "beersheva": "beer_sheva",
  "באר שבע": "beer_sheva",
  "באר-שבע": "beer_sheva",
  // Herzliya
  "herzliya": "herzliya",
  "herzliyya": "herzliya",
  "הרצליה": "herzliya",
  // Kfar Saba
  "kfar saba": "kfar_saba",
  "kfar sava": "kfar_saba",
  "כפר סבא": "kfar_saba",
  // Ra'anana
  "raanana": "raanana",
  "ra'anana": "raanana",
  "רעננה": "raanana",
  // Modi'in
  "modiin": "modiin",
  "modi'in": "modiin",
  "מודיעין": "modiin",
  "מודיעין-מכבים-רעות": "modiin",
  // Bat Yam
  "bat yam": "bat_yam",
  "בת ים": "bat_yam",
  // Ramat HaSharon
  "ramat hasharon": "ramat_hasharon",
  "רמת השרון": "ramat_hasharon",
  // Givatayim
  "givatayim": "givatayim",
  "גבעתיים": "givatayim",
  // Hod HaSharon
  "hod hasharon": "hod_hasharon",
  "הוד השרון": "hod_hasharon",
  // Eilat
  "eilat": "eilat",
  "אילת": "eilat",
};

// Hebrew to ASCII transliteration (ISO 259 simplified)
const HEBREW_TO_ASCII: Record<string, string> = {
  א: "a", ב: "b", ג: "g", ד: "d", ה: "h", ו: "v", ז: "z", ח: "ch", ט: "t",
  י: "y", כ: "k", ך: "k", ל: "l", מ: "m", ם: "m", נ: "n", ן: "n", ס: "s",
  ע: "a", פ: "p", ף: "p", צ: "tz", ץ: "tz", ק: "k", ר: "r", ש: "sh", ת: "t",
};

// Common English transliteration variants for matching (kh/ch/h, chaim/chayim/hayim)
const TRANSLIT_VARIANTS: [RegExp, string][] = [
  [/\bkhayim\b/gi, "chaim"],
  [/\bhayim\b/gi, "chaim"],
  [/\bhaim\b/gi, "chaim"],
  [/\bchayim\b/gi, "chaim"],
  [/\bkhaim\b/gi, "chaim"],
  [/\bchaim\b/gi, "chaim"],
  [/\bkh\b/g, "ch"],
  [/\bch\b/g, "ch"],
  [/tz/g, "ts"],
  [/iy/g, "i"],
  [/yy/g, "y"],
  [/\s+/g, " "],
];

function normalizeKey(s: string): string {
  return s
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/[''-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/** Get canonical city key for matching. Returns same key for equivalent city names. */
export function toCanonicalCityKey(city: string): string {
  const key = normalizeKey(city);
  if (!key) return "";
  return CITY_ALIASES[key] ?? key.replace(/\s+/g, "_");
}

/** Transliterate Hebrew text to ASCII for comparison. */
export function hebrewToAscii(text: string): string {
  if (!text || typeof text !== "string") return "";
  let out = "";
  for (const char of text) {
    if (HEBREW_TO_ASCII[char] !== undefined) {
      out += HEBREW_TO_ASCII[char];
    } else if (/[\u0590-\u05FF]/.test(char)) {
      out += char; // Keep other Hebrew for fallback
    } else if (/[a-zA-Z0-9\s]/.test(char)) {
      out += char.toLowerCase();
    }
  }
  return out.replace(/\s+/g, " ").trim();
}

/** Check if string contains Hebrew characters. */
export function hasHebrew(text: string): boolean {
  return /[\u0590-\u05FF]/.test(text || "");
}

/** Normalize street name to canonical form for matching. Handles Hebrew and English. */
export function toCanonicalStreetKey(street: string): string {
  if (!street || typeof street !== "string") return "";
  let s = street
    .replace(/^\s*רחוב\s+/i, "")
    .replace(/\b(St|Street|Str|Ave|Avenue|Rd|Road)\b\.?/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!s) return "";

  if (hasHebrew(s)) {
    s = hebrewToAscii(s);
  } else {
    s = s.toLowerCase();
  }

  for (const [re, repl] of TRANSLIT_VARIANTS) {
    s = s.replace(re, repl);
  }
  return s.replace(/\s+/g, " ").trim().toLowerCase();
}

/** Normalize house number: 18, 18.0, 18A, 18 א -> canonical. Never match 10 with 100. */
export function toCanonicalHouseKey(house: string): string {
  if (!house || typeof house !== "string") return "";
  const s = String(house).trim();
  const numMatch = s.match(/^(\d+)/);
  const num = numMatch ? numMatch[1] : "";
  const suffixMatch = s.slice(num.length).match(/^[\s.]*([A-Za-zא-ת])?/);
  const suffix = suffixMatch?.[1]?.trim() ?? "";
  if (!num) return s;
  return suffix ? `${num}${suffix}` : num;
}

/** Canonical form for address matching. Both input and record use this. */
export type CanonicalAddress = {
  cityKey: string;
  streetKey: string;
  houseKey: string;
};

export function toCanonicalAddress(city: string, street: string, houseNumber: string): CanonicalAddress {
  return {
    cityKey: toCanonicalCityKey(city),
    streetKey: toCanonicalStreetKey(street),
    houseKey: toCanonicalHouseKey(houseNumber),
  };
}

/** Get Hebrew city name for API search (dataset often uses Hebrew). */
export function toHebrewCityForSearch(cityKey: string): string {
  const map: Record<string, string> = {
    tel_aviv: "תל אביב",
    jerusalem: "ירושלים",
    haifa: "חיפה",
    rishon_lezion: "ראשון לציון",
    petah_tikva: "פתח תקווה",
    netanya: "נתניה",
    holon: "חולון",
    bnei_brak: "בני ברק",
    ramat_gan: "רמת גן",
    ashdod: "אשדוד",
    beer_sheva: "באר שבע",
    herzliya: "הרצליה",
    kfar_saba: "כפר סבא",
    raanana: "רעננה",
    modiin: "מודיעין",
    bat_yam: "בת ים",
    ramat_hasharon: "רמת השרון",
    givatayim: "גבעתיים",
    hod_hasharon: "הוד השרון",
    eilat: "אילת",
  };
  return map[cityKey] ?? "";
}

/** Convert canonical city key to English display name for API output. */
export function cityKeyToEnglish(key: string): string {
  const map: Record<string, string> = {
    tel_aviv: "Tel Aviv-Yafo",
    jerusalem: "Jerusalem",
    haifa: "Haifa",
    rishon_lezion: "Rishon LeZion",
    petah_tikva: "Petah Tikva",
    netanya: "Netanya",
    holon: "Holon",
    bnei_brak: "Bnei Brak",
    ramat_gan: "Ramat Gan",
    ashdod: "Ashdod",
    beer_sheva: "Be'er Sheva",
    herzliya: "Herzliya",
    kfar_saba: "Kfar Saba",
    raanana: "Ra'anana",
    modiin: "Modi'in",
    bat_yam: "Bat Yam",
    ramat_hasharon: "Ramat HaSharon",
    givatayim: "Givatayim",
    hod_hasharon: "Hod HaSharon",
    eilat: "Eilat",
  };
  return map[key] ?? key.replace(/_/g, " ");
}
