/**
 * Parse full address string to structured fields.
 * Shared between client and server. No server-only dependencies.
 */

function normalizeStreetOrCity(val: string | undefined): string {
  if (val == null || typeof val !== "string") return "";
  return String(val)
    .replace(/^\s*רחוב\s+/i, "")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Parse full address string into city, street, house number.
 * Handles "דיזנגוף 10, תל אביב", "123 Dizengoff St, Tel Aviv", "רחוב דיזנגוף 10 תל אביב".
 */
export function parseAddressFromFullString(address: string): { city: string; street: string; houseNumber: string } {
  const trimmed = address
    .trim()
    .replace(/^\s*רחוב\s+/i, "")
    .replace(/\b(St|Street|Str|Ave|Avenue|Rd|Road)\b\.?/gi, " ")
    .replace(/\s+/g, " ")
    .trim();

  if (!trimmed) return { city: "", street: "", houseNumber: "" };

  const parts = trimmed.split(/[,،]/).map((p) => p.trim()).filter(Boolean);

  if (parts[parts.length - 1]?.match(/^(Israel|ישראל)$/i)) {
    parts.pop();
  }
  if (parts[parts.length - 1]?.match(/^(Italy|Italia|IT)$/i)) {
    parts.pop();
  }
  if (parts[parts.length - 1]?.match(/^(France|FR)$/i)) {
    parts.pop();
  }

  const houseMatch = trimmed.match(/\b(\d+)\s*([A-Za-zא-ת])?\b/);
  const houseNumber = houseMatch ? (houseMatch[2] ? `${houseMatch[1]}${houseMatch[2]}` : houseMatch[1]) : "";

  let city = "";
  let street = trimmed.replace(/\d+/g, " ").replace(/\s+/g, " ").trim();

  if (parts.length >= 2) {
    city = parts[parts.length - 1] ?? "";
    street = parts.slice(0, -1).join(" ").replace(/\d+/g, " ").replace(/\s+/g, " ").trim() || street;
  }

  return { city, street: normalizeStreetOrCity(street), houseNumber };
}

/**
 * US address format: "3 Ocean Dr, Miami Beach, FL 33139" or "3 Ocean Dr, Miami Beach, FL 33139, USA"
 * Extracts houseNumber, street, city, state, zip.
 * ZIP: 5 digits or 5+4. State: 2 letters before ZIP.
 * Does not treat ZIP as house number. Strips trailing USA/United States/US.
 */
export function parseUSAddressFromFullString(
  address: string
): { houseNumber: string; street: string; city: string; state: string; zip: string } {
  let trimmed = address.trim().replace(/\s+/g, " ");
  trimmed = trimmed.replace(/,?\s*(USA|United States|US)\s*$/i, "").trim();
  if (!trimmed) return { houseNumber: "", street: "", city: "", state: "", zip: "" };

  const parts = trimmed.split(",").map((p) => p.trim()).filter(Boolean);
  if (parts.length < 2) {
    const first = parts[0] ?? "";
    const hnMatch = first.match(/^(\d+[A-Za-z]?)\s+(.+)$/);
    return {
      houseNumber: hnMatch ? hnMatch[1] : "",
      street: hnMatch ? hnMatch[2].trim() : first,
      city: "",
      state: "",
      zip: "",
    };
  }

  const lastPart = parts[parts.length - 1] ?? "";
  const stateZipMatch = lastPart.match(/^([A-Za-z]{2})\s+(\d{5}(?:-\d{4})?)$/);
  let city = "";
  let state = "";
  let zip = "";

  if (stateZipMatch) {
    state = stateZipMatch[1].toUpperCase();
    zip = stateZipMatch[2];
    city = parts.length >= 2 ? (parts[parts.length - 2] ?? "").trim() : "";
  } else {
    city = lastPart;
  }

  const streetPart = parts[0] ?? "";
  const hnMatch = streetPart.match(/^(\d+[A-Za-z]?)\s+(.+)$/);
  const houseNumber = hnMatch ? hnMatch[1] : "";
  const street = hnMatch ? hnMatch[2].trim() : streetPart;

  return { houseNumber, street, city, state, zip };
}

/** Extract flat/unit/sub-building prefix from start of address (e.g. "Flat 414", "Unit 5", "#10") */
export function extractFlatPrefix(input: string): string | null {
  const trimmed = (input ?? "").trim();
  const m = trimmed.match(
    /^(flat\s+\d+[a-z]?|apartment\s+\d+[a-z]?|apt\.?\s*\d+[a-z]?|unit\s+\d+[a-z]?|suite\s+\d+[a-z]?|ste\.?\s*\d+[a-z]?|#\s*\d+[a-z]?)/i,
  );
  return m ? m[1].trim() : null;
}

/**
 * UK address format: "25 Crossharbour Plaza, London E14 9SH" or "10 Downing Street, London SW1A 2AA"
 * Extracts houseNumber, street, city, postcode. UK postcode: e.g. SW1A 2AA, PL6 8RU, M1 4BT.
 */
export function parseUKAddressFromFullString(
  address: string
): { houseNumber: string; street: string; city: string; postcode: string } {
  const trimmed = address.trim().replace(/\s+/g, " ");
  const withoutUK = trimmed.replace(/,?\s*(UK|United Kingdom|England|Scotland|Wales|Northern Ireland)\s*$/i, "").trim();
  if (!withoutUK) return { houseNumber: "", street: "", city: "", postcode: "" };

  const postcodeRe = /\b([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})\b/i;
  const postcodeMatch = withoutUK.match(postcodeRe);
  let postcode = postcodeMatch ? postcodeMatch[1].replace(/\s+/g, " ").trim().toUpperCase() : "";
  if (postcode && postcode.length >= 5) {
    const noSpace = postcode.replace(/\s/g, "");
    postcode = noSpace.slice(0, -3) + " " + noSpace.slice(-3);
  }

  const beforePostcode = postcodeMatch ? withoutUK.slice(0, postcodeMatch.index).trim() : withoutUK;
  const parts = beforePostcode.split(",").map((p) => p.trim()).filter(Boolean);

  let streetPart = "";
  let city = "";
  if (parts.length >= 2) {
    streetPart = parts[0] ?? "";
    city = parts[parts.length - 1] ?? "";
  } else if (parts.length === 1) {
    streetPart = parts[0] ?? "";
  }

  if (parts.length >= 2 && /^\d+[a-z]?$/.test(parts[0]) && /^\d+[a-z]?\s+.+$/.test(parts[1])) {
    const flatNum = parts[0];
    const rest = parts.slice(1).join(", ");
    const inner = parseUKAddressFromFullString(rest + (postcode ? ` ${postcode}` : ""));
    return { houseNumber: flatNum, street: [inner.houseNumber, inner.street].filter(Boolean).join(" ").trim() || inner.street, city: inner.city, postcode: inner.postcode || postcode };
  }

  const flatPrefixMatch = streetPart.match(
    /^(flat\s+\d+[a-z]?|apartment\s+\d+[a-z]?|apt\.?\s*\d+[a-z]?|unit\s+\d+[a-z]?|suite\s+\d+[a-z]?|ste\.?\s*\d+[a-z]?|#\s*\d+[a-z]?)/i,
  );
  if (flatPrefixMatch && parts.length >= 2) {
    const houseNumber = flatPrefixMatch[1].trim();
    const restOfFirst = streetPart.slice(flatPrefixMatch[0].length).trim();
    const restParts = parts.slice(1, -1).join(", ");
    const street = [restOfFirst, restParts].filter(Boolean).join(", ").trim();
    return { houseNumber, street, city, postcode };
  }

  const hnMatch = streetPart.match(/^(\d+[A-Za-z]?)\s+(.+)$/);
  const houseNumber = hnMatch ? hnMatch[1] : "";
  const street = hnMatch ? hnMatch[2].trim() : streetPart;

  return { houseNumber, street, city, postcode };
}

/**
 * Extract exact 5-digit French postcode token from raw string. Never mutates.
 * French postcodes are exactly 5 digits (e.g. 06400, 75004).
 * Returns the first 5-digit match; prefers postcode before city (e.g. ", 06400 Cannes").
 */
function extractRawFRPostcode(raw: string): string {
  const m = raw.match(/\b(\d{5})\b/);
  return m ? m[1] : "";
}

/**
 * Robust French address parser for raw typed strings.
 * Handles: "6 Chemin du Vallon, 06400 Cannes", "10 Rue de Rivoli, 75004 Paris",
 * "22 Rue Paradis, 13001 Marseille", uppercase-only, missing commas, extra spaces.
 * RAW POSTCODE PRIORITY: When a 5-digit postcode exists in the raw input, it is preserved exactly.
 */
export function parseFRAddressFromFullString(
  address: string
): { houseNumber: string; street: string; city: string; postcode: string } {
  const raw = (address ?? "").trim();
  const trimmed = raw.replace(/\s+/g, " ").trim();
  if (!trimmed) return { houseNumber: "", street: "", city: "", postcode: "" };

  const rawPostcode = extractRawFRPostcode(trimmed);

  // Strip common trailing suffixes
  const withoutFR = trimmed
    .replace(/,?\s*(France|FR)\s*$/i, "")
    .replace(/,?\s*(La\s*)?R\u00e9union\s*$/i, "")
    .replace(/,?\s*Reunion\s*$/i, "")
    .replace(/,?\s*Guadeloupe\s*$/i, "")
    .replace(/,?\s*Martinique\s*$/i, "")
    .replace(/,?\s*Guyane\s*$/i, "")
    .replace(/,?\s*Mayotte\s*$/i, "")
    .replace(/,?\s*Nouvelle-?Cal\u00e9donie\s*$/i, "")
    .replace(/,?\s*Polyn\u00e9sie\s*fran\u00e7aise\s*$/i, "")
    .trim();
  if (!withoutFR) return { houseNumber: "", street: "", city: "", postcode: "" };

  let postcode = "";
  let city = "";
  let beforePostcode = withoutFR;

  // Strategy 1: Postcode + city at end (with optional comma before)
  // "6 Chemin du Vallon, 06400 Cannes" or "6 Chemin du Vallon 06400 Cannes"
  const postcodeCityEnd = withoutFR.match(/,?\s*(\d{4,5})\s+([A-Za-zÀ-ÿ\s\'-]+)\s*$/);
  if (postcodeCityEnd) {
    postcode = postcodeCityEnd[1];
    city = postcodeCityEnd[2].trim();
    beforePostcode = withoutFR.slice(0, postcodeCityEnd.index ?? 0).trim().replace(/,\s*$/, "");
  }

  // Strategy 2: Comma-separated "..., postcode city" or "..., city postcode"
  if (!postcode && !city) {
    const parts = withoutFR.split(",").map((p) => p.trim()).filter(Boolean);
    const lastPart = parts[parts.length - 1] ?? "";
    const pcCityMatch = lastPart.match(/^(\d{4,5})\s+(.+)$/);
    if (pcCityMatch) {
      postcode = pcCityMatch[1];
      city = pcCityMatch[2].trim();
      beforePostcode = parts.slice(0, -1).join(" ").trim();
    } else {
      const cityPcMatch = lastPart.match(/^(.+?)\s+(\d{4,5})$/);
      if (cityPcMatch) {
        city = cityPcMatch[1].trim();
        postcode = cityPcMatch[2];
        beforePostcode = parts.slice(0, -1).join(" ").trim();
      }
    }
  }

  // Strategy 3: Postcode anywhere (5 digits or 4 digits)
  if (!postcode) {
    const fiveDigit = withoutFR.match(/\b(\d{5})\b/);
    const fourDigit = withoutFR.match(/\b([1-9]\d{3})\b/);
    if (fiveDigit) {
      postcode = fiveDigit[1];
    } else if (fourDigit) {
      postcode = "0" + fourDigit[1];
    }
  }

  // If we have postcode but no city, extract city after postcode
  if (postcode && !city) {
    const re = new RegExp(`\\b${postcode.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\s+([A-Za-zÀ-ÿ\\s\\'-]+)`, "i");
    const m = withoutFR.match(re);
    if (m?.[1]) city = m[1].trim();
  }

  // Extract house number and street from beforePostcode
  // "6 Chemin du Vallon" -> house 6, street "Chemin du Vallon"
  let houseNumber = "";
  let street = "";
  const firstPart = beforePostcode.split(",")[0]?.trim() ?? beforePostcode;
  const houseMatch = firstPart.match(/^(\d+[A-Za-z]?)\s+(.+)$/);
  if (houseMatch) {
    houseNumber = houseMatch[1];
    street = houseMatch[2].trim();
  } else {
    const leadingNum = firstPart.match(/^(\d+[A-Za-z]?)\s*$/);
    if (leadingNum) {
      houseNumber = leadingNum[1];
    }
    street = firstPart.replace(/^\d+[A-Za-z]?\s*/, "").trim() || firstPart;
  }

  if (!street && beforePostcode) {
    street = beforePostcode.replace(/^\d+[A-Za-z]?\s*/, "").trim();
  }

  if (rawPostcode) postcode = rawPostcode;
  return { houseNumber, street, city, postcode };
}

/** Remove unit/apartment suffixes from address for better property matching */
export function stripUSAddressUnitSuffixes(address: string): string {
  return address
    .trim()
    .replace(/\s*#\s*\d+[A-Za-z]?\s*$/i, "")
    .replace(/\s*,\s*unit\s+\d+[A-Za-z]?\s*$/i, "")
    .replace(/\s*,\s*apt\.?\s*\d+[A-Za-z]?\s*$/i, "")
    .replace(/\s*,\s*ste\.?\s*\d+[A-Za-z]?\s*$/i, "")
    .replace(/\s*,\s*suite\s+\d+[A-Za-z]?\s*$/i, "")
    .replace(/\s*,\s*#\s*\d+[A-Za-z]?\s*$/i, "")
    .replace(/\s*#\s*\d+[A-Za-z]?\s*,/i, ",")
    .replace(/\s+/g, " ")
    .trim();
}

/** Build address variants for RentCast retry. Returns unique non-empty strings. */
export function buildUSAddressVariants(input: {
  houseNumber: string;
  street: string;
  city: string;
  state: string;
  zip: string;
  fullAddress?: string;
}): string[] {
  const hn = (input.houseNumber ?? "").trim();
  const st = (input.street ?? "").trim();
  const city = (input.city ?? "").trim();
  const state = (input.state ?? "").trim().toUpperCase();
  const zip = (input.zip ?? "").trim();
  const full = (input.fullAddress ?? "").trim();

  const variants: string[] = [];
  const add = (s: string) => {
    const cleaned = stripUSAddressUnitSuffixes(s);
    if (cleaned && !variants.includes(cleaned)) variants.push(cleaned);
  };

  if (full) add(full);
  if (st && city) {
    const streetPart = [hn, st].filter(Boolean).join(" ");
    if (state && zip) add(`${streetPart}, ${city}, ${state} ${zip}`);
    else if (zip) add(`${streetPart}, ${city}, ${state || "CA"} ${zip}`);
    add(`${streetPart}, ${city}`);
  }
  if (st && zip && !variants.some((v) => v.includes(zip))) {
    const streetPart = [hn, st].filter(Boolean).join(" ");
    add(`${streetPart}, ${city || "Unknown"}, ${state || "CA"} ${zip}`);
  }

  return variants;
}
