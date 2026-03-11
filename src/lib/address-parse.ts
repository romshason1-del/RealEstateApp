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

  const hnMatch = streetPart.match(/^(\d+[A-Za-z]?)\s+(.+)$/);
  const houseNumber = hnMatch ? hnMatch[1] : "";
  const street = hnMatch ? hnMatch[2].trim() : streetPart;

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
