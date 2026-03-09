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
