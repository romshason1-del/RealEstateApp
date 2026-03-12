/**
 * Italy OMI (Osservatorio del Mercato Immobiliare) Provider
 * Uses Agenzia delle Entrate OMI data for area price ranges and microzone valuations.
 * Data accessed via third-party API (3eurotools) that aggregates official OMI data.
 * OMI data: https://www.agenziaentrate.gov.it/portale/schede/fabbricatiterreni/omi/banche-dati/quotazioni-immobiliari
 */

const OMI_API_BASE = "https://3eurotools.it/api-quotazioni-immobiliari-omi/ricerca";

/** Italian comune name (normalized) -> codice catastale for OMI lookup */
const COMUNE_TO_CODICE: Record<string, string> = {
  roma: "H501",
  milano: "F205",
  napoli: "F839",
  torino: "L219",
  palermo: "G273",
  genova: "D969",
  bologna: "A944",
  firenze: "D612",
  venezia: "L736",
  verona: "L781",
  messina: "F158",
  padova: "G224",
  trieste: "L424",
  brescia: "B157",
  parma: "G337",
  modena: "F257",
  reggio: "H224",
  "reggio calabria": "H224",
  "reggio nell'emilia": "H223",
  prato: "G999",
  livorno: "E625",
  cagliari: "B354",
  foggia: "D643",
  ravenna: "H199",
  ferrara: "D548",
  rimini: "H294",
  syracuse: "I754",
  siracusa: "I754",
  sassari: "I452",
  latina: "E472",
  bergamo: "A794",
  vicenza: "L840",
  bolzano: "A952",
  "bolzano-bozen": "A952",
  trento: "L378",
  perugia: "G478",
  ancona: "A271",
  udine: "L483",
  arezzo: "A390",
  cesena: "C573",
  lecce: "E506",
  barletta: "A669",
  pescara: "G482",
  savona: "I480",
  "la spezia": "E463",
  gorizia: "E098",
  mantova: "E897",
  cremona: "D150",
  como: "C933",
  treviso: "L407",
  busto: "B295",
  "busto arsizio": "B295",
  varese: "L682",
  piacenza: "G535",
  novara: "F952",
  monza: "F704",
  "monza e brianza": "F704",
  catania: "C351",
  bari: "A662",
  caserta: "B963",
  salerno: "H703",
  taranto: "L049",
};

const DEFAULT_SQM = 100;

export type OMIResult = {
  estimated_value: number;
  area_price_min: number;
  area_price_max: number;
  price_source: "OMI";
};

function normalizeComune(name: string): string {
  return (name ?? "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function getCodiceCatastale(city: string): string | null {
  const n = normalizeComune(city);
  if (!n || n.length < 2) return null;
  if (COMUNE_TO_CODICE[n]) return COMUNE_TO_CODICE[n];
  const keys = Object.keys(COMUNE_TO_CODICE);
  const match = keys.find((k) => n.includes(k) || k.includes(n));
  return match ? COMUNE_TO_CODICE[match] : null;
}

type OMIZoneResponse = {
  abitazioni_civili?: {
    prezzo_acquisto_min?: number;
    prezzo_acquisto_max?: number;
    prezzo_acquisto_medio?: number;
  };
};

/**
 * Fetch OMI area price for a comune.
 * Maps city name to codice catastale and retrieves price band for residential (abitazioni_civili).
 * When zona_omi is omitted, API returns all zones; we use the average of zone midpoints.
 */
export async function fetchOMIByComune(
  city: string,
  sqm: number = DEFAULT_SQM
): Promise<OMIResult | null> {
  const codice = getCodiceCatastale(city);
  if (!codice) return null;

  const params = new URLSearchParams({
    codice_comune: codice,
    tipo_immobile: "abitazioni_civili",
    metri_quadri: String(Math.round(sqm)),
    operazione: "acquisto",
  });

  try {
    const res = await fetch(`${OMI_API_BASE}?${params}`, {
      signal: AbortSignal.timeout(5000),
      headers: { Accept: "application/json" },
    });
    if (!res.ok) return null;

    const data = (await res.json()) as Record<string, OMIZoneResponse>;
    const zones = Object.values(data).filter(
      (z): z is OMIZoneResponse => z != null && typeof z === "object"
    );

    if (zones.length === 0) return null;

    const midpoints: number[] = [];
    let globalMin = Infinity;
    let globalMax = -Infinity;

    for (const zone of zones) {
      const abit = zone.abitazioni_civili;
      if (!abit) continue;
      const min = abit.prezzo_acquisto_min ?? 0;
      const max = abit.prezzo_acquisto_max ?? 0;
      const medio = abit.prezzo_acquisto_medio ?? (min + max) / 2;
      if (medio > 0) midpoints.push(medio);
      if (min > 0 && min < globalMin) globalMin = min;
      if (max > 0 && max > globalMax) globalMax = max;
    }

    if (midpoints.length === 0) return null;

    const estimated_value = Math.round(
      midpoints.reduce((a, b) => a + b, 0) / midpoints.length
    );
    const area_price_min = globalMin === Infinity ? estimated_value * 0.8 : Math.round(globalMin);
    const area_price_max = globalMax === -Infinity ? estimated_value * 1.2 : Math.round(globalMax);

    return {
      estimated_value,
      area_price_min,
      area_price_max,
      price_source: "OMI",
    };
  } catch {
    return null;
  }
}
