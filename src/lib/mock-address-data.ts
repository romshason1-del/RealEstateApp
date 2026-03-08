import type { LatLng } from "./geo";

export type NearbyDeal = {
  id: string;
  name: string;
  type: string;
  distance: string;
  lat: number;
  lng: number;
};

const RESTAURANTS = [
  "Café Rothschild",
  "Hummus Abu Hassan",
  "Shakshukia",
  "Café Café",
  "Aroma",
  "Café Landwer",
  "Café Greg",
  "Café Hillel",
  "Café Joe",
  "Café Tamar",
];

function randomOffset(): number {
  return (Math.random() - 0.5) * 0.01;
}

export function getNearbyData(center: LatLng): { deals: NearbyDeal[] } {
  const deals: NearbyDeal[] = RESTAURANTS.slice(0, 6).map((name, i) => ({
    id: `deal-${i}`,
    name,
    type: "Restaurant",
    distance: `${(100 + i * 50).toFixed(0)}m`,
    lat: center.lat + randomOffset(),
    lng: center.lng + randomOffset(),
  }));
  return { deals };
}
