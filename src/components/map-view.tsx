"use client";

import * as React from "react";
import dynamic from "next/dynamic";
import { useMap } from "react-leaflet";
import type { LatLng } from "@/lib/geo";

const MapContainer = dynamic(
  () => import("react-leaflet").then((m) => m.MapContainer),
  { ssr: false }
);
const TileLayer = dynamic(
  () => import("react-leaflet").then((m) => m.TileLayer),
  { ssr: false }
);
const Marker = dynamic(() => import("react-leaflet").then((m) => m.Marker), {
  ssr: false,
});
const Popup = dynamic(() => import("react-leaflet").then((m) => m.Popup), {
  ssr: false,
});

function MapCenterUpdater({ center }: { center: LatLng }) {
  const map = useMap();
  React.useEffect(() => {
    map.setView([center.lat, center.lng]);
  }, [map, center.lat, center.lng]);
  return null;
}

type MapViewProps = {
  center: LatLng;
  markers?: Array<{ lat: number; lng: number; name: string }>;
  className?: string;
};

export const MapView = React.forwardRef<HTMLDivElement, MapViewProps>(
  function MapView({ center, markers = [], className }, ref) {
    return (
      <div ref={ref} className={className} style={{ height: "100%", width: "100%" }}>
        <MapContainer
          center={[center.lat, center.lng]}
          zoom={16}
          style={{ height: "100%", width: "100%" }}
          scrollWheelZoom
        >
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />
          <MapCenterUpdater center={center} />
          {markers.map((m, i) => (
            <Marker key={i} position={[m.lat, m.lng]}>
              <Popup>{m.name}</Popup>
            </Marker>
          ))}
        </MapContainer>
      </div>
    );
  }
);
