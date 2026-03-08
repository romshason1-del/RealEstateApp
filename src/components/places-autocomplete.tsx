"use client";

import * as React from "react";
import { Search } from "lucide-react";
import { Input } from "@/components/ui/input";
import { cn } from "@/lib/utils";

type PlacesAutocompleteProps = {
  value: string;
  onChange: (value: string) => void;
  onSelect?: (place: { lat: number; lng: number; label: string }) => void;
  placeholder?: string;
  className?: string;
};

export function PlacesAutocomplete({
  value,
  onChange,
  onSelect,
  placeholder = "Search address or location...",
  className,
}: PlacesAutocompleteProps) {
  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && value.trim()) {
      const trimmed = value.trim();
      const match = trimmed.match(/^(-?\d+\.?\d*)\s*,\s*(-?\d+\.?\d*)$/);
      if (match) {
        const lat = parseFloat(match[1]);
        const lng = parseFloat(match[2]);
        if (!isNaN(lat) && !isNaN(lng)) {
          onSelect?.({ lat, lng, label: trimmed });
          return;
        }
      }
      const presets: Record<string, { lat: number; lng: number }> = {
        paris: { lat: 48.8566, lng: 2.3522 },
        london: { lat: 51.5074, lng: -0.1278 },
        nyc: { lat: 40.7128, lng: -74.006 },
        rome: { lat: 41.9028, lng: 12.4964 },
      };
      const key = trimmed.toLowerCase();
      if (presets[key]) {
        const { lat, lng } = presets[key];
        onSelect?.({ lat, lng, label: trimmed });
      }
    }
  };

  return (
    <div className={cn("relative", className)}>
      <Search className="absolute left-3 top-1/2 size-4 -translate-y-1/2 text-amber-600" />
      <Input
        value={value}
        onChange={(e) => onChange(e.target.value)}
        onKeyDown={handleKeyDown}
        placeholder={placeholder}
        className="h-11 rounded-lg border-amber-200 bg-white pl-10 pr-4 text-foreground shadow-sm placeholder:text-amber-700/60 focus-visible:ring-amber-400"
      />
    </div>
  );
}
