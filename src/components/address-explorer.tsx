"use client";

import * as React from "react";
import {
  BadgeDollarSign,
  Bookmark,
  Building,
  Briefcase,
  Building2,
  Eye,
  EyeOff,
  Heart,
  House,
  Mail,
  Map,
  MapPin,
  Plus,
  Search,
  Star,
  Trash2,
  User,
  X,
} from "lucide-react";
import {
  GoogleMap,
  useJsApiLoader,
  type Libraries,
} from "@react-google-maps/api";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { getMockPropertyInsight } from "@/lib/mock-data";
import { createClient } from "@/lib/supabase/client";

type LatLng = {
  lat: number;
  lng: number;
};

type Restaurant = {
  id: string;
  name: string;
  address: string;
  rating?: number;
  reviews: number;
  location: LatLng;
};

type SavedTab = "restaurants" | "properties";
type AssetOwnership = "owned" | "wishlist";
type AssetPropertyType = "house" | "apartment";

type SavedRestaurant = Restaurant;

type PortfolioAsset = {
  id: string;
  address: string;
  position: LatLng;
  ownership: AssetOwnership;
  propertyType: AssetPropertyType;
  estimatedPropertyValue: string;
  estimatedPropertyValueNumber?: number;
  currencySymbol?: string;
  countryCode?: string;
};

type AutocompletePredictionItem = {
  description: string;
  placeId: string;
};

type GeocodeAddressComponent = {
  long_name: string;
  short_name: string;
  types: string[];
};

type BuildingInsight = {
  position: LatLng;
  address: string;
  estimatedPropertyValue: string;
  estimatedPropertyValueNumber: number;
  streetAverageValue: string;
  streetAverageValueNumber: number;
  currencySymbol: string;
  countryCode: string;
  providerLabel: string;
  requiresHouseNumber: boolean;
  valuationMessage: string | null;
  lastTransactions: string[];
};

type GeocodeResult = {
  formatted_address?: string;
  address_components?: GeocodeAddressComponent[];
  geometry: {
    location: {
      lat: () => number;
      lng: () => number;
    };
  };
};

type PlacesSearchResult = {
  geometry?: {
    location?: {
      lat: () => number;
      lng: () => number;
    };
  };
  name?: string;
  place_id?: string;
  rating?: number;
  user_ratings_total?: number;
  vicinity?: string;
};

declare global {
  interface Window {
    google?: typeof google;
    gm_authFailure?: () => void;
  }
}

// Loader: geometry only. NEVER include "places" – load via importLibrary("places") for Place.searchNearby
// mapIds: DEMO_MAP_ID required for AdvancedMarkerElement (Maps JavaScript API + Places API New)
const libraries: Libraries = ["geometry"];
const MAP_ID = "DEMO_MAP_ID";
const mapContainerStyle: React.CSSProperties = {
  width: "100%",
  height: "min(55vh, 50dvh)",
  minHeight: "280px",
  backgroundColor: "#1b1f24",
};

const navItems = [
  { label: "Explore", icon: Map },
  { label: "Saved", icon: Heart },
  { label: "Portfolio", icon: Briefcase },
  { label: "Profile", icon: User },
] as const;

const WORLD_VIEW_CENTER: LatLng = { lat: 20, lng: 0 };
const WORLD_VIEW_ZOOM = 2;
const defaultCenter: LatLng = { lat: 32.0853, lng: 34.7818 };

const STORAGE_KEYS = {
  savedRestaurants: "streetiq.saved-restaurants",
  savedProperties: "streetiq.saved-properties",
  portfolioAssets: "streetiq.portfolio-assets",
  accountProfile: "streetiq.account-profile",
} as const;

function readStoredState<T>(key: string, fallback: T): T {
  if (typeof window === "undefined") {
    return fallback;
  }

  const raw = window.localStorage.getItem(key);
  if (!raw) {
    return fallback;
  }

  try {
    return JSON.parse(raw) as T;
  } catch {
    return fallback;
  }
}

function parseCurrencyAmount(value: string): number {
  const numeric = Number(value.replace(/[^0-9.-]/g, ""));
  return Number.isFinite(numeric) ? numeric : 0;
}

type CountryProfile = {
  countryCode: string;
  currencySymbol: string;
  providerLabel: string;
};

function resolveCountryProfile(address: string): CountryProfile {
  const normalizedAddress = address.toLowerCase();

  if (
    /israel|tel aviv|jerusalem|haifa|eilat|rishon lezion/.test(
      normalizedAddress,
    )
  ) {
    return {
      countryCode: "IL",
      currencySymbol: "₪",
      providerLabel: "data.gov.il",
    };
  }

  if (
    /italy|rome|milan|naples|florence|venice|turin/.test(normalizedAddress)
  ) {
    return {
      countryCode: "IT",
      currencySymbol: "€",
      providerLabel: "RentCast Global Mock",
    };
  }

  if (
    /france|paris|lyon|marseille|nice|bordeaux/.test(normalizedAddress)
  ) {
    return {
      countryCode: "FR",
      currencySymbol: "€",
      providerLabel: "RentCast Global Mock",
    };
  }

  if (
    /spain|madrid|barcelona|valencia|seville|malaga/.test(normalizedAddress)
  ) {
    return {
      countryCode: "ES",
      currencySymbol: "€",
      providerLabel: "RentCast Global Mock",
    };
  }

  if (
    /united states|usa|miami|florida|new york|california|los angeles|chicago|texas/.test(
      normalizedAddress,
    )
  ) {
    return {
      countryCode: "US",
      currencySymbol: "$",
      providerLabel: "RentCast Global Mock",
    };
  }

  return {
    countryCode: "INTL",
    currencySymbol: "$",
    providerLabel: "RentCast Global Mock",
  };
}

function localizeEstimatedValue(baseValue: number, countryCode: string) {
  switch (countryCode) {
    case "IL":
      return Math.round(baseValue * 3.65);
    case "IT":
    case "FR":
    case "ES":
      return Math.round(baseValue * 0.92);
    default:
      return Math.round(baseValue);
  }
}

function formatEstimatedValue(amount: number, currencySymbol: string) {
  return `${currencySymbol}${amount.toLocaleString()}`;
}

function getAddressComponent(
  result: GeocodeResult | undefined,
  type: string,
): GeocodeAddressComponent | undefined {
  return result?.address_components?.find((component) =>
    component.types.includes(type),
  );
}

function hasSpecificHouseNumber(
  address: string,
  addressComponents?: GeocodeAddressComponent[],
) {
  if (addressComponents?.some((component) => component.types.includes("street_number"))) {
    return true;
  }

  return /\b\d+[A-Za-z]?\b/.test(address);
}

function getPropertyInsight(
  position: LatLng,
  address: string,
  addressComponents?: GeocodeAddressComponent[],
): BuildingInsight {
  const seed = Math.abs(Math.round(position.lat * 10000) + Math.round(position.lng * 10000));
  const insight = getMockPropertyInsight(seed);
  const countryProfile = resolveCountryProfile(address);
  const baseValue = parseCurrencyAmount(insight.estimatedPropertyValue);
  const streetAverageValueNumber = localizeEstimatedValue(
    baseValue,
    countryProfile.countryCode,
  );
  const specificPropertyValueNumber =
    streetAverageValueNumber +
    ((Math.abs(Math.round(position.lat * 100000)) + Math.abs(Math.round(position.lng * 100000))) %
      9 -
      4) *
      Math.max(25000, Math.round(streetAverageValueNumber * 0.012));
  const requiresHouseNumber = !hasSpecificHouseNumber(address, addressComponents);

  return {
    position,
    address,
    estimatedPropertyValue: formatEstimatedValue(
      requiresHouseNumber ? streetAverageValueNumber : specificPropertyValueNumber,
      countryProfile.currencySymbol,
    ),
    estimatedPropertyValueNumber: requiresHouseNumber
      ? streetAverageValueNumber
      : specificPropertyValueNumber,
    streetAverageValue: formatEstimatedValue(
      streetAverageValueNumber,
      countryProfile.currencySymbol,
    ),
    streetAverageValueNumber,
    currencySymbol: countryProfile.currencySymbol,
    countryCode: countryProfile.countryCode,
    providerLabel: countryProfile.providerLabel,
    requiresHouseNumber,
    valuationMessage: requiresHouseNumber
      ? "Please add a house number for an accurate valuation"
      : null,
    lastTransactions: insight.lastTransactions,
  };
}

export const AddressExplorer = () => {
  // Requires: Maps JavaScript API, Places API (New). Enable both in Google Cloud Console.
  const apiKey = process.env.NEXT_PUBLIC_GOOGLE_MAPS_API_KEY ?? "";
  const mapRef = React.useRef<google.maps.Map | null>(null);
  const restaurantMarkersRef = React.useRef<google.maps.marker.AdvancedMarkerElement[]>([]);
  const userLocationMarkerRef = React.useRef<google.maps.marker.AdvancedMarkerElement | null>(null);
  const infoWindowRef = React.useRef<google.maps.InfoWindow | null>(null);
  const idleTimeoutRef = React.useRef<number | null>(null);
  const lastGoogleErrorRef = React.useRef<string | null>(null);
  const hasRequestedInitialLocationRef = React.useRef(false);
  const [activeSection, setActiveSection] =
    React.useState<(typeof navItems)[number]["label"]>("Explore");
  const [query, setQuery] = React.useState("");
  const [savedTab, setSavedTab] = React.useState<SavedTab>("restaurants");
  const [center, setCenter] = React.useState<LatLng | null>(null);
  const [searchBiasLocation, setSearchBiasLocation] =
    React.useState<LatLng | null>(null);
  const [searchCountryCode, setSearchCountryCode] = React.useState("");
  const [restaurants, setRestaurants] = React.useState<Restaurant[]>([]);
  const [error, setError] = React.useState<string | null>(null);
  const [map, setMap] = React.useState<google.maps.Map | null>(null);
  const [currentLocation, setCurrentLocation] = React.useState<LatLng | null>(null);
  const [selectedRestaurant, setSelectedRestaurant] = React.useState<Restaurant | null>(null);
  const [selectedBuilding, setSelectedBuilding] = React.useState<BuildingInsight | null>(null);
  const [dismissedBuilding, setDismissedBuilding] =
    React.useState<BuildingInsight | null>(null);
  const [mapsLoadError, setMapsLoadError] = React.useState<string | null>(null);
  const [savedRestaurants, setSavedRestaurants] = React.useState<SavedRestaurant[]>([]);
  const [savedProperties, setSavedProperties] = React.useState<PortfolioAsset[]>([]);
  const [portfolioAssets, setPortfolioAssets] = React.useState<PortfolioAsset[]>([]);
  const [isAddAssetOpen, setIsAddAssetOpen] = React.useState(false);
  const [assetStep, setAssetStep] = React.useState(1);
  const [assetAddressQuery, setAssetAddressQuery] = React.useState("");
  const [searchPredictions, setSearchPredictions] = React.useState<
    AutocompletePredictionItem[]
  >([]);
  const [isSearchDropdownOpen, setIsSearchDropdownOpen] = React.useState(false);
  const [assetPredictions, setAssetPredictions] = React.useState<
    AutocompletePredictionItem[]
  >([]);
  const [assetSelection, setAssetSelection] = React.useState<{
    address: string;
    position: LatLng | null;
  }>({ address: "", position: null });
  const [assetOwnership, setAssetOwnership] =
    React.useState<AssetOwnership>("owned");
  const [assetPropertyType, setAssetPropertyType] =
    React.useState<AssetPropertyType>("house");
  const [showWelcomeScreen, setShowWelcomeScreen] = React.useState(true);
  const [showUpgradeComingSoon, setShowUpgradeComingSoon] = React.useState(false);
  const [showPassword, setShowPassword] = React.useState(false);
  const [flashMessage, setFlashMessage] = React.useState<string | null>(null);
  const [locationNotice, setLocationNotice] = React.useState<string | null>(null);
  const [isWaitingForLocation, setIsWaitingForLocation] = React.useState(false);
  const [initialGeolocationComplete, setInitialGeolocationComplete] = React.useState(false);
  const [isPropertyValueChoiceOpen, setIsPropertyValueChoiceOpen] = React.useState(false);
  const [isPropertyValueAddressInputOpen, setIsPropertyValueAddressInputOpen] = React.useState(false);
  const [propertyValueAddressQuery, setPropertyValueAddressQuery] = React.useState("");
  const [propertyValuePredictions, setPropertyValuePredictions] = React.useState<
    { description: string; placeId: string }[]
  >([]);
  const [welcomeForm, setWelcomeForm] = React.useState({
    fullName: "",
    email: "",
    password: "",
  });
  const { isLoaded, loadError } = useJsApiLoader({
    id: "streetiq-google-map",
    googleMapsApiKey: apiKey,
    libraries,
    mapIds: [MAP_ID],
  });

  const searchNearbyRestaurants = React.useCallback(
    async (location: LatLng) => {
      if (!window.google?.maps?.importLibrary || !map) return;

      setRestaurants([]);
      setSelectedRestaurant(null);
      restaurantMarkersRef.current.forEach((m) => { m.map = null; });
      restaurantMarkersRef.current = [];
      if (infoWindowRef.current) infoWindowRef.current.close();

      const radius = 1500;

      try {
        const placesLib = (await window.google.maps.importLibrary(
          "places",
        )) as google.maps.PlacesLibrary;
        const { Place, SearchNearbyRankPreference } = placesLib;

        const request = {
          fields: ["id", "displayName", "location", "rating", "formattedAddress", "userRatingCount"],
          locationRestriction: {
            center: { lat: location.lat, lng: location.lng },
            radius,
          },
          includedPrimaryTypes: ["restaurant"],
          rankPreference: SearchNearbyRankPreference.DISTANCE,
          maxResultCount: 20,
        };

        const { places } = await Place.searchNearby(request);
        const bounds = map.getBounds();
        const nextRestaurants: Restaurant[] = (places ?? [])
          .filter((place) => place.location)
          .filter((place) => {
            if (!bounds || !place.location) return true;
            const loc = place.location as { lat: () => number; lng: () => number } | { lat: number; lng: number };
            const lat = typeof (loc as { lat: () => number }).lat === "function" ? (loc as { lat: () => number; lng: () => number }).lat() : (loc as { lat: number; lng: number }).lat;
            const lng = typeof (loc as { lng: () => number }).lng === "function" ? (loc as { lat: () => number; lng: () => number }).lng() : (loc as { lat: number; lng: number }).lng;
            return bounds.contains({ lat, lng });
          })
          .map((place, index) => {
            const loc = place.location as { lat: () => number; lng: () => number } | { lat: number; lng: number };
            const lat = typeof (loc as { lat: () => number }).lat === "function"
              ? (loc as { lat: () => number; lng: () => number }).lat()
              : (loc as { lat: number; lng: number }).lat;
            const lng = typeof (loc as { lng: () => number }).lng === "function"
              ? (loc as { lat: () => number; lng: () => number }).lng()
              : (loc as { lat: number; lng: number }).lng;
            const name =
              typeof place.displayName === "string"
                ? place.displayName
                : (place.displayName as unknown as { text?: string } | null)?.text ?? "Restaurant";
            return {
              id: place.id ?? `restaurant-${index}`,
              name,
              address: place.formattedAddress ?? "Address unavailable",
              rating: place.rating ?? undefined,
              reviews: place.userRatingCount ?? 0,
              location: { lat, lng },
            };
          });

        setRestaurants(nextRestaurants);
        setSelectedRestaurant((current) => {
          const nextSelected = current
            ? nextRestaurants.find((restaurant) => restaurant.id === current.id) ?? null
            : null;
          return current?.id === nextSelected?.id ? current : nextSelected;
        });
        setError(null);
      } catch (err) {
        console.error("Restaurant search error:", err);
        setRestaurants([]);
        setSelectedRestaurant(null);
        setError("Unable to load nearby restaurants right now.");
      }
    },
    [map],
  );

  const getGeocoder = React.useCallback(async () => {
    if (!window.google?.maps) {
      return null;
    }

    const mapsApi = window.google.maps as typeof google.maps & {
      importLibrary?: (libraryName: string) => Promise<unknown>;
    };

    if (typeof mapsApi.importLibrary === "function") {
      try {
        const geocodingLibrary = (await mapsApi.importLibrary("geocoding")) as {
          Geocoder?: typeof google.maps.Geocoder;
        };

        if (typeof geocodingLibrary.Geocoder === "function") {
          return new geocodingLibrary.Geocoder();
        }
      } catch {
        // Fall back to the classic constructor path below.
      }
    }

    if (typeof window.google.maps.Geocoder === "function") {
      return new window.google.maps.Geocoder();
    }

    return null;
  }, []);

  const geocodeRequest = React.useCallback(
    async (request: google.maps.GeocoderRequest) => {
      const geocoder = await getGeocoder();

      if (!geocoder) {
        throw new Error("Google geocoding is unavailable.");
      }

      return new Promise<{
        results: GeocodeResult[] | null;
        status: string;
      }>((resolve) => {
        geocoder.geocode(
          request,
          (results: GeocodeResult[] | null, status: string) => {
            resolve({ results, status });
          },
        );
      });
    },
    [getGeocoder],
  );

  const hydrateSearchContext = React.useCallback(
    (
      nextCenter: LatLng,
      fallbackCountryCode = "",
    ) => {
      setSearchBiasLocation(nextCenter);
      setSearchCountryCode(fallbackCountryCode.toLowerCase());

      if (!window.google?.maps) {
        return;
      }

      void geocodeRequest({ location: nextCenter })
        .then(({ results, status }) => {
          if (status !== "OK" || !results?.[0]) {
            return;
          }

          const country =
            getAddressComponent(results[0], "country")?.short_name?.toLowerCase() ??
            fallbackCountryCode.toLowerCase();

          setSearchCountryCode(country);
        })
        .catch(() => {
          // Keep the fallback country code if geocoding is unavailable.
        });
    },
    [geocodeRequest],
  );

  const focusMapOnLocation = React.useCallback(
    (
      nextCenter: LatLng,
      nextQuery: string,
      options?: {
        openPropertyInsight?: boolean;
        flashText?: string | null;
        zoom?: number;
        updateQuery?: boolean;
      },
    ) => {
      const zoom = options?.zoom ?? 16;

      setCenter(nextCenter);
      if (options?.updateQuery !== false) {
        setQuery(nextQuery);
      }
      setSearchPredictions([]);
      setIsSearchDropdownOpen(false);
      setSelectedRestaurant(null);
      const nextBuildingInsight =
        options?.openPropertyInsight === false
          ? null
          : getPropertyInsight(nextCenter, nextQuery);
      setSelectedBuilding(nextBuildingInsight);
      setDismissedBuilding(null);
      setError(null);
      hydrateSearchContext(nextCenter);

      if (options?.flashText) {
        setFlashMessage(options.flashText);
      }

      map?.panTo(nextCenter);
      map?.setZoom(zoom);
      searchNearbyRestaurants(nextCenter);
    },
    [hydrateSearchContext, map, searchNearbyRestaurants],
  );

  const handleSearch = React.useCallback(async () => {
    if (!window.google?.maps || !query.trim()) {
      return;
    }

    const normalizedQuery = query.trim().toLowerCase();
    if (
      normalizedQuery === "near me" ||
      normalizedQuery === "near me restaurant" ||
      normalizedQuery === "near me restaurants"
    ) {
      const targetLocation = currentLocation ?? searchBiasLocation ?? center;
      if (!targetLocation) {
        setLocationNotice("Live location is unavailable. Enable location or search a full address.");
        return;
      }
      setSearchPredictions([]);
      setIsSearchDropdownOpen(false);
      setSelectedRestaurant(null);
      setSelectedBuilding(null);
      setDismissedBuilding(null);
      setError(null);
      map?.panTo(targetLocation);
      map?.setZoom(16);
      searchNearbyRestaurants(targetLocation);
      setFlashMessage("Showing results near you");
      return;
    }

    try {
      const { results, status } = await geocodeRequest({
        address: query.trim(),
      });

      if (status !== "OK" || !results?.[0]) {
        setError("Location not found. Try a different address or city.");
        return;
      }

      const location = results[0].geometry.location;
      const nextCenter = { lat: location.lat(), lng: location.lng() };
      const formattedAddress = results[0].formatted_address ?? query.trim();

      setSearchPredictions([]);
      setQuery(formattedAddress);
      setCenter(nextCenter);
      setSelectedRestaurant(null);
      setSelectedBuilding(
        getPropertyInsight(
          nextCenter,
          formattedAddress,
          results[0].address_components,
        ),
      );
      setDismissedBuilding(null);
      setError(null);
      hydrateSearchContext(nextCenter, formattedAddress);
      map?.panTo(nextCenter);
      map?.setZoom(16);
      searchNearbyRestaurants(nextCenter);
    } catch {
      setError("Google Maps geocoding is unavailable right now.");
    }
  }, [center, currentLocation, geocodeRequest, hydrateSearchContext, map, query, searchBiasLocation, searchNearbyRestaurants]);

  const handleGeolocationError = React.useCallback(
    (geoError: GeolocationPositionError | null) => {
      let nextNotice = "Unable to access your current location.";

      console.log("[StreetIQ] navigator.geolocation error", {
        code: geoError?.code ?? null,
        message: geoError?.message ?? null,
      });

      if (geoError?.code === 1) {
        nextNotice = "Location access was denied. Enter an address in the search bar to proceed.";
      } else if (geoError?.code === 2) {
        nextNotice = "Location is unavailable. Enter an address in the search bar to proceed.";
      } else if (geoError?.code === 3) {
        nextNotice = "Location request timed out. Enter an address in the search bar to proceed.";
      } else if (
        typeof window !== "undefined" &&
        !window.isSecureContext &&
        window.location.hostname !== "localhost"
      ) {
        nextNotice = "Location access requires HTTPS or localhost. Enter an address in the search bar to proceed.";
      }

      setLocationNotice(nextNotice);
      setError(null);
      setCurrentLocation(null);
      setCenter(null);
      setSearchBiasLocation(null);
      setSelectedBuilding(null);
      setSelectedRestaurant(null);
      setDismissedBuilding(null);
      setIsWaitingForLocation(false);
      setInitialGeolocationComplete(true);
    },
    [],
  );

  const handleUseCurrentLocation = React.useCallback((source: "initial" | "recenter" = "recenter") => {
    console.log("[StreetIQ] requesting live geolocation", { source });

    if (!navigator.geolocation) {
      setLocationNotice("Location is not supported in this browser. Enter an address in the search bar to proceed.");
      setError(null);
      setCurrentLocation(null);
      setCenter(null);
      setSearchBiasLocation(null);
      setSelectedBuilding(null);
      setDismissedBuilding(null);
      setIsWaitingForLocation(false);
      setInitialGeolocationComplete(true);
      return;
    }

    if (source === "initial") {
      setIsWaitingForLocation(true);
    }

    const onSuccess = (position: GeolocationPosition) => {
      const nextCenter = {
        lat: position.coords.latitude,
        lng: position.coords.longitude,
      };

      console.log("[StreetIQ] navigator.geolocation success", {
        source,
        latitude: position.coords.latitude,
        longitude: position.coords.longitude,
        accuracy: position.coords.accuracy,
      });

      // Force map camera update via ref—required for mobile (React state may not trigger map update)
      const mapInstance = mapRef.current;
      if (mapInstance) {
        mapInstance.panTo(nextCenter);
        mapInstance.setZoom(17);
        window.google?.maps?.event?.trigger(mapInstance, "resize");
      }

      setCurrentLocation(nextCenter);
      setCenter(nextCenter);
      setSearchBiasLocation(nextCenter);
      setLocationNotice(null);
      setDismissedBuilding(null);
      setIsWaitingForLocation(false);
      setInitialGeolocationComplete(true);
        focusMapOnLocation(nextCenter, "Near me", {
          openPropertyInsight: true,
          flashText: source === "initial" ? "Showing your location" : "Showing results near you",
          updateQuery: false,
          zoom: 17,
        });
    };

    const tryGeolocation = (highAccuracy: boolean) => {
      navigator.geolocation.getCurrentPosition(
        onSuccess,
        (geoError) => {
          const isTimeoutOrUnavailable = geoError?.code === 2 || geoError?.code === 3;
          if (highAccuracy && isTimeoutOrUnavailable) {
            console.log("[StreetIQ] high-accuracy failed, retrying with low accuracy");
            tryGeolocation(false);
          } else {
            void handleGeolocationError(geoError);
          }
        },
        {
          enableHighAccuracy: highAccuracy,
          timeout: highAccuracy ? 5000 : 25000,
          maximumAge: 0,
        },
      );
    };

    tryGeolocation(true);
  }, [focusMapOnLocation, handleGeolocationError]);

  // Request real-time GPS on load; no hardcoded defaults—map centers only on live location or user search
  React.useEffect(() => {
    if (showWelcomeScreen || typeof window === "undefined") {
      return;
    }

    if (!navigator.geolocation) {
      setInitialGeolocationComplete(true);
      return;
    }

    if (hasRequestedInitialLocationRef.current) {
      return;
    }

    hasRequestedInitialLocationRef.current = true;
    handleUseCurrentLocation("initial");
  }, [handleUseCurrentLocation, showWelcomeScreen]);

  const handleRecenterToLocation = React.useCallback(() => {
    handleUseCurrentLocation("recenter");
  }, [handleUseCurrentLocation]);

  React.useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    const locationLikeKeys = Object.keys(window.localStorage).filter((key) =>
      /location|center|lat|lng/i.test(key),
    );

    console.log("[StreetIQ] location-like localStorage keys", locationLikeKeys);
  }, []);

  React.useEffect(() => {
    if (!map || !currentLocation) {
      return;
    }

    console.log("[StreetIQ] syncing map to live location", currentLocation);
    map.panTo(currentLocation);
    map.setZoom(17);
  }, [currentLocation, map]);

  React.useEffect(() => {
    if (!map || !center || currentLocation) {
      return;
    }

    console.log("[StreetIQ] syncing map to center state", center);
    map.setCenter(center);
    map.setZoom(17);
  }, [center, currentLocation, map]);

  const dismissSelectedBuilding = React.useCallback(() => {
    setDismissedBuilding((current) => current ?? selectedBuilding);
    setSelectedBuilding(null);
  }, [selectedBuilding]);

  const showRestoreButton = Boolean(selectedBuilding || dismissedBuilding || center || currentLocation);

  const handleShowPropertyValueCard = React.useCallback(() => {
    setIsPropertyValueChoiceOpen(true);
  }, []);

  const handlePropertyValueYes = React.useCallback(async () => {
    setIsPropertyValueChoiceOpen(false);
    if (!currentLocation) {
      setLocationNotice("Enable location or search an address to value your current location.");
      return;
    }
    try {
      const { results, status } = await geocodeRequest({ location: currentLocation });
      if (status !== "OK" || !results?.[0]) {
        setLocationNotice("Could not resolve your location. Try searching an address instead.");
        return;
      }
      const formattedAddress = results[0].formatted_address ?? `${currentLocation.lat.toFixed(5)}, ${currentLocation.lng.toFixed(5)}`;
      setSelectedBuilding(
        getPropertyInsight(currentLocation, formattedAddress, results[0].address_components),
      );
      setDismissedBuilding(null);
      setQuery(formattedAddress);
      setCenter(currentLocation);
      map?.panTo(currentLocation);
      map?.setZoom(17);
    } catch {
      setLocationNotice("Unable to get your address. Try searching instead.");
    }
  }, [currentLocation, geocodeRequest, map]);

  const handlePropertyValueNo = React.useCallback(() => {
    setIsPropertyValueChoiceOpen(false);
    setIsPropertyValueAddressInputOpen(true);
    setPropertyValueAddressQuery("");
    setPropertyValuePredictions([]);
  }, []);

  const handlePropertyValueSelectAddress = React.useCallback(
    async (prediction: { description: string; placeId: string }) => {
      if (!window.google?.maps) return;
      try {
        const { results, status } = await geocodeRequest({ placeId: prediction.placeId });
        if (status !== "OK" || !results?.[0]) {
          setError("Unable to resolve the selected address.");
          return;
        }
        const location = results[0].geometry.location;
        const nextCenter = { lat: location.lat(), lng: location.lng() };
        const formattedAddress = results[0].formatted_address ?? prediction.description;
        setIsPropertyValueAddressInputOpen(false);
        setPropertyValueAddressQuery("");
        setPropertyValuePredictions([]);
        setSelectedBuilding(
          getPropertyInsight(nextCenter, formattedAddress, results[0].address_components),
        );
        setDismissedBuilding(null);
        setQuery(formattedAddress);
        setCenter(nextCenter);
        hydrateSearchContext(nextCenter, formattedAddress);
        map?.panTo(nextCenter);
        map?.setZoom(17);
        searchNearbyRestaurants(nextCenter);
      } catch {
        setError("Google Maps geocoding is unavailable right now.");
      }
    },
    [geocodeRequest, hydrateSearchContext, map, searchNearbyRestaurants],
  );

  const handleSelectSearchPrediction = React.useCallback(
    async (prediction: AutocompletePredictionItem) => {
      if (!window.google?.maps) {
        return;
      }

      try {
        const { results, status } = await geocodeRequest({
          placeId: prediction.placeId,
        });

        if (status !== "OK" || !results?.[0]) {
          setError("Location not found. Try a different address or city.");
          return;
        }

        const location = results[0].geometry.location;
        const nextCenter = { lat: location.lat(), lng: location.lng() };
        const formattedAddress =
          results[0].formatted_address ?? prediction.description;

        setSearchPredictions([]);
        setQuery(formattedAddress);
        setCenter(nextCenter);
        setSelectedRestaurant(null);
        setSelectedBuilding(
          getPropertyInsight(
            nextCenter,
            formattedAddress,
            results[0].address_components,
          ),
        );
        setDismissedBuilding(null);
        setError(null);
        hydrateSearchContext(nextCenter, formattedAddress);
        map?.panTo(nextCenter);
        map?.setZoom(16);
        searchNearbyRestaurants(nextCenter);
      } catch {
        setError("Google Maps geocoding is unavailable right now.");
      }
    },
    [geocodeRequest, hydrateSearchContext, map, searchNearbyRestaurants],
  );

  const handleMapIdle = React.useCallback(() => {
    if (!map) return;

    const mapCenter = map.getCenter();
    if (!mapCenter) return;

    const viewCenter = { lat: mapCenter.lat(), lng: mapCenter.lng() };
    setCenter(viewCenter);
    searchNearbyRestaurants(viewCenter);
  }, [map, searchNearbyRestaurants]);

  React.useEffect(() => {
    if (loadError) {
      setMapsLoadError("Google Maps failed to load. Check the API key and enabled APIs.");
    }
  }, [loadError]);

  React.useEffect(() => {
    const previousAuthFailure = window.gm_authFailure;
    const handleWindowError = (event: ErrorEvent) => {
      const message =
        typeof event.message === "string"
          ? event.message.replace(/\s+/g, " ").trim()
          : "";

      if (
        message.includes("Google Maps JavaScript API") &&
        lastGoogleErrorRef.current !== message
      ) {
        lastGoogleErrorRef.current = message;
        setMapsLoadError(message);
      }
    };

    window.gm_authFailure = () => {
      const authMessage =
        "Google Maps auth failure: gm_authFailure. Check API key restrictions, billing, and allowed referrers.";
      if (lastGoogleErrorRef.current !== authMessage) {
        lastGoogleErrorRef.current = authMessage;
        setMapsLoadError(authMessage);
      }
      previousAuthFailure?.();
    };

    window.addEventListener("error", handleWindowError);

    return () => {
      window.gm_authFailure = previousAuthFailure;
      window.removeEventListener("error", handleWindowError);
    };
  }, []);

  React.useEffect(() => {
    setSavedRestaurants(readStoredState(STORAGE_KEYS.savedRestaurants, []));
    setSavedProperties(readStoredState(STORAGE_KEYS.savedProperties, []));
    setPortfolioAssets(readStoredState(STORAGE_KEYS.portfolioAssets, []));
    const account = readStoredState(STORAGE_KEYS.accountProfile, {
      fullName: "",
      email: "",
      password: "",
    });
    setWelcomeForm(account);
    if (account.email?.trim()) {
      setShowWelcomeScreen(false);
    }
  }, []);

  React.useEffect(() => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem(
        STORAGE_KEYS.savedRestaurants,
        JSON.stringify(savedRestaurants),
      );
    }
  }, [savedRestaurants]);

  React.useEffect(() => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem(
        STORAGE_KEYS.savedProperties,
        JSON.stringify(savedProperties),
      );
    }
  }, [savedProperties]);

  React.useEffect(() => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem(
        STORAGE_KEYS.portfolioAssets,
        JSON.stringify(portfolioAssets),
      );
    }
  }, [portfolioAssets]);

  React.useEffect(() => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem(
        STORAGE_KEYS.accountProfile,
        JSON.stringify(welcomeForm),
      );
    }
  }, [welcomeForm]);

  React.useEffect(() => {
    setPortfolioAssets((current) => {
      let hasChanges = false;

      const nextAssets = current.map((asset) => {
        if (
          asset.estimatedPropertyValue &&
          (asset.estimatedPropertyValueNumber ?? 0) > 0 &&
          asset.currencySymbol
        ) {
          return asset;
        }

        const insight = getPropertyInsight(asset.position, asset.address);
        hasChanges = true;

        return {
          ...asset,
          estimatedPropertyValue: insight.estimatedPropertyValue,
          estimatedPropertyValueNumber: insight.estimatedPropertyValueNumber,
          currencySymbol: insight.currencySymbol,
          countryCode: insight.countryCode,
        };
      });

      return hasChanges ? nextAssets : current;
    });
  }, []);

  React.useEffect(() => {
    setSavedProperties((current) => {
      let hasChanges = false;

      const nextProperties = current.map((property) => {
        if (
          property.estimatedPropertyValue &&
          (property.estimatedPropertyValueNumber ?? 0) > 0 &&
          property.currencySymbol
        ) {
          return property;
        }

        const insight = getPropertyInsight(property.position, property.address);
        hasChanges = true;

        return {
          ...property,
          estimatedPropertyValue: insight.estimatedPropertyValue,
          estimatedPropertyValueNumber: insight.estimatedPropertyValueNumber,
          currencySymbol: insight.currencySymbol,
          countryCode: insight.countryCode,
        };
      });

      return hasChanges ? nextProperties : current;
    });
  }, []);

  const filteredRestaurants = restaurants;

  const ownedAssets = React.useMemo(
    () => portfolioAssets.filter((asset) => asset.ownership === "owned"),
    [portfolioAssets],
  );

  const wishlistAssets = React.useMemo(
    () => portfolioAssets.filter((asset) => asset.ownership === "wishlist"),
    [portfolioAssets],
  );

  const ownedTotalValue = React.useMemo(
    () =>
      ownedAssets.reduce(
        (total, asset) =>
          total +
          (asset.estimatedPropertyValueNumber ??
            parseCurrencyAmount(asset.estimatedPropertyValue)),
        0,
      ),
    [ownedAssets],
  );

  const ownedPrimaryCurrencySymbol = React.useMemo(
    () => ownedAssets[0]?.currencySymbol ?? "$",
    [ownedAssets],
  );

  const isRestaurantSaved = React.useCallback(
    (restaurantId: string) =>
      savedRestaurants.some((restaurant) => restaurant.id === restaurantId),
    [savedRestaurants],
  );

  const isPropertySaved = React.useCallback(
    (address: string) =>
      savedProperties.some((property) => property.address === address),
    [savedProperties],
  );

  const toggleSavedRestaurant = React.useCallback((restaurant: SavedRestaurant) => {
    setSavedRestaurants((current) =>
      current.some((item) => item.id === restaurant.id)
        ? current.filter((item) => item.id !== restaurant.id)
        : [restaurant, ...current],
    );
  }, []);

  React.useEffect(() => {
    if (!map || !window.google?.maps?.importLibrary || !isLoaded) return;
    if (!selectedRestaurant && infoWindowRef.current) infoWindowRef.current.close();

    const setupMarkers = async () => {
      if (userLocationMarkerRef.current) {
        userLocationMarkerRef.current.map = null;
        userLocationMarkerRef.current = null;
      }
      restaurantMarkersRef.current.forEach((m) => { m.map = null; });
      restaurantMarkersRef.current = [];
      if (!infoWindowRef.current) infoWindowRef.current = new window.google.maps.InfoWindow();

      const markerLib = (await window.google.maps.importLibrary("marker")) as google.maps.MarkerLibrary;
      const { AdvancedMarkerElement, PinElement } = markerLib;

      if (currentLocation) {
        const userPin = new PinElement({
          background: "#4285F4",
          borderColor: "#ffffff",
          glyphColor: "#ffffff",
          scale: 1.2,
        });
        const userMarker = new AdvancedMarkerElement({
          map,
          position: currentLocation,
          title: "You are here",
          content: userPin.element,
          zIndex: 1000,
        });
        userLocationMarkerRef.current = userMarker;
      }

      for (const restaurant of filteredRestaurants) {
        const pin = new PinElement({
          background: selectedRestaurant?.id === restaurant.id ? "#f59e0b" : "#d4af37",
          borderColor: "#1b1f24",
          glyphColor: "#1b1f24",
          scale: 1.1,
        });
        const marker = new AdvancedMarkerElement({
          map,
          position: restaurant.location,
          title: restaurant.name,
          content: pin.element,
          zIndex: selectedRestaurant?.id === restaurant.id ? 1001 : 1,
        });
        marker.addListener("click", () => {
          setSelectedRestaurant(restaurant);
          setSelectedBuilding(null);
          setDismissedBuilding(null);
          map.panTo(restaurant.location);
          if (infoWindowRef.current) {
            const div = document.createElement("div");
            div.className = "max-w-[220px] bg-white p-3 text-black";
            div.innerHTML = `
              <div class="flex items-start justify-between gap-3">
                <div>
                  <div class="text-sm font-semibold">${restaurant.name}</div>
                  <div class="mt-1 text-xs text-zinc-500">${restaurant.address}</div>
                </div>
              </div>
              <div class="mt-3 flex items-center gap-3 text-sm text-zinc-700">
                <span class="font-medium text-amber-600">★ ${restaurant.rating?.toFixed(1) ?? "N/A"}</span>
                <span>${restaurant.reviews} reviews</span>
              </div>
            `;
            const saveBtn = document.createElement("button");
            saveBtn.type = "button";
            saveBtn.className = "mt-2 rounded-full border border-amber-400/30 px-2 py-1 text-amber-500 text-xs";
            saveBtn.textContent = "Save";
            saveBtn.onclick = () => toggleSavedRestaurant(restaurant);
            div.appendChild(saveBtn);
            infoWindowRef.current.setContent(div);
            infoWindowRef.current.open({ map, anchor: marker });
            window.google.maps.event.addListenerOnce(infoWindowRef.current, "closeclick", () => setSelectedRestaurant(null));
          }
        });
        restaurantMarkersRef.current.push(marker);
      }
    };
    setupMarkers();
    return () => {
      if (userLocationMarkerRef.current) {
        userLocationMarkerRef.current.map = null;
        userLocationMarkerRef.current = null;
      }
      restaurantMarkersRef.current.forEach((m) => { m.map = null; });
      restaurantMarkersRef.current = [];
    };
  }, [map, filteredRestaurants, selectedRestaurant, currentLocation, isLoaded, toggleSavedRestaurant]);

  const toggleSavedProperty = React.useCallback((property: PortfolioAsset) => {
    setSavedProperties((current) =>
      current.some((item) => item.address === property.address)
        ? current.filter((item) => item.address !== property.address)
        : [property, ...current],
    );
  }, []);

  const openSavedRestaurant = React.useCallback(
    (restaurant: SavedRestaurant) => {
      setActiveSection("Explore");
      setCenter(restaurant.location);
      setSelectedBuilding(null);
      setDismissedBuilding(null);
      setSelectedRestaurant(restaurant);
      map?.panTo(restaurant.location);
      map?.setZoom(16);
    },
    [map],
  );

  const openSavedProperty = React.useCallback(
    (property: PortfolioAsset) => {
      setActiveSection("Explore");
      setCenter(property.position);
      setSelectedRestaurant(null);
      setSelectedBuilding(
        getPropertyInsight(property.position, property.address),
      );
      setDismissedBuilding(null);
      map?.panTo(property.position);
      map?.setZoom(16);
    },
    [map],
  );

  const resetAssetFlow = React.useCallback(() => {
    setAssetStep(1);
    setAssetAddressQuery("");
    setAssetPredictions([]);
    setAssetSelection({ address: "", position: null });
    setAssetOwnership("owned");
    setAssetPropertyType("house");
  }, []);

  const handleSelectAssetPrediction = React.useCallback(
    async (prediction: AutocompletePredictionItem) => {
      if (!window.google?.maps) {
        return;
      }

      try {
        const { results, status } = await geocodeRequest({
          placeId: prediction.placeId,
        });

        if (status !== "OK" || !results?.[0]) {
          setError("Unable to resolve the selected address.");
          return;
        }

        const location = results[0].geometry.location;
        setAssetSelection({
          address: prediction.description,
          position: { lat: location.lat(), lng: location.lng() },
        });
        setAssetAddressQuery(prediction.description);
        setAssetPredictions([]);
      } catch {
        setError("Google Maps geocoding is unavailable right now.");
      }
    },
    [geocodeRequest],
  );

  const handleSaveAsset = React.useCallback(() => {
    if (!assetSelection.position || !assetSelection.address) {
      return;
    }

    const insight = getPropertyInsight(
      assetSelection.position,
      assetSelection.address,
    );

    const nextAsset: PortfolioAsset = {
      id: `${assetOwnership}-${assetPropertyType}-${Date.now()}`,
      address: assetSelection.address,
      position: assetSelection.position,
      ownership: assetOwnership,
      propertyType: assetPropertyType,
      estimatedPropertyValue: insight.estimatedPropertyValue,
      estimatedPropertyValueNumber: insight.estimatedPropertyValueNumber,
      currencySymbol: insight.currencySymbol,
      countryCode: insight.countryCode,
    };

    setPortfolioAssets((current) => [nextAsset, ...current]);
    setCenter(nextAsset.position);
    setSelectedBuilding(getPropertyInsight(nextAsset.position, nextAsset.address));
    setDismissedBuilding(null);
    setActiveSection("Portfolio");
    setIsAddAssetOpen(false);
    resetAssetFlow();
  }, [
    assetOwnership,
    assetPropertyType,
    assetSelection.address,
    assetSelection.position,
    resetAssetFlow,
  ]);

  const handleDeleteAsset = React.useCallback((assetId: string) => {
    setPortfolioAssets((current) =>
      current.filter((asset) => asset.id !== assetId),
    );
  }, []);

  const [isSigningUp, setIsSigningUp] = React.useState(false);
  const [signUpError, setSignUpError] = React.useState<string | null>(null);

  const handleGetStarted = React.useCallback(async () => {
    const email = welcomeForm.email.trim();
    const password = welcomeForm.password;

    if (!email || !password) {
      setSignUpError("Please enter your email and password.");
      console.log("[StreetIQ] Sign up validation failed: missing email or password");
      return;
    }

    if (password.length < 6) {
      setSignUpError("Password must be at least 6 characters.");
      console.log("[StreetIQ] Sign up validation failed: password too short");
      return;
    }

    const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL;
    const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;

    if (!supabaseUrl || !supabaseAnonKey) {
      setSignUpError("Supabase is not configured. Check NEXT_PUBLIC_SUPABASE_URL and NEXT_PUBLIC_SUPABASE_ANON_KEY.");
      console.error("[StreetIQ] Sign up failed: Supabase env vars missing", {
        hasUrl: Boolean(supabaseUrl),
        hasAnonKey: Boolean(supabaseAnonKey),
      });
      return;
    }

    setIsSigningUp(true);
    setSignUpError(null);

    try {
      const supabase = createClient();
      const { data, error } = await supabase.auth.signUp({
        email,
        password,
        options: {
          data: {
            full_name: welcomeForm.fullName.trim() || undefined,
          },
        },
      });

      if (error) {
        setSignUpError(error.message);
        console.error("[StreetIQ] Sign up error", {
          message: error.message,
          code: error.code,
          status: error.status,
        });
        return;
      }

      if (data.user) {
        const { error: profileError } = await supabase
          .from("profiles")
          .upsert(
            {
              id: data.user.id,
              role: "basic",
              full_name: welcomeForm.fullName.trim() || null,
            },
            { onConflict: "id" },
          );

        if (profileError) {
          console.error("[StreetIQ] Profile upsert error", profileError);
        } else {
          console.log("[StreetIQ] Profile created/updated for user", data.user.id);
        }
      }

      console.log("[StreetIQ] Sign up success", {
        userId: data.user?.id,
        email: data.user?.email,
        needsEmailConfirmation: data.user && !data.session,
      });

      hasRequestedInitialLocationRef.current = false;
      setShowWelcomeScreen(false);
      setActiveSection("Explore");
      setShowPassword(false);
      setFlashMessage("Account created successfully!");
    } catch (err) {
      const message = err instanceof Error ? err.message : "An unexpected error occurred.";
      setSignUpError(message);
      console.error("[StreetIQ] Sign up exception", err);
    } finally {
      setIsSigningUp(false);
    }
  }, [welcomeForm.email, welcomeForm.password, welcomeForm.fullName]);

  React.useEffect(() => {
    if (!filteredRestaurants.length) {
      setSelectedRestaurant(null);
      return;
    }

    setSelectedRestaurant((current) => {
      const nextSelected = current
        ? filteredRestaurants.find((restaurant) => restaurant.id === current.id) ??
          null
        : null;

      return current?.id === nextSelected?.id ? current : nextSelected;
    });
  }, [filteredRestaurants]);

  React.useEffect(() => {
    if (!isAddAssetOpen || !isLoaded || !window.google?.maps?.importLibrary) {
      setAssetPredictions([]);
      return;
    }

    const trimmedQuery = assetAddressQuery.trim();
    if (trimmedQuery.length < 3) {
      setAssetPredictions([]);
      return;
    }

    (async () => {
      try {
        const placesLib = (await window.google.maps.importLibrary("places")) as google.maps.PlacesLibrary;
        const { AutocompleteSuggestion } = placesLib;
        const { suggestions } = await AutocompleteSuggestion.fetchAutocompleteSuggestions({
          input: trimmedQuery,
        });
        setAssetPredictions(
          (suggestions ?? []).slice(0, 5).map((s) => {
            const p = s.placePrediction;
            return {
              description: p?.text?.text ?? "",
              placeId: p?.placeId ?? "",
            };
          }).filter((x) => x.placeId),
        );
      } catch {
        setAssetPredictions([]);
      }
    })();
  }, [assetAddressQuery, isAddAssetOpen, isLoaded]);

  React.useEffect(() => {
    if (!isLoaded || !window.google?.maps?.importLibrary || !isSearchDropdownOpen) {
      setSearchPredictions([]);
      return;
    }

    const trimmedQuery = query.trim();
    if (trimmedQuery.length < 2) {
      setSearchPredictions([]);
      return;
    }

    const activeBiasLocation = currentLocation ?? searchBiasLocation;
    if (!activeBiasLocation) {
      setSearchPredictions([]);
      return;
    }

    (async () => {
      try {
        const placesLib = (await window.google.maps.importLibrary("places")) as google.maps.PlacesLibrary;
        const { AutocompleteSuggestion } = placesLib;
        const { suggestions } = await AutocompleteSuggestion.fetchAutocompleteSuggestions({
          input: trimmedQuery,
          locationBias: {
            center: { lat: activeBiasLocation.lat, lng: activeBiasLocation.lng },
            radius: 1500,
          },
          ...(searchCountryCode ? { includedRegionCodes: [searchCountryCode.toUpperCase()] } : {}),
        });
        setSearchPredictions(
          (suggestions ?? []).slice(0, 6).map((s) => {
            const p = s.placePrediction;
            return {
              description: p?.text?.text ?? "",
              placeId: p?.placeId ?? "",
            };
          }).filter((x) => x.placeId),
        );
      } catch {
        setSearchPredictions([]);
      }
    })();
  }, [currentLocation, isLoaded, isSearchDropdownOpen, query, searchBiasLocation, searchCountryCode]);

  React.useEffect(() => {
    if (!isLoaded || !window.google?.maps?.importLibrary || !isPropertyValueAddressInputOpen) {
      setPropertyValuePredictions([]);
      return;
    }
    const trimmed = propertyValueAddressQuery.trim();
    if (trimmed.length < 3) {
      setPropertyValuePredictions([]);
      return;
    }
    (async () => {
      try {
        const placesLib = (await window.google.maps.importLibrary("places")) as google.maps.PlacesLibrary;
        const { AutocompleteSuggestion } = placesLib;
        const { suggestions } = await AutocompleteSuggestion.fetchAutocompleteSuggestions({
          input: trimmed,
        });
        setPropertyValuePredictions(
          (suggestions ?? []).slice(0, 5).map((s) => {
            const p = s.placePrediction;
            return {
              description: p?.text?.text ?? "",
              placeId: p?.placeId ?? "",
            };
          }).filter((x) => x.placeId),
        );
      } catch {
        setPropertyValuePredictions([]);
      }
    })();
  }, [isPropertyValueAddressInputOpen, isLoaded, propertyValueAddressQuery]);

  React.useEffect(() => {
    if (!flashMessage) {
      return;
    }

    const timeout = window.setTimeout(() => {
      setFlashMessage(null);
    }, 2500);

    return () => window.clearTimeout(timeout);
  }, [flashMessage]);

  React.useEffect(() => {
    if (!locationNotice) {
      return;
    }

    const timeout = window.setTimeout(() => {
      setLocationNotice(null);
    }, 4000);

    return () => window.clearTimeout(timeout);
  }, [locationNotice]);

  React.useEffect(() => {
    if (!error) {
      return;
    }

    const normalizedError = error.toLowerCase();
    const isLocationError =
      normalizedError.includes("current location") ||
      normalizedError.includes("enable location") ||
      normalizedError.includes("geolocation") ||
      normalizedError.includes("https or localhost");

    if (!isLocationError) {
      return;
    }

    setLocationNotice(error);
    setError(null);
  }, [error]);

  React.useEffect(() => {
    return () => {
      if (idleTimeoutRef.current !== null) {
        window.clearTimeout(idleTimeoutRef.current);
      }
    };
  }, []);

  const renderExplore = () => (
    <div className="relative min-h-0 flex-1 flex flex-col gap-0 overflow-hidden">
      <div className="relative flex h-full min-h-0 flex-col gap-0 overflow-hidden bg-[#000000]">
        <div className="shrink-0 p-3">
          <div className="flex items-center gap-2">
            <div className="relative min-w-0 flex-1">
              <Search className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-amber-400" />
              <input
                value={query}
                onChange={(event) => {
                  const nextQuery = event.target.value;
                  setQuery(nextQuery);
                  setIsSearchDropdownOpen(nextQuery.trim().length >= 2);
                  if (!nextQuery.trim()) {
                    setSearchPredictions([]);
                  }
                  setSelectedBuilding(null);
                }}
                onFocus={() => {
                  if (query.trim().length >= 2) {
                    setIsSearchDropdownOpen(true);
                  }
                }}
                onKeyDown={(event) => {
                  if (event.key === "Enter") {
                    setIsSearchDropdownOpen(false);
                    handleSearch();
                  }
                }}
                placeholder="Search addresses or streets..."
                className="h-12 w-full rounded-2xl border border-amber-400/40 bg-[#101010] pl-10 pr-4 text-white outline-none placeholder:text-zinc-400 focus:border-amber-400"
              />
              {searchPredictions.length ? (
                <div className="absolute inset-x-0 top-full z-20 mt-2 rounded-2xl border border-amber-400/20 bg-black/95 p-2 shadow-2xl backdrop-blur">
                  <div className="space-y-1">
                    {searchPredictions.map((prediction) => (
                      <button
                        key={prediction.placeId}
                        type="button"
                        onClick={() => handleSelectSearchPrediction(prediction)}
                        className="w-full rounded-xl px-3 py-2 text-left text-sm text-white transition-colors hover:bg-white/5"
                      >
                        {prediction.description}
                      </button>
                    ))}
                  </div>
                </div>
              ) : null}
            </div>

            <button
              type="button"
              onClick={handleSearch}
              className="inline-flex size-12 shrink-0 items-center justify-center rounded-2xl bg-amber-400 text-black transition-colors hover:bg-amber-300"
            >
              <Search className="size-5" />
            </button>
          </div>
        </div>

        <div className="relative min-h-0 flex-1 overflow-hidden bg-[#000000]" style={{ marginBottom: "60px" }}>
          {!apiKey ? (
            <div className="flex h-full items-center justify-center p-6 text-center text-amber-300">
              Missing `NEXT_PUBLIC_GOOGLE_MAPS_API_KEY` in `.env.local`.
            </div>
          ) : mapsLoadError ? (
            <div className="flex h-full items-center justify-center p-6 text-center text-amber-300">
              {mapsLoadError}
            </div>
          ) : !isLoaded ? (
            <div className="flex h-full items-center justify-center p-6 text-center text-amber-300">
              Loading StreetIQ map...
            </div>
          ) : (
            <>
            <div className="relative w-full shrink-0 touch-pan-x touch-pan-y" style={{ height: "min(55vh, 50dvh)", minHeight: "280px" }}>
            <GoogleMap
              key={center ? "loaded" : "loading"}
              mapContainerStyle={mapContainerStyle}
              mapContainerClassName="h-full min-h-[280px] w-full bg-[#1b1f24]"
              center={currentLocation ?? center ?? (initialGeolocationComplete ? defaultCenter : WORLD_VIEW_CENTER)}
              zoom={currentLocation ?? center ? 17 : (initialGeolocationComplete ? 15 : WORLD_VIEW_ZOOM)}
              onLoad={(instance) => {
                mapRef.current = instance;
                setMap(instance);

                console.log("[StreetIQ] GoogleMap loaded", {
                  hasCurrentLocation: Boolean(currentLocation),
                  center,
                });

                if (currentLocation) {
                  console.log("[StreetIQ] applying live location on map load", currentLocation);
                  instance.panTo(currentLocation);
                  instance.setZoom(17);
                } else if (center) {
                  console.log("[StreetIQ] applying searched/fallback center on map load", center);
                  instance.setCenter(center);
                  instance.setZoom(17);
                } else if (initialGeolocationComplete) {
                  instance.setCenter(defaultCenter);
                  instance.setZoom(15);
                } else {
                  instance.setCenter(WORLD_VIEW_CENTER);
                  instance.setZoom(WORLD_VIEW_ZOOM);
                }

                // Force map to recalculate tiles (fixes gray/blank map on some setups)
                window.requestAnimationFrame(() => {
                  window.google?.maps?.event?.trigger(instance, "resize");
                });
              }}
              onUnmount={() => {
                if (userLocationMarkerRef.current) {
                  userLocationMarkerRef.current.map = null;
                  userLocationMarkerRef.current = null;
                }
                restaurantMarkersRef.current.forEach((m) => { m.map = null; });
                restaurantMarkersRef.current = [];
                if (infoWindowRef.current) infoWindowRef.current.close();
                mapRef.current = null;
                setMap(null);
              }}
              onIdle={handleMapIdle}
              onClick={(event) => {
                if (!event.latLng) {
                  return;
                }

                setSearchPredictions([]);
                setIsSearchDropdownOpen(false);
                setSelectedRestaurant(null);
                setDismissedBuilding(null);
                setSelectedBuilding(
                  getPropertyInsight(
                    {
                      lat: event.latLng.lat(),
                      lng: event.latLng.lng(),
                    },
                    `${event.latLng.lat().toFixed(5)}, ${event.latLng.lng().toFixed(5)}`,
                  ),
                );
              }}
              options={{
                mapId: MAP_ID,
                mapTypeId: "roadmap",
                mapTypeControl: false,
                streetViewControl: false,
                fullscreenControl: false,
                clickableIcons: false,
                gestureHandling: "greedy",
                draggable: true,
                scrollwheel: true,
                styles: [
                  { elementType: "geometry", stylers: [{ color: "#24272c" }] },
                  { elementType: "labels.text.fill", stylers: [{ color: "#cfd4dc" }] },
                  { elementType: "labels.text.stroke", stylers: [{ color: "#1b1f24" }] },
                  { featureType: "administrative", elementType: "geometry.stroke", stylers: [{ color: "#3b4149" }] },
                  { featureType: "administrative.land_parcel", elementType: "labels.text.fill", stylers: [{ color: "#8a919c" }] },
                  { featureType: "landscape", elementType: "geometry", stylers: [{ color: "#2a2f36" }] },
                  { featureType: "poi", elementType: "geometry", stylers: [{ color: "#303640" }] },
                  { featureType: "poi", elementType: "labels.text.fill", stylers: [{ color: "#d4af37" }] },
                  { featureType: "road", elementType: "geometry", stylers: [{ color: "#505761" }] },
                  { featureType: "road", elementType: "geometry.stroke", stylers: [{ color: "#2d3238" }] },
                  { featureType: "road", elementType: "labels.text.fill", stylers: [{ color: "#e5e7eb" }] },
                  { featureType: "road.highway", elementType: "geometry", stylers: [{ color: "#676f7a" }] },
                  { featureType: "road.highway", elementType: "geometry.stroke", stylers: [{ color: "#3a4048" }] },
                  { featureType: "transit", elementType: "geometry", stylers: [{ color: "#353b44" }] },
                  { featureType: "transit.station", elementType: "labels.text.fill", stylers: [{ color: "#d4af37" }] },
                  { featureType: "water", elementType: "geometry", stylers: [{ color: "#1d2f3f" }] },
                  { featureType: "water", elementType: "labels.text.fill", stylers: [{ color: "#aab6c3" }] },
                ],
              }}
            >
              {/* Restaurant + user location markers via AdvancedMarkerElement in useEffect */}
            </GoogleMap>
            </div>

            {isWaitingForLocation && (
              <div className="absolute inset-0 z-[10000] flex flex-col items-center justify-center gap-4 bg-black/60 backdrop-blur-sm">
                <div className="size-10 animate-spin rounded-full border-2 border-amber-400 border-t-transparent" />
                <p className="text-amber-300">Loading your location...</p>
                <p className="text-xs text-zinc-500">
                  Allow location access when prompted.
                </p>
                <button
                  type="button"
                  onClick={() => {
                    setIsWaitingForLocation(false);
                    setCenter(WORLD_VIEW_CENTER);
                    setLocationNotice("Search for an address to get started.");
                  }}
                  className="mt-2 inline-flex items-center gap-2 rounded-full border border-amber-400/50 bg-amber-400/20 px-4 py-2 text-sm font-medium text-amber-300 transition-colors hover:bg-amber-400/30"
                >
                  <Search className="size-4" />
                  Manual Search
                </button>
              </div>
            )}
            </>
          )}

          <div className="pointer-events-none absolute inset-y-4 right-3 z-30 flex flex-col items-end justify-between">
            <div />
            <button
              type="button"
              onClick={handleRecenterToLocation}
              className="pointer-events-auto inline-flex items-center gap-2 rounded-full border border-sky-400/30 bg-black/80 px-3 py-2 text-xs font-medium text-sky-200 shadow-lg shadow-sky-500/10 backdrop-blur hover:bg-[#151515]"
            >
              <span className="size-2.5 rounded-full bg-sky-400 shadow-[0_0_10px_rgba(56,189,248,0.8)]" />
              <span>Recenter</span>
            </button>
          </div>

          {error ? (
            <div className="pointer-events-none absolute inset-x-4 top-24 z-10">
              <div className="rounded-2xl border border-amber-400/20 bg-black/85 p-3 text-sm text-amber-200 backdrop-blur">
                {error}
              </div>
            </div>
          ) : null}

          {locationNotice ? (
            <div className="pointer-events-none absolute inset-x-4 bottom-4 z-20 flex justify-end">
              <div className="pointer-events-auto flex w-full max-w-sm items-start justify-between gap-3 rounded-2xl border border-amber-400/20 bg-black/90 p-3 text-sm text-amber-200 shadow-2xl backdrop-blur">
                <div>{locationNotice}</div>
                <button
                  type="button"
                  onClick={() => setLocationNotice(null)}
                  className="rounded-full border border-white/10 p-1 text-zinc-400 hover:text-white"
                >
                  <X className="size-4" />
                </button>
              </div>
            </div>
          ) : null}

          {mapsLoadError ? (
            <div className="pointer-events-none absolute inset-x-4 top-40 z-10">
              <div className="rounded-2xl border border-red-500/30 bg-black/90 p-3 text-sm text-red-300 backdrop-blur">
                <div className="font-medium">Google Maps error</div>
                <div className="mt-1 break-words">{mapsLoadError}</div>
              </div>
            </div>
          ) : null}

          {selectedBuilding ? (
            <div className="pointer-events-none absolute inset-x-4 bottom-4 z-10">
              <div className="pointer-events-auto ml-auto max-w-[300px] rounded-2xl border border-amber-400/20 bg-black/90 p-4 text-white shadow-2xl backdrop-blur">
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0">
                    <div className="text-xs uppercase tracking-[0.18em] text-amber-300">
                      Property Value
                    </div>
                    <div className="mt-2 truncate text-sm font-semibold text-white">
                      {selectedBuilding.address}
                    </div>
                  </div>
                  <div className="flex items-center gap-2">
                    <button
                      type="button"
                      onClick={() =>
                        toggleSavedProperty({
                          id: `saved-${selectedBuilding.address}`,
                          address: selectedBuilding.address,
                          position: selectedBuilding.position,
                          ownership: "wishlist",
                          propertyType: "apartment",
                          estimatedPropertyValue:
                            selectedBuilding.estimatedPropertyValue,
                          estimatedPropertyValueNumber:
                            selectedBuilding.estimatedPropertyValueNumber,
                          currencySymbol: selectedBuilding.currencySymbol,
                          countryCode: selectedBuilding.countryCode,
                        })
                      }
                      className="rounded-full border border-amber-400/30 p-1 text-amber-500"
                    >
                      <Heart
                        className={[
                          "size-3.5",
                          isPropertySaved(selectedBuilding.address)
                            ? "fill-amber-500 text-amber-500"
                            : "",
                        ].join(" ")}
                      />
                    </button>
                    <button
                      type="button"
                      onClick={dismissSelectedBuilding}
                      className="rounded-full border border-white/10 p-1 text-zinc-400 hover:text-white"
                    >
                      <X className="size-4" />
                    </button>
                  </div>
                </div>
                <div className="mt-4 rounded-xl border border-amber-400/15 bg-amber-400/10 px-3 py-2">
                  <div className="text-[11px] uppercase tracking-[0.18em] text-amber-200/80">
                    Estimated Value
                  </div>
                  {selectedBuilding.requiresHouseNumber ? (
                    <div className="mt-1 text-sm font-medium text-amber-100">
                      {selectedBuilding.valuationMessage}
                    </div>
                  ) : (
                    <div className="mt-1 text-lg font-bold text-amber-300">
                      {selectedBuilding.estimatedPropertyValue}
                    </div>
                  )}
                </div>
                <div className="mt-3 rounded-xl border border-white/10 bg-white/5 px-3 py-2">
                  <div className="text-[11px] uppercase tracking-[0.18em] text-zinc-400">
                    Street Average
                  </div>
                  <div className="mt-1 text-sm font-semibold text-white">
                    {selectedBuilding.streetAverageValue}
                  </div>
                </div>
              </div>
            </div>
          ) : null}

          {showRestoreButton ? (
            <div className="pointer-events-none absolute right-4 z-50" style={{ bottom: "20px" }}>
              <div className="pointer-events-auto">
                <button
                  type="button"
                  onClick={handleShowPropertyValueCard}
                  aria-label="Show property value"
                  title="Show property value"
                  className="inline-flex items-center gap-2 rounded-full border border-amber-300 bg-amber-400 px-4 py-2 text-sm font-semibold text-black shadow-xl shadow-amber-500/20 ring-1 ring-amber-200/60 transition-colors hover:bg-amber-300"
                >
                  <BadgeDollarSign className="size-5 shrink-0 stroke-[2.4] text-black" />
                  <span>Property Value</span>
                </button>
              </div>
            </div>
          ) : null}

        </div>
      </div>
    </div>
  );

  const renderSaved = () => (
    <div className="flex h-full min-h-0 flex-1 flex-col overflow-y-auto px-4 py-5 pb-24">
      <div className="space-y-5">
        <div>
          <div className="inline-flex items-center gap-2 text-sm text-amber-300">
            <Heart className="size-4" />
            <span>Saved</span>
          </div>
          <h2 className="mt-2 text-4xl font-semibold text-white">Your Saved Items</h2>
          <p className="mt-2 text-base text-zinc-400">Stored locally in your browser.</p>
        </div>

        <div className="rounded-3xl border border-amber-400/20 bg-[#161116] p-6">
          <div className="flex flex-wrap items-center justify-center gap-3">
            <button
              type="button"
              onClick={() => setSavedTab("restaurants")}
              className={[
                "rounded-xl px-5 py-3 text-sm font-medium transition-colors",
                savedTab === "restaurants"
                  ? "bg-amber-400 text-black"
                  : "border border-amber-400/30 text-amber-300",
              ].join(" ")}
            >
              Restaurants
            </button>
            <button
              type="button"
              onClick={() => setSavedTab("properties")}
              className={[
                "rounded-xl px-5 py-3 text-sm font-medium transition-colors",
                savedTab === "properties"
                  ? "bg-amber-400 text-black"
                  : "border border-amber-400/30 text-amber-300",
              ].join(" ")}
            >
              Properties
            </button>
          </div>

          <div className="mt-6 space-y-3">
            {savedTab === "restaurants" && savedRestaurants.length === 0 ? (
              <div className="text-center">
                <div className="text-2xl font-semibold text-white">
                  No saved restaurants yet
                </div>
                <p className="mx-auto mt-3 max-w-md text-sm text-zinc-400">
                  Save restaurants from the map and they&apos;ll appear here.
                </p>
              </div>
            ) : null}

            {savedTab === "properties" && savedProperties.length === 0 ? (
              <div className="text-center">
                <div className="text-2xl font-semibold text-white">
                  No saved properties yet
                </div>
                <p className="mx-auto mt-3 max-w-md text-sm text-zinc-400">
                  Save property insights from the map and they&apos;ll appear here.
                </p>
              </div>
            ) : null}

            {savedTab === "restaurants"
              ? savedRestaurants.map((restaurant) => (
                  <button
                    key={restaurant.id}
                    type="button"
                    onClick={() => openSavedRestaurant(restaurant)}
                    className="w-full rounded-2xl border border-amber-400/20 bg-black/70 p-4 text-left backdrop-blur"
                  >
                    <div className="flex items-start justify-between gap-3">
                      <div>
                        <div className="font-medium text-white">{restaurant.name}</div>
                        <div className="mt-1 text-sm text-zinc-400">
                          {restaurant.address}
                        </div>
                      </div>
                      <Bookmark className="size-4 fill-amber-400 text-amber-400" />
                    </div>
                  </button>
                ))
              : savedProperties.map((property) => (
                  <button
                    key={property.id}
                    type="button"
                    onClick={() => openSavedProperty(property)}
                    className="w-full rounded-2xl border border-amber-400/20 bg-black/70 p-4 text-left backdrop-blur"
                  >
                    <div className="flex items-start justify-between gap-3">
                      <div>
                        <div className="font-medium text-white">{property.address}</div>
                        <div className="mt-1 text-sm text-zinc-400">
                          {property.propertyType === "house"
                            ? "Private House"
                            : "Apartment"}{" "}
                          • {property.ownership === "owned" ? "I own this" : "I want to buy this"}
                        </div>
                      </div>
                      <Bookmark className="size-4 fill-amber-400 text-amber-400" />
                    </div>
                  </button>
                ))}
          </div>
        </div>
      </div>
    </div>
  );

  const renderPortfolio = () => (
    <div className="flex h-full min-h-0 flex-1 flex-col overflow-y-auto px-4 py-5 pb-24">
      <div className="space-y-5">
        <div className="flex items-start justify-between gap-4">
          <div>
            <h2 className="text-4xl font-semibold text-amber-400">Portfolio</h2>
            <p className="mt-2 max-w-md text-base text-amber-100/80">
              Manage your owned properties and track assets you intend to purchase.
            </p>
          </div>
          <button
            type="button"
            onClick={() => {
              resetAssetFlow();
              setIsAddAssetOpen(true);
            }}
            className="shrink-0 rounded-xl bg-amber-400 px-5 py-3 text-sm font-medium text-black"
          >
            <span className="inline-flex items-center gap-2">
              <Plus className="size-4" />
              Add Asset
            </span>
          </button>
        </div>

        <div>
          <h3 className="text-2xl font-semibold text-white">Current Portfolio (Owned)</h3>
          <div className="mt-4 rounded-3xl border border-amber-400/20 bg-[#161116] p-6">
            <p className="text-sm font-medium text-zinc-400">Total Value of Owned Assets</p>
            <div className="mt-4 text-6xl font-semibold text-amber-400">
              {ownedPrimaryCurrencySymbol}
              {ownedTotalValue.toLocaleString()}
            </div>
          </div>
          {ownedAssets.length ? (
            <div className="mt-3 space-y-3">
              {ownedAssets.map((asset) => (
                <div
                  key={asset.id}
                  className="w-full rounded-2xl border border-amber-400/15 bg-[#161116] p-5"
                >
                  <div className="flex items-start justify-between gap-3">
                    <button
                      type="button"
                      onClick={() => openSavedProperty(asset)}
                      className="min-w-0 flex-1 text-left"
                    >
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0">
                          <div className="font-medium text-white">{asset.address}</div>
                          <div className="mt-1 text-sm text-zinc-400">
                            {asset.propertyType === "house"
                              ? "Private House"
                              : "Apartment"}
                          </div>
                          <div className="mt-3 inline-flex rounded-full border border-amber-400/25 bg-amber-400/10 px-3 py-1 text-xs font-medium text-amber-300">
                            Market Value: {asset.estimatedPropertyValue}
                          </div>
                        </div>
                        {asset.propertyType === "house" ? (
                          <House className="size-5 shrink-0 text-amber-400" />
                        ) : (
                          <Building className="size-5 shrink-0 text-amber-400" />
                        )}
                      </div>
                    </button>
                    <button
                      type="button"
                      onClick={() => handleDeleteAsset(asset.id)}
                      aria-label={`Delete ${asset.address}`}
                      className="rounded-full border border-red-500/25 bg-red-500/10 p-2 text-red-300 transition-colors hover:bg-red-500/20"
                    >
                      <Trash2 className="size-4" />
                    </button>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <div className="mt-3 rounded-2xl border border-amber-400/15 bg-[#161116] p-5 text-center text-sm text-zinc-400">
              No owned properties yet. Add an asset and tag it as &quot;I own this&quot;.
            </div>
          )}
        </div>

        <div>
          <h3 className="text-2xl font-semibold text-white">Watchlist / Targets</h3>
          {wishlistAssets.length ? (
            <div className="mt-3 space-y-3">
              {wishlistAssets.map((asset) => (
                <div
                  key={asset.id}
                  className="w-full rounded-2xl border border-amber-400/15 bg-[#161116] p-5"
                >
                  <div className="flex items-start justify-between gap-3">
                    <button
                      type="button"
                      onClick={() => openSavedProperty(asset)}
                      className="min-w-0 flex-1 text-left"
                    >
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0">
                          <div className="font-medium text-white">{asset.address}</div>
                          <div className="mt-1 text-sm text-zinc-400">
                            {asset.propertyType === "house"
                              ? "Private House"
                              : "Apartment"}
                          </div>
                          <div className="mt-3 inline-flex rounded-full border border-amber-400/25 bg-amber-400/10 px-3 py-1 text-xs font-medium text-amber-300">
                            Market Value: {asset.estimatedPropertyValue}
                          </div>
                        </div>
                        {asset.propertyType === "house" ? (
                          <House className="size-5 shrink-0 text-amber-400" />
                        ) : (
                          <Building className="size-5 shrink-0 text-amber-400" />
                        )}
                      </div>
                    </button>
                    <button
                      type="button"
                      onClick={() => handleDeleteAsset(asset.id)}
                      aria-label={`Delete ${asset.address}`}
                      className="rounded-full border border-red-500/25 bg-red-500/10 p-2 text-red-300 transition-colors hover:bg-red-500/20"
                    >
                      <Trash2 className="size-4" />
                    </button>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <div className="mt-3 rounded-2xl border border-amber-400/15 bg-[#161116] p-5 text-center text-sm text-zinc-400">
              No target properties yet. Add an asset and tag it as &quot;I want to buy this&quot;.
            </div>
          )}
        </div>
      </div>
    </div>
  );

  const renderProfile = () => (
    <div className="flex h-full min-h-0 flex-1 flex-col overflow-y-auto px-4 py-5 pb-32">
      <div className="space-y-5">
        <div>
          <h2 className="text-4xl font-semibold text-amber-400">Profile</h2>
          <p className="mt-2 text-base text-zinc-400">Your account details</p>
        </div>

        <div className="rounded-3xl border border-amber-400/20 bg-[#161116] p-5">
          <div className="flex items-center gap-2 text-amber-400">
            <User className="size-4" />
            <span className="text-sm font-medium">Personal Information</span>
          </div>

          <div className="mt-4 space-y-3 text-sm">
            <div className="rounded-2xl border border-amber-400/20 bg-black p-4">
              <label className="text-zinc-500">Full Name</label>
              <input
                type="text"
                value={welcomeForm.fullName}
                onChange={(event) =>
                  setWelcomeForm((current) => ({
                    ...current,
                    fullName: event.target.value,
                  }))
                }
                placeholder="Enter your full name"
                className="mt-2 h-11 w-full rounded-xl border border-amber-400/15 bg-[#090909] px-3 text-white outline-none placeholder:text-zinc-500 focus:border-amber-400"
              />
            </div>

            <div className="rounded-2xl border border-amber-400/20 bg-black p-4">
              <label className="flex items-center gap-2 text-zinc-500">
                <Mail className="size-4" />
                <span>Email</span>
              </label>
              <input
                type="email"
                value={welcomeForm.email}
                onChange={(event) =>
                  setWelcomeForm((current) => ({
                    ...current,
                    email: event.target.value,
                  }))
                }
                placeholder="Enter your email"
                className="mt-2 h-11 w-full rounded-xl border border-amber-400/15 bg-[#090909] px-3 text-white outline-none placeholder:text-zinc-500 focus:border-amber-400"
              />
            </div>
          </div>
        </div>

        <div className="rounded-3xl border border-amber-400/20 bg-[#161116] p-5">
          <div className="inline-flex rounded-full border border-amber-400/30 bg-amber-400/10 px-3 py-1 text-xs font-semibold uppercase tracking-[0.2em] text-amber-300">
            Go Pro
          </div>
          <div className="mt-3 rounded-2xl border border-amber-400/15 bg-black/60 p-4">
            <div className="text-base font-semibold text-white">
              Upgrade for Unlimited Property Insights &amp; Real Tax Data
            </div>
            <p className="mt-2 text-sm text-zinc-400">
              Unlock deeper ownership records, valuation history, and cross-market
              intelligence.
            </p>
          </div>
          <div className="mt-5 text-lg font-semibold text-white">Subscription</div>
          <p className="mt-1 text-sm text-zinc-400">Manage your plan</p>
          <div className="mt-4 space-y-3">
            <button
              type="button"
              onClick={() => setShowUpgradeComingSoon(true)}
              className="w-full rounded-xl bg-amber-400 px-5 py-3 text-sm font-medium text-black"
            >
              Upgrade Subscription
            </button>
            <button className="w-full rounded-xl px-5 py-2 text-sm font-medium text-white">
              Cancel Subscription
            </button>
          </div>
          {showUpgradeComingSoon ? (
            <p className="mt-3 text-center text-sm text-amber-300">Coming Soon</p>
          ) : null}
        </div>

        <div className="mt-10 flex justify-start border-t border-white/5 pt-6">
          <button
            type="button"
            onClick={async () => {
              const supabase = createClient();
              await supabase.auth.signOut();
              if (typeof window !== "undefined") {
                window.localStorage.clear();
                window.location.href = "/";
              }
            }}
            className="rounded-xl border border-amber-400/30 px-5 py-2.5 text-sm font-medium text-amber-300 hover:bg-amber-400/10"
          >
            Sign Out
          </button>
        </div>
      </div>
    </div>
  );

  if (showWelcomeScreen) {
    return (
      <div className="flex min-h-0 flex-1 items-center justify-center bg-[#000000] px-6 py-10 text-white">
        <div className="w-full max-w-lg rounded-3xl border border-amber-400/20 bg-[#161116] p-8 shadow-2xl shadow-black/40">
          <div className="text-center">
            <p className="text-4xl font-semibold text-amber-400">StreetIQ</p>
            <p className="mt-2 text-zinc-400">The Intelligence Behind Every Street</p>
            <p className="mt-6 text-base text-zinc-300">
              Create your account to unlock your map, saved places, and portfolio
              workspace.
            </p>
          </div>

          <div className="mt-8 space-y-4">
            <div>
              <label className="text-sm font-medium text-zinc-300">Full Name</label>
              <input
                type="text"
                value={welcomeForm.fullName}
                onChange={(event) => {
                  setSignUpError(null);
                  setWelcomeForm((current) => ({
                    ...current,
                    fullName: event.target.value,
                  }));
                }}
                placeholder="Enter your full name"
                className="mt-2 h-12 w-full rounded-2xl border border-amber-400/20 bg-black/70 px-4 text-white outline-none placeholder:text-zinc-500 focus:border-amber-400"
              />
            </div>

            <div>
              <label className="text-sm font-medium text-zinc-300">Email</label>
              <input
                type="email"
                value={welcomeForm.email}
                onChange={(event) => {
                  setSignUpError(null);
                  setWelcomeForm((current) => ({
                    ...current,
                    email: event.target.value,
                  }));
                }}
                placeholder="Enter your email"
                className="mt-2 h-12 w-full rounded-2xl border border-amber-400/20 bg-black/70 px-4 text-white outline-none placeholder:text-zinc-500 focus:border-amber-400"
              />
            </div>

            <div>
              <label className="text-sm font-medium text-zinc-300">Password</label>
              <div className="relative mt-2">
                <input
                  type={showPassword ? "text" : "password"}
                  value={welcomeForm.password}
                  onChange={(event) => {
                    setSignUpError(null);
                    setWelcomeForm((current) => ({
                      ...current,
                      password: event.target.value,
                    }));
                  }}
                  placeholder="Create a password"
                  className="h-12 w-full rounded-2xl border border-amber-400/20 bg-black/70 px-4 pr-12 text-white outline-none placeholder:text-zinc-500 focus:border-amber-400"
                />
                <button
                  type="button"
                  onClick={() => setShowPassword((current) => !current)}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-zinc-400 transition-colors hover:text-amber-300"
                  aria-label={showPassword ? "Hide password" : "Show password"}
                >
                  {showPassword ? (
                    <EyeOff className="size-5" />
                  ) : (
                    <Eye className="size-5" />
                  )}
                </button>
              </div>
            </div>
          </div>

          {signUpError ? (
            <p className="mt-4 text-sm text-red-400">{signUpError}</p>
          ) : null}
          <button
            type="button"
            onClick={() => void handleGetStarted()}
            disabled={isSigningUp}
            className="mt-8 w-full rounded-xl bg-amber-400 px-5 py-3 text-sm font-medium text-black disabled:opacity-60 disabled:cursor-not-allowed hover:bg-amber-300"
          >
            {isSigningUp ? "Creating account..." : "Get Started"}
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-0 overflow-hidden bg-[#000000] text-white">
      {flashMessage ? (
        <div className="pointer-events-none absolute inset-x-4 top-4 z-50 flex justify-center">
          <div className="rounded-2xl border border-amber-400/30 bg-black/90 px-4 py-3 text-sm font-medium text-amber-200 shadow-lg shadow-black/30 backdrop-blur">
            {flashMessage}
          </div>
        </div>
      ) : null}

      <header className="shrink-0 border-b border-amber-400/15 bg-[#000000] px-4 pb-4 pt-5">
        <div className="flex flex-col items-center text-center">
          <div className="rounded-2xl border border-amber-400/25 bg-[#0b0b0b] p-2">
            <Building2 className="size-5 text-amber-400" />
          </div>
          <div className="mt-3 flex items-center gap-3">
            <h1 className="text-xl font-semibold tracking-wide text-amber-400">StreetIQ</h1>
          </div>
          <div className="mt-1">
            <p className="text-sm text-zinc-400">The Intelligence Behind Every Street</p>
          </div>
        </div>
      </header>

      <div className="min-h-0 flex-1 overflow-hidden">
        {activeSection === "Explore" && renderExplore()}
        {activeSection === "Saved" && renderSaved()}
        {activeSection === "Portfolio" && renderPortfolio()}
        {activeSection === "Profile" && renderProfile()}
      </div>

      <nav className="fixed bottom-0 left-0 right-0 z-50 shrink-0 border-t border-amber-400/20 bg-[#000000] px-3 py-1.5">
        <div className="grid grid-cols-4 gap-2">
          {navItems.map(({ label, icon: Icon }) => {
            const isActive = activeSection === label;

            return (
              <button
                key={label}
                type="button"
                onClick={() => setActiveSection(label)}
                className="flex flex-col items-center justify-center gap-1 rounded-2xl px-3 py-2 transition-colors"
              >
                <Icon className={isActive ? "size-5 text-amber-400" : "size-5 text-zinc-500"} />
                <span className={isActive ? "text-xs text-amber-300" : "text-xs text-zinc-500"}>
                  {label}
                </span>
              </button>
            );
          })}
        </div>
      </nav>

      <Dialog open={isPropertyValueChoiceOpen} onOpenChange={setIsPropertyValueChoiceOpen}>
        <DialogContent className="border-amber-400/20 bg-[#0f0f12] text-white">
          <DialogHeader>
            <DialogTitle className="text-amber-400">Property Value</DialogTitle>
            <DialogDescription className="text-zinc-400">
              Do you want to value your current location?
            </DialogDescription>
          </DialogHeader>
          {!currentLocation ? (
            <p className="text-xs text-zinc-500">
              Enable location access to use this option.
            </p>
          ) : null}
          <DialogFooter className="flex gap-2 sm:gap-0">
            <button
              type="button"
              onClick={handlePropertyValueYes}
              disabled={!currentLocation}
              className="rounded-xl bg-amber-400 px-5 py-2.5 text-sm font-medium text-black disabled:opacity-50 disabled:cursor-not-allowed hover:bg-amber-300"
            >
              Yes
            </button>
            <button
              type="button"
              onClick={handlePropertyValueNo}
              className="rounded-xl border border-amber-400/40 px-5 py-2.5 text-sm font-medium text-amber-300 hover:bg-amber-400/10"
            >
              No
            </button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog
        open={isPropertyValueAddressInputOpen}
        onOpenChange={(open) => {
          setIsPropertyValueAddressInputOpen(open);
          if (!open) {
            setPropertyValueAddressQuery("");
            setPropertyValuePredictions([]);
          }
        }}
      >
        <DialogContent className="border-amber-400/20 bg-[#0f0f12] text-white">
          <DialogHeader>
            <DialogTitle className="text-amber-400">Enter Address</DialogTitle>
            <DialogDescription className="text-zinc-400">
              Type an address to get its property value.
            </DialogDescription>
          </DialogHeader>
          <div className="relative space-y-2">
            <MapPin className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-amber-400" />
            <input
              value={propertyValueAddressQuery}
              onChange={(e) => setPropertyValueAddressQuery(e.target.value)}
              placeholder="Start typing an address..."
              className="h-12 w-full rounded-2xl border border-amber-400/30 bg-black pl-10 pr-4 text-white outline-none placeholder:text-zinc-500 focus:border-amber-400"
              autoFocus
            />
            {propertyValuePredictions.length > 0 ? (
              <div className="rounded-2xl border border-amber-400/20 bg-black/95 p-2 shadow-lg">
                {propertyValuePredictions.map((prediction) => (
                  <button
                    key={prediction.placeId}
                    type="button"
                    onClick={() => handlePropertyValueSelectAddress(prediction)}
                    className="w-full rounded-xl px-3 py-2 text-left text-sm text-white transition-colors hover:bg-white/5"
                  >
                    {prediction.description}
                  </button>
                ))}
              </div>
            ) : null}
          </div>
        </DialogContent>
      </Dialog>

      <Dialog
        open={isAddAssetOpen}
        onOpenChange={(open) => {
          setIsAddAssetOpen(open);
          if (!open) {
            resetAssetFlow();
          }
        }}
      >
        <DialogContent className="border-amber-400/20 bg-[#0f0f12] text-white">
          <DialogHeader>
            <DialogTitle className="text-amber-400">Add Asset</DialogTitle>
            <DialogDescription className="text-zinc-400">
              Add a property to your owned portfolio or wishlist.
            </DialogDescription>
          </DialogHeader>

          {assetStep === 1 ? (
            <div className="space-y-4">
              <div className="text-sm font-medium text-zinc-300">
                Step 1: Search for an address
              </div>
              <div className="relative">
                <MapPin className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-amber-400" />
                <input
                  value={assetAddressQuery}
                  onChange={(event) => setAssetAddressQuery(event.target.value)}
                  placeholder="Start typing an address..."
                  className="h-12 w-full rounded-2xl border border-amber-400/30 bg-black pl-10 pr-4 text-white outline-none placeholder:text-zinc-500"
                />
              </div>

              {assetSelection.address ? (
                <div className="rounded-2xl border border-amber-400/20 bg-black p-3 text-sm text-amber-200">
                  Selected: {assetSelection.address}
                </div>
              ) : null}

              <div className="space-y-2">
                {assetPredictions.map((prediction) => (
                  <button
                    key={prediction.placeId}
                    type="button"
                    onClick={() => handleSelectAssetPrediction(prediction)}
                    className="w-full rounded-2xl border border-amber-400/15 bg-black/70 p-3 text-left text-sm text-white"
                  >
                    {prediction.description}
                  </button>
                ))}
              </div>
            </div>
          ) : null}

          {assetStep === 2 ? (
            <div className="space-y-4">
              <div className="text-sm font-medium text-zinc-300">
                Step 2: Select ownership status
              </div>
              <div className="grid gap-3">
                {[
                  { value: "owned", label: "I own this" },
                  { value: "wishlist", label: "I want to buy this" },
                ].map((option) => (
                  <button
                    key={option.value}
                    type="button"
                    onClick={() =>
                      setAssetOwnership(option.value as AssetOwnership)
                    }
                    className={[
                      "rounded-2xl border p-4 text-left",
                      assetOwnership === option.value
                        ? "border-amber-400 bg-amber-400/10 text-amber-300"
                        : "border-amber-400/20 bg-black/70 text-white",
                    ].join(" ")}
                  >
                    {option.label}
                  </button>
                ))}
              </div>
            </div>
          ) : null}

          {assetStep === 3 ? (
            <div className="space-y-4">
              <div className="text-sm font-medium text-zinc-300">
                Step 3: Select property type
              </div>
              <div className="grid gap-3">
                {[
                  { value: "house", label: "Private House" },
                  { value: "apartment", label: "Apartment" },
                ].map((option) => (
                  <button
                    key={option.value}
                    type="button"
                    onClick={() =>
                      setAssetPropertyType(option.value as AssetPropertyType)
                    }
                    className={[
                      "rounded-2xl border p-4 text-left",
                      assetPropertyType === option.value
                        ? "border-amber-400 bg-amber-400/10 text-amber-300"
                        : "border-amber-400/20 bg-black/70 text-white",
                    ].join(" ")}
                  >
                    {option.label}
                  </button>
                ))}
              </div>
            </div>
          ) : null}

          <DialogFooter>
            {assetStep > 1 ? (
              <button
                type="button"
                onClick={() => setAssetStep((step) => step - 1)}
                className="rounded-xl border border-amber-400/20 px-4 py-2 text-sm text-amber-300"
              >
                Back
              </button>
            ) : null}

            {assetStep < 3 ? (
              <button
                type="button"
                onClick={() => setAssetStep((step) => step + 1)}
                disabled={assetStep === 1 && !assetSelection.position}
                className="rounded-xl bg-amber-400 px-4 py-2 text-sm font-medium text-black disabled:opacity-50"
              >
                Next
              </button>
            ) : (
              <button
                type="button"
                onClick={handleSaveAsset}
                className="rounded-xl bg-amber-400 px-4 py-2 text-sm font-medium text-black"
              >
                Save Asset
              </button>
            )}
          </DialogFooter>
        </DialogContent>
      </Dialog>

    </div>
  );
};
