"use client";

import * as React from "react";
import { Heart } from "lucide-react";

export type HeartButtonProps = {
  /** Whether the item is saved (heart is solid gold) */
  isSaved: boolean;
  /** Called on click. Parent should add/remove from favorites and show "Added to favorites" only when adding. */
  onToggle: () => void;
  /** Optional aria-label for accessibility */
  ariaLabel?: string;
  /** Optional className for the button wrapper */
  className?: string;
  /** Optional size for the icon (default: size-3.5) */
  iconSize?: string;
};

export function HeartButton({
  isSaved,
  onToggle,
  ariaLabel,
  className = "",
  iconSize = "size-3.5",
}: HeartButtonProps) {
  const handleClick = React.useCallback(() => {
    onToggle();
  }, [onToggle]);

  return (
    <button
      type="button"
      onClick={handleClick}
      className={`heart-save-btn rounded-full border border-amber-400/30 p-1 focus:outline-none focus:ring-0 focus:shadow-none hover:border-amber-400/50 ${className}`}
      aria-label={ariaLabel ?? (isSaved ? "Remove from favorites" : "Add to favorites")}
    >
      <Heart
        className={`heart-save-icon ${iconSize} ${isSaved ? "heart-saved" : "heart-unsaved"}`}
      />
    </button>
  );
}
