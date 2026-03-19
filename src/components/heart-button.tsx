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
  const [optimisticSaved, setOptimisticSaved] = React.useState(isSaved);
  const expectedRef = React.useRef<boolean | null>(null);

  React.useEffect(() => {
    // If the user just clicked, keep the optimistic UI until the parent
    // reports the expected value. This prevents brief prop sync from
    // canceling the immediate visual update.
    if (expectedRef.current != null) {
      if (isSaved === expectedRef.current) {
        setOptimisticSaved(isSaved);
        expectedRef.current = null;
      }
      return;
    }
    setOptimisticSaved(isSaved);
  }, [isSaved]);

  const displaySaved = optimisticSaved;

  const handleClick = React.useCallback(
    (e: React.MouseEvent) => {
      e.preventDefault();
      e.stopPropagation();
      const next = !displaySaved;
      setOptimisticSaved(next);
      expectedRef.current = next;
      onToggle();
    },
    [displaySaved, onToggle]
  );

  return (
    <button
      type="button"
      onClick={handleClick}
      className={`heart-save-btn rounded-full border border-amber-400/30 p-1 focus:outline-none focus:ring-0 focus:shadow-none hover:border-amber-400/50 ${className}`}
      aria-label={ariaLabel ?? (displaySaved ? "Remove from favorites" : "Add to favorites")}
    >
      <Heart
        className={`heart-save-icon heart-save-icon-instant ${iconSize} ${displaySaved ? "heart-saved" : "heart-unsaved"}`}
      />
    </button>
  );
}
