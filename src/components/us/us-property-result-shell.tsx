"use client";

import type { USLastTransaction, USPropertyValueResponse } from "@/lib/us/us-property-response-contract";

export type USPropertyResultShellProps = {
  result: USPropertyValueResponse | null;
};

function formatUSLastTransaction(t: USLastTransaction | null | undefined): string {
  if (t == null) return "—";
  const parts = [t.amount != null ? String(t.amount) : null, t.date].filter((x): x is string => x != null && x !== "");
  return parts.length > 0 ? parts.join(" · ") : "—";
}

/**
 * Isolated US result surface — placeholder layout for StreetIQ-style blocks.
 * Wire to /api/us/property-value when integrating data.
 */
export function USPropertyResultShell({ result }: USPropertyResultShellProps) {
  const r = result ?? null;

  return (
    <section aria-label="United States property valuation" className="space-y-3 text-sm">
      <div>
        <div className="font-semibold">Estimated True Value</div>
        <div>{r?.estimated_value != null ? String(r.estimated_value) : "—"}</div>
      </div>
      <div>
        <div className="font-semibold">Last official sale</div>
        <div>{formatUSLastTransaction(r?.last_transaction ?? null)}</div>
      </div>
      <div>
        <div className="font-semibold">Street average</div>
        <div>{r?.street_average != null ? String(r.street_average) : "—"}</div>
      </div>
      <div>
        <div className="font-semibold">Area demand</div>
        <div>{r?.area_demand ?? "—"}</div>
      </div>
      <div className="text-xs opacity-70">
        <span>display_context: {r?.display_context ?? "—"}</span>
        {" · "}
        <span>confidence: {r?.confidence ?? "—"}</span>
        {" · "}
        <span>source: {r?.source_label ?? "—"}</span>
      </div>
    </section>
  );
}
