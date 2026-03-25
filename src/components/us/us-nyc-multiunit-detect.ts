/**
 * NYC apartment re-query UI must not use address heuristics or string guessing.
 * Enable only when the main property-value payload explicitly opts in.
 */

export function usNycPayloadSupportsApartmentFlow(payload: { supports_apartment_requery?: boolean }): boolean {
  return payload.supports_apartment_requery === true;
}
