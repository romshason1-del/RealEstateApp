/**
 * Supabase connection test.
 * POST: Inserts a hardcoded "Test Property" into cached_locations.
 * Use when geocoding fails to verify database connectivity.
 */

import { NextResponse } from "next/server";
import { createAdminClient } from "@/lib/supabase/admin";

const TEST_LOOKUP_KEY = "test_property_diagnostic";
const TEST_ADDRESS = "Test Property (Supabase diagnostic)";
const TEST_LAT = 51.5074;
const TEST_LNG = -0.1278;

export async function POST() {
  try {
    const supabase = createAdminClient();
    const { data, error } = await supabase
      .from("cached_locations")
      .upsert(
        {
          lookup_key: TEST_LOOKUP_KEY,
          lookup_type: "address",
          formatted_address: TEST_ADDRESS,
          lat: TEST_LAT,
          lng: TEST_LNG,
          place_id: null,
          address_components: null,
        },
        { onConflict: "lookup_key" }
      )
      .select("id, lookup_key, formatted_address, lat, lng, created_at")
      .single();

    if (error) {
      console.error("[supabase-test] Insert error:", error);
      return NextResponse.json(
        { success: false, error: error.message, code: error.code },
        { status: 500 }
      );
    }

    console.log("[supabase-test] Success:", data);
    return NextResponse.json({
      success: true,
      message: "Test record saved to cached_locations",
      record: data,
    });
  } catch (err) {
    console.error("[supabase-test] Error:", err);
    return NextResponse.json(
      {
        success: false,
        error: err instanceof Error ? err.message : "Supabase test failed",
      },
      { status: 500 }
    );
  }
}
