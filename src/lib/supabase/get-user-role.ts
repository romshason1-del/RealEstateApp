"use client";

import { createClient } from "./client";
import type { UserRole } from "./types";

/**
 * Fetches the current user's role from the profiles table.
 * Returns 'basic' if not logged in or profile not found.
 * Use this when you're ready to gate PRO features.
 */
export async function getUserRole(): Promise<UserRole> {
  const supabase = createClient();

  const {
    data: { user },
  } = await supabase.auth.getUser();

  if (!user) {
    return "basic";
  }

  const { data: profile } = await supabase
    .from("profiles")
    .select("role")
    .eq("id", user.id)
    .single();

  return (profile?.role as UserRole) ?? "basic";
}
