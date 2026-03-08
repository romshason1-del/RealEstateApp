import { createClient } from "@/lib/supabase/server";
import { createAdminClient } from "@/lib/supabase/admin";
import { NextRequest, NextResponse } from "next/server";

export async function POST(request: NextRequest) {
  try {
    const supabase = await createClient();
    const authHeader = request.headers.get("Authorization");
    const accessToken = authHeader?.replace(/^Bearer\s+/i, "");

    const {
      data: { user },
      error: getUserError,
    } = accessToken
      ? await supabase.auth.getUser(accessToken)
      : await supabase.auth.getUser();

    if (getUserError || !user) {
      return NextResponse.json(
        { error: "Not authenticated" },
        { status: 401 },
      );
    }

    const admin = createAdminClient();
    await admin.auth.admin.deleteUser(user.id);

    return NextResponse.json({ success: true });
  } catch (err) {
    console.error("[delete-account] Error:", err);
    return NextResponse.json(
      { error: err instanceof Error ? err.message : "Failed to delete account" },
      { status: 500 },
    );
  }
}
