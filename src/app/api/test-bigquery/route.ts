import { NextResponse } from "next/server";
import { getBigQueryClient } from "@/lib/bigquery-client";

export async function GET() {
  try {
    const bigquery = getBigQueryClient();

    const query = `
      SELECT COUNT(*) as total
      FROM \`streetiq-bigquery.streetiq_gold.property_latest_facts\`
    `;

    const [rows] = await bigquery.query({ query });

    return NextResponse.json(
      {
        success: true,
        data: rows?.[0] ?? null,
      },
      { status: 200 }
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    return NextResponse.json(
      {
        success: false,
        error: message,
      },
      { status: 500 }
    );
  }
}

