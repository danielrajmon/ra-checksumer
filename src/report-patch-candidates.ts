import * as fs from "fs/promises";
import * as path from "path";
import { query } from "./db-client";

type SummaryRow = {
    totalGames: string;
    totalFiles: string;
};

type CandidateRow = {
    platformId: number;
    gameTitle: string;
    missingRequiredFileName: string | null;
    alternateMd5: string;
    alternateFileName: string | null;
    alternateOwned: boolean;
};

type PlatformConfig = {
    id?: number | string;
    destinationRomPath?: string;
    unknownRomPath?: string;
    sourceRomPaths?: string[];
};

type ChecksumerConfig = {
    platforms?: PlatformConfig[];
};

function extractPlatformCodeFromPath(value: string): string | null {
    const normalized = value.replace(/\\/g, "/").trim();
    const parts = normalized.split("/").filter(Boolean);
    const last = parts[parts.length - 1];
    return last || null;
}

async function loadPlatformCodeMap(): Promise<Map<number, string>> {
    const configPath = path.resolve(__dirname, "../platforms.json");
    const raw = await fs.readFile(configPath, "utf8");
    const parsed = JSON.parse(raw) as ChecksumerConfig;
    const map = new Map<number, string>();

    for (const platform of parsed.platforms ?? []) {
        const platformId = Number.parseInt(String(platform.id), 10);
        if (Number.isNaN(platformId)) {
            continue;
        }

        const code =
            (platform.destinationRomPath ? extractPlatformCodeFromPath(platform.destinationRomPath) : null) ??
            (platform.unknownRomPath ? extractPlatformCodeFromPath(platform.unknownRomPath) : null) ??
            (Array.isArray(platform.sourceRomPaths) && platform.sourceRomPaths.length > 0
                ? extractPlatformCodeFromPath(String(platform.sourceRomPaths[0]))
                : null);

        if (code && !map.has(platformId)) {
            map.set(platformId, code);
        }
    }

    return map;
}

function printCandidates(rows: CandidateRow[], platformCodeById: Map<number, string>): void {
    const columns = [
        { key: "platformCode", header: "platformCode" },
        { key: "gameTitle", header: "gameTitle" },
        { key: "missingRequiredFileName", header: "missingRequiredFileName" },
        { key: "alternateFileName", header: "alternateFileName" },
    ] as const;

    const toCell = (row: CandidateRow, key: (typeof columns)[number]["key"]): string => {
        if (key === "platformCode") {
            return platformCodeById.get(row.platformId) ?? String(row.platformId);
        }

        return String((row as unknown as Record<string, string | null>)[key] ?? "");
    };

    const maxColumnWidths = columns.map((column) => {
        let width = column.header.length;

        for (const row of rows) {
            width = Math.max(width, toCell(row, column.key).length);
        }

        return Math.min(width, 60);
    });

    const fit = (value: string, width: number): string => {
        if (value.length <= width) {
            return value.padEnd(width, " ");
        }

        if (width <= 1) {
            return value.slice(0, width);
        }

        return `${value.slice(0, width - 1)}~`;
    };

    const border = `+${maxColumnWidths.map((width) => "-".repeat(width + 2)).join("+")}+`;
    const header = `| ${columns.map((column, index) => fit(column.header, maxColumnWidths[index])).join(" | ")} |`;

    console.log(border);
    console.log(header);
    console.log(border);

    for (const row of rows) {
        const line = `| ${columns
            .map((column, index) => fit(toCell(row, column.key), maxColumnWidths[index]))
            .join(" | ")} |`;
        console.log(line);
    }

    console.log(border);
}

async function printStrictNotOwnedSummary(): Promise<void> {
    const result = await query<SummaryRow>(
        `
      SELECT
        COUNT(DISTINCT g.platform_id::text || ':' || g.id::text) AS "totalGames",
        COUNT(*) AS "totalFiles"
      FROM games g
      INNER JOIN files rf
        ON rf.platform_id = g.platform_id
       AND rf.game_id = g.id
      WHERE COALESCE(g.is_owned, FALSE) = FALSE
        AND COALESCE(rf.is_required, FALSE) = TRUE
        AND COALESCE(rf.is_owned, FALSE) = FALSE
        AND EXISTS (
          SELECT 1
          FROM files af
          WHERE af.platform_id = g.platform_id
            AND af.game_id = g.id
            AND af.md5 <> rf.md5
            AND COALESCE(af.is_required, FALSE) = FALSE
        )
    `,
    );

    const row = result.rows[0];
    console.log("\n[patch-candidates] strict mode (game not owned)");
    console.log(`games: ${row.totalGames}, missing files: ${row.totalFiles}`);
}

async function printPracticalCandidates(platformCodeById: Map<number, string>, limit = 200): Promise<void> {
    const summary = await query<SummaryRow>(
        `
      SELECT
        COUNT(DISTINCT g.platform_id::text || ':' || g.id::text) AS "totalGames",
        COUNT(*) AS "totalFiles"
      FROM games g
      INNER JOIN files rf
        ON rf.platform_id = g.platform_id
       AND rf.game_id = g.id
      WHERE COALESCE(rf.is_required, FALSE) = TRUE
        AND COALESCE(rf.is_owned, FALSE) = FALSE
        AND COALESCE(g.is_owned, FALSE) = FALSE
        AND EXISTS (
          SELECT 1
          FROM files af
          WHERE af.platform_id = g.platform_id
            AND af.game_id = g.id
            AND af.md5 <> rf.md5
            AND COALESCE(af.is_required, FALSE) = FALSE
        )
    `,
    );

    const rows = await query<CandidateRow>(
        `
      SELECT
        g.platform_id AS "platformId",
        g.title AS "gameTitle",
        rf.name AS "missingRequiredFileName",
        af.md5 AS "alternateMd5",
        af.name AS "alternateFileName",
        COALESCE(af.is_owned, FALSE) AS "alternateOwned"
      FROM games g
      INNER JOIN files rf
        ON rf.platform_id = g.platform_id
       AND rf.game_id = g.id
      INNER JOIN LATERAL (
        SELECT
          af1.md5,
          af1.name,
          af1.is_owned
        FROM files af1
        WHERE af1.platform_id = g.platform_id
          AND af1.game_id = g.id
          AND af1.md5 <> rf.md5
          AND COALESCE(af1.is_required, FALSE) = FALSE
        ORDER BY COALESCE(af1.is_owned, FALSE) DESC, af1.name ASC
        LIMIT 1
      ) af ON TRUE
      WHERE COALESCE(rf.is_required, FALSE) = TRUE
        AND COALESCE(rf.is_owned, FALSE) = FALSE
        AND COALESCE(g.is_owned, FALSE) = FALSE
      ORDER BY g.platform_id, g.title, rf.name
      LIMIT $1
    `,
        [limit],
    );

    const totals = summary.rows[0];
    console.log("\n[patch-candidates] practical mode (not-owned games, required hash missing, non-required sibling exists)");
    console.log(`games: ${totals.totalGames}, missing required files: ${totals.totalFiles}, showing rows: ${rows.rowCount}`);

    if (rows.rowCount > 0) {
        printCandidates(rows.rows, platformCodeById);
    }
}

async function run(): Promise<void> {
    const platformCodeById = await loadPlatformCodeMap();
    await printStrictNotOwnedSummary();
    await printPracticalCandidates(platformCodeById);
}

void run().catch((error) => {
    console.error("[patch-candidates] failed", error instanceof Error ? error.message : String(error));
    process.exit(1);
});
