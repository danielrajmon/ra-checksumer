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

type PlatformContext = {
    code: string;
    searchPaths: string[];
};

function extractPlatformCodeFromPath(value: string): string | null {
    const normalized = value.replace(/\\/g, "/").trim();
    const parts = normalized.split("/").filter(Boolean);
    const last = parts[parts.length - 1];
    return last || null;
}

async function loadPlatformContexts(): Promise<Map<number, PlatformContext>> {
    const configPath = path.resolve(__dirname, "../platforms.json");
    const raw = await fs.readFile(configPath, "utf8");
    const parsed = JSON.parse(raw) as ChecksumerConfig;
    const map = new Map<number, PlatformContext>();

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

        const existing = map.get(platformId);
        const mergedSearchPaths = [
            ...(existing?.searchPaths ?? []),
            ...((platform.sourceRomPaths ?? []).map((value) => String(value).trim()).filter(Boolean)),
        ];
        const uniqueSearchPaths = Array.from(new Set(mergedSearchPaths));

        map.set(platformId, {
            code: code ?? existing?.code ?? String(platformId),
            searchPaths: uniqueSearchPaths,
        });
    }

    return map;
}

function parseMd5FromFileName(fileName: string): string | null {
    const baseName = path.basename(fileName, path.extname(fileName));
    const bracketMatch = /\[([0-9a-fA-F]{32})\](?: \(\d+\))?$/.exec(baseName);
    if (bracketMatch) {
        return bracketMatch[1].toLowerCase();
    }

    const inlineMatch = /\b([0-9a-fA-F]{32})\b/.exec(baseName);
    return inlineMatch ? inlineMatch[1].toLowerCase() : null;
}

async function listFilesRecursiveSafe(dirPath: string): Promise<string[]> {
    const entries = await fs.readdir(dirPath, { withFileTypes: true });
    const files: string[] = [];

    for (const entry of entries) {
        const fullPath = path.join(dirPath, entry.name);

        if (entry.isDirectory()) {
            files.push(...(await listFilesRecursiveSafe(fullPath)));
            continue;
        }

        if (entry.isFile()) {
            files.push(fullPath);
        }
    }

    return files;
}

async function buildLocalMd5ByPlatform(platformContexts: Map<number, PlatformContext>): Promise<Map<number, Set<string>>> {
    const byPlatform = new Map<number, Set<string>>();

    for (const [platformId, context] of platformContexts.entries()) {
        const md5Set = new Set<string>();

        for (const searchPath of context.searchPaths) {
            try {
                const files = await listFilesRecursiveSafe(searchPath);
                for (const filePath of files) {
                    const md5 = parseMd5FromFileName(path.basename(filePath));
                    if (md5) {
                        md5Set.add(md5);
                    }
                }
            } catch {
            }
        }

        byPlatform.set(platformId, md5Set);
    }

    return byPlatform;
}

function printCandidates(rows: CandidateRow[], platformContexts: Map<number, PlatformContext>): void {
    const columns = [
        { key: "platformCode", header: "platformCode" },
        { key: "gameTitle", header: "gameTitle" },
        { key: "missingRequiredFileName", header: "missingRequiredFileName" },
        { key: "alternateFileName", header: "alternateFileName" },
    ] as const;

    const toCell = (row: CandidateRow, key: (typeof columns)[number]["key"]): string => {
        if (key === "platformCode") {
            return platformContexts.get(row.platformId)?.code ?? String(row.platformId);
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

async function printPracticalCandidates(platformContexts: Map<number, PlatformContext>, limit = 200): Promise<void> {
    const localMd5ByPlatform = await buildLocalMd5ByPlatform(platformContexts);

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
    `,
    );

    const locallyPresentRows = rows.rows.filter((row) => {
        const md5Set = localMd5ByPlatform.get(row.platformId);
        return md5Set?.has(row.alternateMd5.toLowerCase()) ?? false;
    });

    const limitedRows = locallyPresentRows.slice(0, limit);
    const uniqueGames = new Set(limitedRows.map((row) => `${row.platformId}|${row.gameTitle}`));

    const totals = summary.rows[0];
    console.log("\n[patch-candidates] practical mode (not-owned games, required hash missing, valid non-required sibling exists)");
    console.log(
        `db games: ${totals.totalGames}, db missing required files: ${totals.totalFiles}, local md5-matched rows: ${locallyPresentRows.length}, showing rows: ${limitedRows.length}, showing games: ${uniqueGames.size}`,
    );

    if (limitedRows.length > 0) {
        printCandidates(limitedRows, platformContexts);
    }
}

async function run(): Promise<void> {
    const platformContexts = await loadPlatformContexts();
    await printStrictNotOwnedSummary();
    await printPracticalCandidates(platformContexts);
}

void run().catch((error) => {
    console.error("[patch-candidates] failed", error instanceof Error ? error.message : String(error));
    process.exit(1);
});
