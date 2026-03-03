import { query } from "./db-client";

type FileMatchRow = {
  platformId: number;
  gameId: number;
  gameTitle: string;
  fileName: string | null;
  hasMultipleRequiredFiles: boolean;
  md5: string;
  isOwned: boolean;
  isRequired: boolean | null;
};

export type FileMatch = {
  platformId: number;
  gameId: number;
  gameTitle: string;
  fileName: string | null;
  hasMultipleRequiredFiles: boolean;
  md5: string;
  isOwned: boolean;
  isRequired: boolean | null;
};

export async function findFileMatchByChecksum(platformId: number, md5: string): Promise<FileMatch | null> {
  const normalizedMd5 = md5.toLowerCase();

  const result = await query<FileMatchRow>(
    `
      SELECT
        f.platform_id AS "platformId",
        f.game_id AS "gameId",
        g.title AS "gameTitle",
        f.name AS "fileName",
        (
          SELECT COUNT(*) > 1
          FROM files f2
          WHERE f2.platform_id = f.platform_id
            AND f2.game_id = f.game_id
            AND f2.is_required = TRUE
        ) AS "hasMultipleRequiredFiles",
        f.md5 AS md5,
        f.is_owned AS "isOwned",
        f.is_required AS "isRequired"
      FROM files f
      INNER JOIN games g
        ON g.platform_id = f.platform_id
        AND g.id = f.game_id
      WHERE f.platform_id = $1
        AND f.md5 = $2
      LIMIT 1
    `,
    [platformId, normalizedMd5],
  );

  if (result.rows.length === 0) {
    return null;
  }

  return result.rows[0];
}

export async function markFileOwned(
  platformId: number,
  gameId: number,
  md5: string
): Promise<void> {
  const normalizedMd5 = md5.toLowerCase();

  await query(
    `
      UPDATE files
      SET
        is_owned = TRUE
      WHERE platform_id = $1
        AND game_id = $2
        AND md5 = $3
    `,
    [platformId, gameId, normalizedMd5],
  );

  await query(
    `
      UPDATE games g
      SET is_owned = EXISTS (
        SELECT 1
        FROM files f
        WHERE f.platform_id = g.platform_id
          AND f.game_id = g.id
          AND f.is_owned = TRUE
      )
      WHERE g.platform_id = $1
        AND g.id = $2
    `,
    [platformId, gameId],
  );
}
