import { config } from "dotenv";
import { Pool, QueryResult, QueryResultRow } from "pg";
import * as path from "path";

config({ path: path.join(__dirname, "../.env"), quiet: true });

const pool = new Pool({
    user: process.env.POSTGRES_USER,
    password: process.env.POSTGRES_PASSWORD,
    host: process.env.POSTGRES_HOST || "localhost",
    port: parseInt(process.env.POSTGRES_PORT || "5432", 10),
    database: process.env.POSTGRES_DB,
});

export async function query<T extends QueryResultRow = QueryResultRow>(text: string, values: unknown[] = []): Promise<QueryResult<T>> {
    return pool.query<T>(text, values as any[]);
}
