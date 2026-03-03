import * as fs from "fs/promises";
import * as path from "path";

const logsDir = path.resolve(__dirname, "../");

async function ensureLogsDir(): Promise<void> {
    await fs.mkdir(logsDir, { recursive: true });
}

async function appendLog(fileName: string, line: string): Promise<void> {
    await ensureLogsDir();
    const logPath = path.join(logsDir, fileName);
    await fs.appendFile(logPath, `${line}\n`, "utf8");
}

export async function logMoved(entry: string): Promise<void> {
    await appendLog("checksumer-moved.log", entry);
}

export async function logUnmatched(entry: string): Promise<void> {
    await appendLog("checksumer-unmatched.log", entry);
}

export async function logDuplicateDeleted(entry: string): Promise<void> {
    await appendLog("checksumer-deleted.log", entry);
}
