import { spawn } from "child_process";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";
import { findFileMatchByChecksum, findFileMatchByFileName, markFileOwned } from "./db";
import { logDuplicateDeleted, logMoved, logUnmatched } from "./logger";

type PlatformConfig = {
    id?: number;
    name?: string;
    sourceRomPaths?: string[];
    destinationRomPath?: string;
    unknownRomPath?: string;
    shouldArchive?: boolean;
    matchByFileNameOnly?: boolean;
};

type ChecksumerConfig = {
    processOnlyPlatformId?: number | null;
    platforms?: PlatformConfig[];
};

type ParsedConfig = {
    processOnlyPlatformId: number | null;
    platforms: PlatformConfig[];
};

type ProcessSummary = {
    platformsProcessed: number;
    filesScanned: number;
    filesMatched: number;
    filesArchived: number;
    filesDeletedAsDuplicate: number;
    filesSkipped: number;
};

type SourceFileResult = {
    matched: boolean;
    archived: boolean;
    duplicateDeleted: boolean;
    skipped: boolean;
    status: "unmatched" | "already-present" | "unknown-duplicate-deleted" | "done";
};

function colorizeStatus(status: SourceFileResult["status"]): string {
    const label = status.toUpperCase();
    const reset = "\x1b[0m";

    switch (status) {
        case "done":
            return `\x1b[32m${label}${reset}`;
        case "unmatched":
            return `\x1b[33m${label}${reset}`;
        case "already-present":
        case "unknown-duplicate-deleted":
            return `\x1b[31m${label}${reset}`;
        default:
            return label;
    }
}

function sanitizeFileNamePart(value: string): string {
    const sanitized = value
        .replace(/[<>:"/\\|?*\x00-\x1F]/g, " ")
        .replace(/[. ]+$/g, "")
        .replace(/\s+/g, " ")
        .trim();

    return sanitized || "Unknown";
}

type ParsedChecksumFileName = {
    baseName: string;
    checksum: string;
    copyIndex: number | null;
};

function parseChecksumTaggedBaseName(baseName: string): ParsedChecksumFileName | null {
    const match = /^(.*) \[([0-9a-fA-F]{32})\](?: \((\d+)\))?$/.exec(baseName);
    if (!match) {
        return null;
    }

    return {
        baseName: match[1],
        checksum: match[2].toLowerCase(),
        copyIndex: match[3] ? Number.parseInt(match[3], 10) : null,
    };
}

function parseChecksumFromFileName(fileName: string): string | null {
    const ext = path.extname(fileName);
    const baseName = path.basename(fileName, ext);
    const parsed = parseChecksumTaggedBaseName(baseName);
    return parsed?.checksum ?? null;
}

function resolveRAHasherPath(): string {
    return path.resolve(__dirname, "./utils/RAHasher.exe");
}

async function resolveSevenZipPath(): Promise<string> {
    const candidatePaths = [
        process.env.SEVEN_ZIP_PATH,
        path.resolve(__dirname, "./utils/7z.exe"),
        "C:/Program Files/7-Zip/7z.exe",
        "C:/Program Files (x86)/7-Zip/7z.exe",
    ].filter((value): value is string => Boolean(value));

    for (const candidate of candidatePaths) {
        try {
            await fs.access(candidate);
            return candidate;
        } catch {
        }
    }

    throw new Error("Could not find 7z executable. Set SEVEN_ZIP_PATH or install 7-Zip.");
}

function runProcess(executable: string, args: string[]): Promise<{ stdout: string; stderr: string; exitCode: number }> {
    return new Promise((resolve, reject) => {
        const child = spawn(executable, args, { shell: false, windowsHide: true });

        let stdout = "";
        let stderr = "";

        child.stdout.on("data", (chunk: Buffer) => {
            stdout += chunk.toString();
        });

        child.stderr.on("data", (chunk: Buffer) => {
            stderr += chunk.toString();
        });

        child.on("error", (error) => {
            reject(error);
        });

        child.on("close", (exitCode) => {
            resolve({ stdout, stderr, exitCode: exitCode ?? 1 });
        });
    });
}

async function hashFileWithRAHasher(platformId: number, filePath: string): Promise<string | null> {
    const hasherPath = resolveRAHasherPath();
    const result = await runProcess(hasherPath, [String(platformId), filePath]);

    const checksumMatch = /\b([0-9a-fA-F]{32})\b/.exec(result.stdout);
    if (result.exitCode !== 0 || !checksumMatch) {
        return null;
    }

    return checksumMatch[1].toLowerCase();
}

async function listFilesRecursive(dirPath: string): Promise<string[]> {
    const entries = await fs.readdir(dirPath, { withFileTypes: true });
    const files: string[] = [];

    for (const entry of entries) {
        const fullPath = path.join(dirPath, entry.name);

        if (entry.isDirectory()) {
            files.push(...(await listFilesRecursive(fullPath)));
            continue;
        }

        if (entry.isFile()) {
            files.push(fullPath);
        }
    }

    return files;
}

async function buildArchiveInTemp(
    sourceFilePath: string,
    targetBaseName: string,
    sevenZipPath: string,
): Promise<{ archivePath: string; archiveFileName: string; tempRoot: string }> {
    const tempRoot = await fs.mkdtemp(path.join(os.tmpdir(), "ra-checksumer-"));
    const safeBaseName = sanitizeFileNamePart(targetBaseName);

    let extension = path.extname(sourceFilePath);
    let sourcePathForRename = sourceFilePath;

    if (extension.toLowerCase() === ".zip") {
        const extractDir = path.join(tempRoot, "unzipped");
        await fs.mkdir(extractDir, { recursive: true });

        const extractResult = await runProcess(sevenZipPath, [
            "x",
            "-y",
            `-o${extractDir}`,
            sourceFilePath,
        ]);

        if (extractResult.exitCode !== 0) {
            throw new Error(`7z unzip failed: ${extractResult.stderr || extractResult.stdout}`);
        }

        const extractedFiles = await listFilesRecursive(extractDir);
        if (extractedFiles.length !== 1) {
            throw new Error(`ZIP must contain exactly one file: ${sourceFilePath}`);
        }

        sourcePathForRename = extractedFiles[0];
        extension = path.extname(sourcePathForRename) || ".bin";
    }

    const normalizedExtension = extension || ".bin";
    const stagedFilePath = path.join(tempRoot, `${safeBaseName}${normalizedExtension}`);
    await fs.copyFile(sourcePathForRename, stagedFilePath);

    const archiveFileName = `${safeBaseName}.7z`;
    const archivePath = path.join(tempRoot, archiveFileName);

    const result = await runProcess(sevenZipPath, [
        "a",
        "-t7z",
        "-mx=9",
        "-m0=lzma2",
        "-mmt=on",
        "-y",
        archivePath,
        stagedFilePath,
    ]);

    if (result.exitCode !== 0) {
        throw new Error(`7z failed: ${result.stderr || result.stdout}`);
    }

    return { archivePath, archiveFileName, tempRoot };
}

async function fileExists(filePath: string): Promise<boolean> {
    try {
        await fs.access(filePath);
        return true;
    } catch {
        return false;
    }
}

async function moveFile(sourceFilePath: string, destinationFilePath: string): Promise<void> {
    if (path.resolve(sourceFilePath) === path.resolve(destinationFilePath)) {
        return;
    }

    try {
        await fs.rename(sourceFilePath, destinationFilePath);
    } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "EXDEV") {
            throw error;
        }

        await fs.copyFile(sourceFilePath, destinationFilePath);
        await fs.unlink(sourceFilePath);
    }
}

async function buildNonConflictingPath(destinationDir: string, originalFileName: string, checksum: string): Promise<string> {
    const ext = path.extname(originalFileName);
    const originalBaseName = path.basename(originalFileName, ext);
    const parsedBaseName = parseChecksumTaggedBaseName(originalBaseName);
    const taggedBaseName = parsedBaseName
        ? `${parsedBaseName.baseName} [${parsedBaseName.checksum}]`
        : `${originalBaseName} [${checksum}]`;
    const startingIndex = parsedBaseName?.copyIndex ? parsedBaseName.copyIndex + 1 : 2;

    const firstCandidate = path.join(destinationDir, originalFileName);
    if (!(await fileExists(firstCandidate))) {
        return firstCandidate;
    }

    const md5Candidate = path.join(destinationDir, `${taggedBaseName}${ext}`);
    if (md5Candidate !== firstCandidate && !(await fileExists(md5Candidate))) {
        return md5Candidate;
    }

    let index = startingIndex;
    while (true) {
        const candidate = path.join(destinationDir, `${taggedBaseName} (${index})${ext}`);
        if (!(await fileExists(candidate))) {
            return candidate;
        }

        index += 1;
    }
}

async function buildNonConflictingPathWithoutChecksum(destinationDir: string, originalFileName: string): Promise<string> {
    const ext = path.extname(originalFileName);
    const baseName = path.basename(originalFileName, ext);
    const firstCandidate = path.join(destinationDir, originalFileName);

    if (!(await fileExists(firstCandidate))) {
        return firstCandidate;
    }

    let index = 2;
    while (true) {
        const candidate = path.join(destinationDir, `${baseName} (${index})${ext}`);
        if (!(await fileExists(candidate))) {
            return candidate;
        }

        index += 1;
    }
}

function resolveFileStem(fileName: string): string {
    return path.basename(fileName, path.extname(fileName)).trim().toLowerCase();
}

async function moveFileToUnknownPath(
    platformId: number,
    sourceFilePath: string,
    unknownRomPath: string,
    checksum: string,
    reason: string,
): Promise<void> {
    await fs.mkdir(unknownRomPath, { recursive: true });
    const existingChecksum = parseChecksumFromFileName(path.basename(sourceFilePath));
    const sourceDirectory = path.resolve(path.dirname(sourceFilePath));
    const unknownDirectory = path.resolve(unknownRomPath);
    const shouldKeepSourcePath = sourceDirectory === unknownDirectory && existingChecksum === checksum;
    const destinationUnknownPath = shouldKeepSourcePath
        ? sourceFilePath
        : await buildNonConflictingPath(unknownRomPath, path.basename(sourceFilePath), checksum);

    if (destinationUnknownPath !== sourceFilePath) {
        await moveFile(sourceFilePath, destinationUnknownPath);
    }

    await logMoved(
        `platform=${platformId} md5=${checksum} reason=${reason} source="${sourceFilePath}" destination="${destinationUnknownPath}"`,
    );
}

async function resolveChecksum(platformId: number, sourceFilePath: string): Promise<string | null> {
    const checksumFromName = parseChecksumFromFileName(path.basename(sourceFilePath));
    if (checksumFromName) {
        return checksumFromName;
    }

    return hashFileWithRAHasher(platformId, sourceFilePath);
}

async function processSourceFile(
    platformId: number,
    sourceFilePath: string,
    destinationRomPath: string,
    unknownRomPath: string,
    sevenZipPath: string | null,
    shouldArchive: boolean,
    matchByFileNameOnly: boolean,
    seenUnknownMd5ByPlatform: Map<number, Set<string>>,
): Promise<SourceFileResult> {
    if (matchByFileNameOnly) {
        const originalFileName = path.basename(sourceFilePath);
        const fileStem = resolveFileStem(originalFileName);
        const matchedFile = await findFileMatchByFileName(platformId, fileStem);

        if (!matchedFile) {
            await fs.mkdir(unknownRomPath, { recursive: true });
            const destinationUnknownPath = await buildNonConflictingPathWithoutChecksum(unknownRomPath, originalFileName);
            await moveFile(sourceFilePath, destinationUnknownPath);
            await logUnmatched(
                `platform=${platformId} file="${sourceFilePath}" normalized="${fileStem}" reason=filename-not-found destination="${destinationUnknownPath}"`,
            );
            return { matched: false, archived: false, duplicateDeleted: false, skipped: false, status: "unmatched" };
        }

        if (matchedFile.isOwned) {
            await fs.unlink(sourceFilePath);
            await logDuplicateDeleted(
                `platform=${platformId} file="${sourceFilePath}" normalized="${fileStem}" reason=already-owned-by-filename`,
            );
            return { matched: true, archived: false, duplicateDeleted: true, skipped: false, status: "already-present" };
        }

        if (matchedFile.isRequired === null) {
            await fs.mkdir(unknownRomPath, { recursive: true });
            const destinationUnknownPath = await buildNonConflictingPathWithoutChecksum(unknownRomPath, originalFileName);
            await moveFile(sourceFilePath, destinationUnknownPath);
            await logMoved(
                `platform=${platformId} file="${sourceFilePath}" normalized="${fileStem}" reason=db-known-is-required-null destination="${destinationUnknownPath}"`,
            );
            return { matched: true, archived: false, duplicateDeleted: false, skipped: false, status: "unmatched" };
        }

        if (matchedFile.isRequired === false) {
            await fs.mkdir(unknownRomPath, { recursive: true });
            const destinationUnknownPath = await buildNonConflictingPathWithoutChecksum(unknownRomPath, originalFileName);
            await moveFile(sourceFilePath, destinationUnknownPath);
            await logMoved(
                `platform=${platformId} file="${sourceFilePath}" normalized="${fileStem}" reason=db-known-not-required destination="${destinationUnknownPath}"`,
            );
            return { matched: true, archived: false, duplicateDeleted: false, skipped: false, status: "unmatched" };
        }

        await fs.mkdir(destinationRomPath, { recursive: true });
        const destinationPath = path.join(destinationRomPath, originalFileName);

        if (await fileExists(destinationPath)) {
            throw new Error(`Destination file already exists: ${destinationPath}`);
        }

        await moveFile(sourceFilePath, destinationPath);
        await markFileOwned(
            matchedFile.platformId,
            matchedFile.gameId,
            matchedFile.md5,
        );
        await logMoved(
            `platform=${platformId} file="${sourceFilePath}" normalized="${fileStem}" destination="${destinationPath}" archive=false mode=filename-only`,
        );
        return { matched: true, archived: false, duplicateDeleted: false, skipped: false, status: "done" };
    }

    const checksum = await resolveChecksum(platformId, sourceFilePath);

    if (!checksum) {
        await logUnmatched(`platform=${platformId} file="${sourceFilePath}" hash=<none>`);
        return { matched: false, archived: false, duplicateDeleted: false, skipped: true, status: "unmatched" };
    }

    const matchedFile = await findFileMatchByChecksum(platformId, checksum);
    if (!matchedFile) {
        let seenSet = seenUnknownMd5ByPlatform.get(platformId);
        if (!seenSet) {
            seenSet = new Set<string>();
            seenUnknownMd5ByPlatform.set(platformId, seenSet);
        }

        if (seenSet.has(checksum)) {
            await fs.unlink(sourceFilePath);
            await logDuplicateDeleted(`platform=${platformId} md5=${checksum} reason=unknown-md5-duplicate file="${sourceFilePath}"`);
            return {
                matched: false,
                archived: false,
                duplicateDeleted: true,
                skipped: false,
                status: "unknown-duplicate-deleted",
            };
        }

        seenSet.add(checksum);
        await moveFileToUnknownPath(platformId, sourceFilePath, unknownRomPath, checksum, "unknown-first-kept");
        return { matched: false, archived: false, duplicateDeleted: false, skipped: false, status: "unmatched" };
    }

    if (matchedFile.isOwned) {
        await fs.unlink(sourceFilePath);
        await logDuplicateDeleted(`platform=${platformId} md5=${checksum} reason=already-owned file="${sourceFilePath}"`);
        return { matched: true, archived: false, duplicateDeleted: true, skipped: false, status: "already-present" };
    }

    if (matchedFile.isRequired === null) {
        await moveFileToUnknownPath(platformId, sourceFilePath, unknownRomPath, checksum, "db-known-is-required-null");
        return { matched: true, archived: false, duplicateDeleted: false, skipped: false, status: "unmatched" };
    }

    if (matchedFile.isRequired === false) {
        await moveFileToUnknownPath(platformId, sourceFilePath, unknownRomPath, checksum, "db-known-not-required");
        return { matched: true, archived: false, duplicateDeleted: false, skipped: false, status: "unmatched" };
    }

    const preferredBaseName = matchedFile.hasMultipleRequiredFiles && matchedFile.fileName
        ? matchedFile.fileName
        : matchedFile.gameTitle;
    const safeTargetBaseName = sanitizeFileNamePart(preferredBaseName);
    const sourceExtension = path.extname(sourceFilePath) || ".bin";
    const targetFileName = `${safeTargetBaseName}${sourceExtension}`;

    if (!shouldArchive) {
        await fs.mkdir(destinationRomPath, { recursive: true });
        const destinationPath = await buildNonConflictingPath(destinationRomPath, targetFileName, checksum);
        await moveFile(sourceFilePath, destinationPath);

        await markFileOwned(
            matchedFile.platformId,
            matchedFile.gameId,
            matchedFile.md5,
        );

        await logMoved(
            `platform=${platformId} md5=${checksum} original="${path.basename(sourceFilePath)}" new="${path.basename(destinationPath)}" destination="${destinationPath}" archive=false`,
        );
        return { matched: true, archived: false, duplicateDeleted: false, skipped: false, status: "done" };
    }

    if (!sevenZipPath) {
        throw new Error(`Archiving is enabled but 7z is not configured for platform ${platformId}.`);
    }

    const { archivePath, archiveFileName, tempRoot } = await buildArchiveInTemp(sourceFilePath, safeTargetBaseName, sevenZipPath);

    try {
        await fs.mkdir(destinationRomPath, { recursive: true });
        const destinationArchivePath = path.join(destinationRomPath, archiveFileName);

        if (await fileExists(destinationArchivePath)) {
            throw new Error(`Destination archive already exists: ${destinationArchivePath}`);
        }

        await moveFile(archivePath, destinationArchivePath);
        await fs.unlink(sourceFilePath);

        await markFileOwned(
            matchedFile.platformId,
            matchedFile.gameId,
            matchedFile.md5,
        );

        await logMoved(
            `platform=${platformId} md5=${checksum} original="${path.basename(sourceFilePath)}" new="${archiveFileName}" destination="${destinationArchivePath}" archive=true`,
        );
        return { matched: true, archived: true, duplicateDeleted: false, skipped: false, status: "done" };
    } finally {
        try {
            await fs.rm(tempRoot, { recursive: true, force: true });
        } catch {
        }
    }
}

function parsePlatformsConfig(rawContent: string): ParsedConfig {
    const parsed = JSON.parse(rawContent) as ChecksumerConfig;
    const processOnlyPlatformId = parsed.processOnlyPlatformId == null
        ? null
        : Number.parseInt(String(parsed.processOnlyPlatformId), 10);

    return {
        processOnlyPlatformId: Number.isNaN(processOnlyPlatformId) ? null : processOnlyPlatformId,
        platforms: Array.isArray(parsed.platforms) ? parsed.platforms : [],
    };
}

export async function runChecksumerService(): Promise<ProcessSummary> {
    const configPath = path.resolve(__dirname, "../platforms.json");
    const rawConfig = await fs.readFile(configPath, "utf8");
    const parsedConfig = parsePlatformsConfig(rawConfig);
    const platformConfigs = parsedConfig.processOnlyPlatformId == null
        ? parsedConfig.platforms
        : parsedConfig.platforms.filter((platform) => Number.parseInt(String(platform.id), 10) === parsedConfig.processOnlyPlatformId);

    if (parsedConfig.processOnlyPlatformId != null && platformConfigs.length === 0) {
        console.warn(`[checksumer] no platform found for processOnlyPlatformId=${parsedConfig.processOnlyPlatformId}`);
    }

    const shouldUseArchive = platformConfigs.some(
        (platform) => platform.shouldArchive !== false && platform.matchByFileNameOnly !== true,
    );
    const sevenZipPath = shouldUseArchive ? await resolveSevenZipPath() : null;

    const summary: ProcessSummary = {
        platformsProcessed: 0,
        filesScanned: 0,
        filesMatched: 0,
        filesArchived: 0,
        filesDeletedAsDuplicate: 0,
        filesSkipped: 0,
    };
    const seenUnknownMd5ByPlatform = new Map<number, Set<string>>();

    for (const platform of platformConfigs) {
        const platformId = Number.parseInt(String(platform.id), 10);
        const destinationRomPath = typeof platform.destinationRomPath === "string" ? platform.destinationRomPath : "";
        const unknownRomPath = typeof platform.unknownRomPath === "string" ? platform.unknownRomPath : "";
        const shouldArchive = platform.shouldArchive !== false;
        const matchByFileNameOnly = platform.matchByFileNameOnly === true;
        const sourceRomPaths = Array.isArray(platform.sourceRomPaths)
            ? platform.sourceRomPaths.map((value) => String(value).trim()).filter(Boolean)
            : [];

        if (Number.isNaN(platformId) || !destinationRomPath || !unknownRomPath || sourceRomPaths.length === 0) {
            if (!unknownRomPath && !Number.isNaN(platformId)) {
                console.error(`[checksumer] skipping platform ${platformId}: missing required unknownRomPath`);
            }
            continue;
        }

        summary.platformsProcessed += 1;

        const allFiles: string[] = [];

        for (const sourceRootPath of sourceRomPaths) {
            try {
                allFiles.push(...(await listFilesRecursive(sourceRootPath)));
            } catch (error) { }
        }

        const totalFiles = allFiles.length;

        for (const [index, filePath] of allFiles.entries()) {
            summary.filesScanned += 1;

            try {
                const result = await processSourceFile(
                    platformId,
                    filePath,
                    destinationRomPath,
                    unknownRomPath,
                    sevenZipPath,
                    shouldArchive,
                    matchByFileNameOnly,
                    seenUnknownMd5ByPlatform,
                );
                const fileName = path.basename(filePath);
                console.log(`(${index + 1}/${totalFiles}) ${colorizeStatus(result.status)} ${fileName}`);

                if (result.matched) {
                    summary.filesMatched += 1;
                }

                if (result.archived) {
                    summary.filesArchived += 1;
                }

                if (result.duplicateDeleted) {
                    summary.filesDeletedAsDuplicate += 1;
                }

                if (result.skipped) {
                    summary.filesSkipped += 1;
                }
            } catch (error) {
                summary.filesSkipped += 1;
                console.error(`[checksumer] failed for file "${filePath}": ${error instanceof Error ? error.message : String(error)}`);
            }
        }
    }

    console.log("+------------------------+");
    console.log("|     CHECKSUMER DONE    |");
    console.log("+------------------------+");
    console.log(`| SCANNED : ${summary.filesScanned}`);
    console.log(`| MATCHED : ${summary.filesMatched}`);
    console.log(`| ARCHIVED: ${summary.filesArchived}`);
    console.log(`| DELETED : ${summary.filesDeletedAsDuplicate}`);
    console.log(`| SKIPPED : ${summary.filesSkipped}`);
    console.log("+------------------------+");

    return summary;
}
