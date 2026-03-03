import { runChecksumerService } from "./service";

async function run(): Promise<void> {
    await runChecksumerService();
}

void run().catch((error) => {
    console.error("[checksumer] failed", error instanceof Error ? error.message : String(error));
    process.exit(1);
});
