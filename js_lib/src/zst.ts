import { init, decompress } from'@bokuweb/zstd-wasm';

let initComplete = false;
const initCompletePromise = init().then(() => {
	initComplete = true;
});

export async function zstDecompress(data: Buffer): Promise<Buffer> {
	if (!initComplete)
		await initCompletePromise;
    return Buffer.from(decompress(data));
}
