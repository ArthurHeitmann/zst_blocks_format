//@ts-ignore
import { ZstdCodec } from "zstd-codec";

let hasLoadedResolve: () => void;
const hasLoaded: Promise<void> = new Promise((resolve) => {
	hasLoadedResolve = resolve;
});
let simple: any;
ZstdCodec.run((zstd: { Simple: new () => any }) => {
	simple = new zstd.Simple();
	hasLoadedResolve();
});

export async function zstCompress(data: Uint8Array): Promise<Uint8Array> {
	await hasLoaded;
	return simple.compress(data);
}

export async function zstDecompress(data: Uint8Array): Promise<Uint8Array> {
	await hasLoaded;
	return simple.decompress(data);
}
