import { compress, decompress } from '@mongodb-js/zstd';


export async function zstCompress(data: Buffer): Promise<Buffer> {
	return compress(data);
}

export async function zstDecompress(data: Buffer): Promise<Buffer> {
	return decompress(data);
}
