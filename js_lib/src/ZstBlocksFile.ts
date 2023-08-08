import { promises as fsp } from "fs";
import { zstCompress, zstDecompress } from "./zst";


const defaultCompressionLevel = 3;

export class ZstBlocksFile {
	static async readBlockRowAt(file: fsp.FileHandle, position: RowPosition): Promise<Uint8Array> {
		return await ZstBlock.readRowAt(file, position);
	}

	static async readMultipleBlockRowsAt(file: fsp.FileHandle, positions: RowPosition[]): Promise<Uint8Array[]> {
		const blockGroups: { [blockOffset: number]: RowPosition[] } = {};
		for (let i = 0; i < positions.length; i++) {
			const position = positions[i];
			if (!blockGroups[position.blockOffset])
				blockGroups[position.blockOffset] = [];
			blockGroups[position.blockOffset].push(position);
		}

		const blocks: Uint8Array[][] = await Promise.all(Object.keys(blockGroups).map(async (blockOffsetStr) => {
			const blockOffset = parseInt(blockOffsetStr);
			const blockPositions = blockGroups[blockOffset];
			return await ZstBlock.readMultipleRowsAt(file, blockOffset, blockPositions);
		}));

		return blocks.flat();
	}
}

class ZstBlock {
	rows: Uint8Array[] = [];

	constructor(rows: Uint8Array[]) {
		this.rows = rows;
	}

	static async readRowAt(file: fsp.FileHandle, position: RowPosition): Promise<Uint8Array> {
		const compressedSize = await readUint32(file, position.blockOffset);
		const compressedData = await readBytes(file, position.blockOffset + 4, compressedSize);
		const decompressedDataAr = await zstDecompress(compressedData);
		const decompressedData = Buffer.from(decompressedDataAr.buffer);

		const rowCount = decompressedData.readUInt32LE(0);
		const rowInfos: ZstRowInfo[] = new Array(rowCount);
		for (let i = 0; i < rowCount; i++) {
			const rowInfo = ZstRowInfo.read(decompressedData, 4 + i * ZstRowInfo.structSize);
			rowInfos[i] = rowInfo;
		}

		const dataStart = 4 + rowCount * ZstRowInfo.structSize;
		const rowInfo = rowInfos[position.rowIndex];
		return decompressedData.subarray(dataStart + rowInfo.offset, dataStart + rowInfo.offset + rowInfo.size);
	}

	static async readMultipleRowsAt(file: fsp.FileHandle, blockOffset: number, positions: RowPosition[]): Promise<Uint8Array[]> {
		const compressedSize = await readUint32(file, blockOffset);
		const compressedData = await readBytes(file, blockOffset + 4, compressedSize);
		const decompressedDataAr = await zstDecompress(compressedData);
		const decompressedData = Buffer.from(decompressedDataAr.buffer);

		const rowCount = decompressedData.readUInt32LE(0);
		const rowInfos: ZstRowInfo[] = new Array(rowCount);
		for (let i = 0; i < rowCount; i++) {
			const rowInfo = ZstRowInfo.read(decompressedData, 4 + i * ZstRowInfo.structSize);
			rowInfos[i] = rowInfo;
		}

		const dataStart = 4 + rowCount * ZstRowInfo.structSize;
		const rows: Uint8Array[] = [];
		for (let i = 0; i < positions.length; i++) {
			const position = positions[i];
			const rowInfo = rowInfos[position.rowIndex];
			rows.push(decompressedData.subarray(dataStart + rowInfo.offset, dataStart + rowInfo.offset + rowInfo.size));
		}
		return rows;
	}
}

class ZstRowInfo {
	static readonly structSize = 8;
	offset: number;
	size: number;

	constructor(offset: number, size: number) {
		this.offset = offset;
		this.size = size;
	}

	static read(buffer: Buffer, offset: number): ZstRowInfo {
		const rowOffset = buffer.readUInt32LE(offset);
		const rowSize = buffer.readUInt32LE(offset + 4);
		return new ZstRowInfo(rowOffset, rowSize);
	}

	write(buffer: Buffer, offset: number): void {
		buffer.writeUInt32LE(this.offset, offset);
		buffer.writeUInt32LE(this.size, offset + 4);
	}

}

export interface RowPosition {
	blockOffset: number;
	rowIndex: number;
}

async function readUint32(file: fsp.FileHandle, offset: number): Promise<number> {
	const uint32ReadBuffer = Buffer.alloc(4);
	await file.read(uint32ReadBuffer, 0, 4, offset);
	return uint32ReadBuffer.readUInt32LE();
}

async function readBytes(file: fsp.FileHandle, offset: number, length: number): Promise<Buffer> {
	const bytes = Buffer.alloc(length);
	await file.read(bytes, 0, length, offset);
	return bytes;
}
