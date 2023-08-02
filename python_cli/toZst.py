import os
import sys
import time

from typing import Iterable

import zstandard

from ZstBlocksFile import ZstBlocksFile


def zstBlocksBytesStream(path: str) -> Iterable[bytes]:
	isFirst = True
	with open(path, "rb") as file:
		if os.path.getsize(path) > 1024 * 1024 * 1024 * 2:
			print("Preparing...")
		totalBlocksCount = ZstBlocksFile.countBlocks(file)
		def printProgress(blockIndex: int):
			if blockIndex % 200 != 0:
				return
			print(f"\r{blockIndex/totalBlocksCount:.1%}", end="")
		for row in ZstBlocksFile.streamRows(file, blockIndexProgressCallback=printProgress):
			if not isFirst:
				yield b"\n"
			isFirst = False
			yield row
	print("\r100.0%")


def main():
	if "-h" in sys.argv or "--help" in sys.argv:
		print("Usage: python toZst.py <input file> [<output file>]")
		exit(0)
	inFiles = sys.argv[1:]
	if len(inFiles) not in (1, 2):
		print("Usage: python toZst.py <input file> [<output file>]")
		exit(1)
	inFile = inFiles[0]
	if not os.path.exists(inFile):
		print(f"Input file '{inFile}' does not exist")
		exit(1)
	outFile = inFiles[1] if len(inFiles) == 2 else inFile + ".zst"

	zstdCompressor = zstandard.ZstdCompressor()
	maxFrameSize = 1024 * 1024 * 1024
	pendingBytes = bytearray(maxFrameSize)
	pendingBytesLength = 0
	with open(outFile, "wb") as file:
		compressedStream = zstdCompressor.stream_writer(file)
		for row in zstBlocksBytesStream(inFile):
			if pendingBytesLength + len(row) >= maxFrameSize:
				compressedStream.write(pendingBytes[:pendingBytesLength])
				pendingBytesLength = 0
			pendingBytes[pendingBytesLength:pendingBytesLength + len(row)] = row
			pendingBytesLength += len(row)
		if pendingBytesLength > 0:
			compressedStream.write(pendingBytes[:pendingBytesLength])
			compressedStream.flush()
		compressedStream.flush(zstandard.FLUSH_FRAME)


if __name__ == "__main__":
	main()
