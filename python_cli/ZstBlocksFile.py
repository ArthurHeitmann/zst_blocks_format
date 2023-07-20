from __future__ import annotations
import os
from typing import BinaryIO, Iterable
from zstandard import ZstdDecompressor, ZstdCompressor

_endian = "little"

class ZstBlocksFile:
	blocks: list[ZstBlock]

	def __init__(self, blocks: list[ZstBlock]):
		self.blocks = blocks

	@staticmethod
	def readBlockRowAt(file: BinaryIO, blockPosition: int, rowIndex: int) -> bytes:
		file.seek(blockPosition)
		return ZstBlock.readRow(file, rowIndex)
	
	@staticmethod
	def streamRows(file: BinaryIO) -> bytes:
		fileSize = os.path.getsize(file.name)
		while file.tell() < fileSize:
			yield from ZstBlock.streamRows(file)
	
	@staticmethod
	def appendBlock(file: BinaryIO, rows: list[bytes]) -> None:
		file.seek(file.tell())
		ZstBlock(rows).write(file)
	
	@staticmethod
	def writeStream(file: BinaryIO, rowStream: Iterable[bytes], blockSize: int) -> None:
		pendingRows = []
		for row in rowStream:
			pendingRows.append(row)
			if len(pendingRows) >= blockSize:
				ZstBlock(pendingRows).write(file)
				pendingRows = []
		if len(pendingRows) > 0:
			ZstBlock(pendingRows).write(file)

	@staticmethod
	def writeBlocksStream(file: BinaryIO, blocksStream: Iterable[list[bytes]]) -> None:
		for rows in blocksStream:
			ZstBlock(rows).write(file)
	

class ZstBlock:
	rows: list[bytes]

	def __init__(self, rows: list[bytes]):
		self.rows = rows

	@staticmethod
	def fromStream(stream: Iterable[bytes]) -> ZstBlock:
		return ZstBlock(list(stream))

	@classmethod
	def streamRows(cls, file: BinaryIO) -> bytes:
		blockSize = int.from_bytes(file.read(4), _endian)
		compressedData = file.read(blockSize)
		stream = ZstdDecompressor().stream_reader(compressedData)
		decompressedData = stream.read()
		stream.close()
		bytes = bytearray(decompressedData)
		
		count = int.from_bytes(bytes[0:4], _endian)
		entries = [ZstBlockEntryInfo.read(bytes, 4 + i * ZstBlockEntryInfo.structSize) for i in range(count)]
		
		dataStart = 4 + count * ZstBlockEntryInfo.structSize
		for entry in entries:
			yield bytes[dataStart + entry.offset : dataStart + entry.offset + entry.size]
	
	@classmethod
	def readRow(cls, file: BinaryIO, rowIndex: int) -> bytes:
		blockSize = int.from_bytes(file.read(4), _endian)
		compressedData = file.read(blockSize)
		stream = ZstdDecompressor().stream_reader(compressedData)
		decompressedData = stream.read()
		stream.close()
		bytes = bytearray(decompressedData)
		
		count = int.from_bytes(bytes[0:4], _endian)
		if rowIndex >= count:
			raise Exception("Entry index out of range")
		row = ZstBlockEntryInfo.read(bytes, 4 + rowIndex * ZstBlockEntryInfo.structSize)

		dataStart = 4 + count * ZstBlockEntryInfo.structSize
		return bytes[dataStart + row.offset : dataStart + row.offset + row.size]

	def write(self, file: BinaryIO) -> None:
		uncompressedSize = \
			4 + \
			len(self.rows) * ZstBlockEntryInfo.structSize + \
			sum(len(row) for row in self.rows)
		uncompressedBytes = bytearray(uncompressedSize)
		uncompressedBytes[0:4] = len(self.rows).to_bytes(4, _endian)
		
		dataOffset = 4 + len(self.rows) * ZstBlockEntryInfo.structSize
		currentDataLocalOffset = 0
		for i in range(len(self.rows)):
			row = self.rows[i]
			entryInfo = ZstBlockEntryInfo(currentDataLocalOffset, len(row))
			entryInfo.write(uncompressedBytes, 4 + i * ZstBlockEntryInfo.structSize)
			uncompressedBytes[dataOffset + currentDataLocalOffset : dataOffset + currentDataLocalOffset + len(row)] = row
			currentDataLocalOffset += len(row)
		uncompressedData = bytes(uncompressedBytes)
		compressedData = ZstdCompressor().compress(uncompressedData)
		compressedSize = len(compressedData)
		blockBytes = bytearray(4 + compressedSize)
		blockBytes[0:4] = compressedSize.to_bytes(4, _endian)
		blockBytes[4:4+compressedSize] = compressedData
		file.write(blockBytes)

class ZstBlockEntryInfo:
	structSize = 8
	offset: int
	size: int

	def __init__(self, offset: int, size: int):
		self.offset = offset
		self.size = size

	@staticmethod
	def read(bytes: bytearray, position: int) -> ZstBlockEntryInfo:
		offset = int.from_bytes(bytes[position + 0 : position + 4], _endian)
		size = int.from_bytes(bytes[position + 4 : position + 8], _endian)
		return ZstBlockEntryInfo(offset, size)

	def write(self, bytes: bytearray, position: int) -> None:
		bytes[position + 0 : position + 4] = self.offset.to_bytes(4, _endian)
		bytes[position + 4 : position + 8] = self.size.to_bytes(4, _endian)
