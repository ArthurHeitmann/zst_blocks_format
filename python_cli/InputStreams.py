
import sys
from typing import Iterable

def _getUint32FromList(bytes: bytes) -> int:
	return int.from_bytes(bytes[:4], "little")

_endFlag = 0xFFFFFFFF

def textFileLineStream(path: str) -> Iterable[bytes]:
	with open(path, "r", encoding="utf-8") as file:
		for line in file:
			yield line.encode("utf-8")

def stdinByteStream() -> Iterable[bytes]:
	with open(sys.stdin.fileno(), "rb") as file:
		while True:
			sizeBytes = file.read(4)
			if len(sizeBytes) != 4:
				break
			size = _getUint32FromList(sizeBytes)
			if size == _endFlag:
				break
			yield file.read(size)

def stdinByteBlocksStream() -> Iterable[list[bytes]]:
	with open(sys.stdin.fileno(), "rb") as file:
		while True:
			countBytes = file.read(4)
			if len(countBytes) != 4:
				break
			count = _getUint32FromList(countBytes)
			if count == _endFlag:
				break
			rows = []
			for i in range(count):
				sizeBytes = file.read(4)
				if len(sizeBytes) != 4:
					break
				size = _getUint32FromList(sizeBytes)
				if size == _endFlag:
					break
				rows.append(file.read(size))
			yield rows
