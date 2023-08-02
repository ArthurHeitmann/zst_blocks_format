import argparse
from dataclasses import dataclass
import os
import sys
from typing import BinaryIO

from ZstBlocksFile import ZstBlocksFile
from InputStreams import stdinByteBlocksStream, stdinByteStream, textFileLineStream

@dataclass
class CliArgs:
	inputFile: str|None
	outputFile: str|None
	decode: bool
	encode: bool
	append: bool
	fromStdin: bool
	toStdout: bool
	inputAsText: bool
	stdinAsBlocks: bool
	separateLines: bool
	blockSize: int|None

def main():
	parser = argparse.ArgumentParser(description="Encode or decode zst blocks")
	parser.add_argument("command", choices=["encode", "decode"], help="Command to run")
	parser.add_argument("-i", "--input", help="Input file")
	parser.add_argument("-t", "--input-as-text", action="store_true", help="Read input file as text (one line = one row)")
	parser.add_argument("--stdin", action="store_true", help="Read from stdin (stream format: uint32 data_size, byte data[data_size])")
	parser.add_argument("--stdin-as-blocks", action="store_true", help="Read from stdin (block format: uint32 count, (uint32 data_size, byte data[data_size])[count])")
	parser.add_argument("-o", "--output", help="Output file")
	parser.add_argument("--stdout", action="store_true", help="Write to stdout")
	parser.add_argument("-a", "--append", action="store_true", help="Append to output file")
	parser.add_argument("--no-line-separator", action="store_false", help="Separate lines in output file (only for decode)")
	parser.add_argument("-b", "--block-size", type=int, default=256, help="Block size")

	args = parser.parse_args()
	options = CliArgs(
		args.input,
		args.output,
		args.command == "decode",
		args.command == "encode",
		args.append,
		args.stdin,
		args.stdout,
		args.input_as_text,
		args.stdin_as_blocks,
		args.no_line_separator,
		args.block_size,
	)

	if options.encode == options.decode:
		print("Either --encode or --decode must be specified")
		exit(1)
	if (options.inputFile == None) ^ options.fromStdin:
		print("Either --input or --stdin must be specified")
		exit(1)
	if options.outputFile == None and options.toStdout == None:
		print("Either --output or --stdout must be specified")
		exit(1)
	if options.blockSize == None:
		print("Block size must be an integer")
		exit(1)
	if options.blockSize <= 0:
		print("Block size must be greater than 0")
		exit(1)
	if options.blockSize > 0x7FFFFFFF:
		print("Block size must be less than 0x7FFFFFFF")
		exit(1)
	
	inOpenFile: BinaryIO|None = None
	outOpenFile: BinaryIO|None = None
	inRowsStream = None
	inBlocksStream = None
	try:
		if options.inputFile != None:
			if not os.path.exists(options.inputFile):
				print(f"Input file '{options.inputFile}' does not exist")
				exit(1)
			if options.inputAsText:
				inRowsStream = textFileLineStream(options.inputFile)
			elif not options.inputFile.endswith(".zst_blocks"):
				print("Input file must have .zst_blocks extension")
				exit(1)
			else:
				inFile = open(options.inputFile, "rb")
				inOpenFile = inFile
				inRowsStream = ZstBlocksFile.streamRows(inFile)
		elif options.stdinAsBlocks:
			inBlocksStream = stdinByteBlocksStream()
		else:
			inRowsStream = stdinByteStream()

		if options.toStdout:
			outOpenFile = open(sys.stdout.fileno(), "wb")
		elif options.outputFile != None:
			outOpenFile = open(options.outputFile, "ab" if options.append else "wb")
		else:
			raise Exception("No output file")
		if options.append:
			outOpenFile.seek(outOpenFile.tell())

		if options.encode:
			if inRowsStream != None:
				ZstBlocksFile.writeStream(outOpenFile, inRowsStream, options.blockSize)
			elif inBlocksStream != None:
				ZstBlocksFile.writeBlocksStream(outOpenFile, inBlocksStream)
			else:
				raise Exception("No input stream")
		else:
			if inOpenFile is None:
				raise Exception("No input file")
			isFirst = True
			for row in ZstBlocksFile.streamRows(inOpenFile):
				if options.separateLines and not isFirst:
					outOpenFile.write(b"\n")
				isFirst = False
				outOpenFile.write(row)
	finally:
		if inOpenFile is not None:
			inOpenFile.close()
		if outOpenFile is not None:
			outOpenFile.close()

if __name__ == "__main__":
	main()
