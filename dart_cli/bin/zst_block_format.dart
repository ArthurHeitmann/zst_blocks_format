
import "dart:io";
import "dart:typed_data";

import "package:args/args.dart";
import "package:zst_block_format/CliArgs.dart";
import "package:zst_block_format/RandomAccessFileWrapper.dart";
import "package:zst_block_format/StdinParser.dart";
import "package:zst_block_format/ZstBlocksFile.dart";

void main(List<String> arguments) async {
  var argParser = ArgParser();
  argParser.addCommand("decode");
  argParser.addCommand("encode");
  argParser.addOption("input", abbr: "i", help: "Input file (.zst_blocks))");
  argParser.addFlag("stdin", abbr: "s", help: "Read from stdin (stream format: uint32 data_size, byte data[data_size])", defaultsTo: false, negatable: false);
  argParser.addFlag("stdin-as-blocks", abbr: "S", help: "Read from stdin (block format: uint32 count, (uint32 data_size, byte data[data_size])[count])", defaultsTo: false, negatable: false);
  argParser.addOption("output", abbr: "o", help: "Output file", mandatory: true);
  argParser.addFlag("append", abbr: "a", help: "Append to output file", defaultsTo: false, negatable: false);
  argParser.addOption("block-size", abbr: "b", help: "Block size", defaultsTo: "128");
  argParser.addFlag("help", abbr: "h", help: "Show help", defaultsTo: false, negatable: false);
  
  var args = argParser.parse(arguments);
  if (args["help"]) {
    print(argParser.usage);
    return;
  }
  var options = CliArgs(
    args["input"],
    args["output"],
    args.command?.name == "decode",
    args.command?.name == "encode",
    args["append"],
    args["stdin"],
    args["stdin-as-blocks"],
    int.tryParse(args["block-size"]),
  );

  if (options.encode == options.decode) {
    print("Either --encode or --decode must be specified");
    return;
  }
  if ((options.inputFile == null) ^ options.fromStdin) {
    print("Either --input or --stdin must be specified");
    return;
  }
  if (options.outputFile == null) {
    print("Output file must be specified");
    return;
  }
  if (options.blockSize == null) {
    print("Block size must be an integer");
    return;
  }
  if (options.blockSize! <= 0) {
    print("Block size must be greater than 0");
    return;
  }
  if (options.blockSize! > 0x7FFFFFFF) {
    print("Block size must be less than 0x7FFFFFFF");
    return;
  }
  
  RandomAccessFile? inOpenFile;
  RandomAccessFile? outOpenFile;
  Stream<Uint8List>? inRowsStream;
  Stream<List<Uint8List>>? inBlocksStream;
  try {
    if (options.inputFile != null) {
      var inFile = File(options.inputFile!);
      if (!await inFile.exists()) {
        print("Input file does not exist");
        return;
      }
      inOpenFile = await inFile.open(mode: FileMode.read);
      inRowsStream = ZstBlocksFile.streamRows(RandomAccessFileWrapper(inOpenFile));
    } else if (options.stdinAsBlocks) {
      inBlocksStream = stdinByteBlocksStream();
    } else {
      inRowsStream = stdinByteStream();
    }

    var outFile = File(options.outputFile!);
    var outDir = outFile.parent;
    await outDir.create(recursive: true);
    outOpenFile = await outFile.open(mode: options.append ? FileMode.append : FileMode.write);
    var outFileWrapper = RandomAccessFileWrapper(outOpenFile);
    if (options.append)
      await outFileWrapper.setPosition(await outOpenFile.length());

    if (options.encode) {
      if (inRowsStream != null) {
        await ZstBlocksFile.writeStream(outFileWrapper, inRowsStream, options.blockSize!);
      } else if (inBlocksStream != null) {
        await ZstBlocksFile.writeBlocksStream(outFileWrapper, inBlocksStream);
      } else
        throw Exception("No input stream");
    } else {
      await for (var row in ZstBlocksFile.streamRows(RandomAccessFileWrapper(inOpenFile!))) {
        await outOpenFile.writeFrom(row);
      }
    }
  } finally {
    await inOpenFile?.close();
    await outOpenFile?.close();
  }
}
