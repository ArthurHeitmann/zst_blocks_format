
class CliArgs {
  final String? inputFile;
  final String? outputFile;
  final bool decode;
  final bool encode;
  final bool append;
  final bool fromStdin;
  final bool inputAsText;
  final bool stdinAsBlocks;
  final int? blockSize;

  const CliArgs(this.inputFile, this.outputFile, this.decode, this.encode, this.append, this.fromStdin, this.inputAsText, this.stdinAsBlocks, this.blockSize);
}
