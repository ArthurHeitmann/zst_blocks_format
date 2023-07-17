
import "dart:typed_data";

import "package:es_compression/zstd.dart";

import "RandomAccessFileWrapper.dart";

const _endian = Endian.little;

class ZstBlocksFile {
  final List<ZstBlock> blocks;

  const ZstBlocksFile(this.blocks);

  static Future<Uint8List> readBlockRowAt(RandomAccessFileWrapper file, int blockPosition, int rowIndex) async {
    await file.setPosition(blockPosition);
    return await ZstBlock.readRow(file, rowIndex);
  }

  static Stream<Uint8List> streamRows(RandomAccessFileWrapper file) async* {
    var fileSize = await file.length;
    while (file.position < fileSize) {
      yield* ZstBlock.streamRows(file);
    }
  }

  static Future<void> appendBlock(RandomAccessFileWrapper file, List<Uint8List> rows) async {
    await file.setPosition(await file.length);
    await ZstBlock(rows).write(file);
  }

  static Future<void> writeStream(RandomAccessFileWrapper file, Stream<Uint8List> rowStream, int blockSize) async {
    List<Uint8List> pendingRows = [];
    await for (var row in rowStream) {
      pendingRows.add(row);
      if (pendingRows.length >= blockSize) {
        await ZstBlock(pendingRows).write(file);
        pendingRows = [];
      }
    }
    if (pendingRows.isNotEmpty)
      await ZstBlock(pendingRows).write(file);
  }

  static Future<void> writeBlocksStream(RandomAccessFileWrapper file, Stream<List<Uint8List>> blocksStream) async {
    await for (var rows in blocksStream) {
      await ZstBlock(rows).write(file);
    }
  }
}

/// struct ZstCompressedBlock {
/// 	uint32 compressedSize;
/// 	byte compressedData[compressedSize];
/// }
/// 
/// struct ZstDecompressedBlock {
/// 	uint32 count;
/// 	ZstBlockEntry entries[count];
/// 	BlockRow rows[count];
/// }
class ZstBlock {
  final List<Uint8List> rows;

  const ZstBlock(this.rows);

  static Future<ZstBlock> fromStream(Stream<Uint8List> stream) async {
    return ZstBlock(await stream.toList());
  }

  static Stream<Uint8List> streamRows(RandomAccessFileWrapper file) async* {
    var blockSize = await file.readUint32();
    var compressedData = await file.readBytes(blockSize);
    var decompressedData = zstd.decode(compressedData);
    var bytes = ByteData(decompressedData.length);
    bytes.buffer.asUint8List().setAll(0, decompressedData);
    
    var count = bytes.getUint32(0, _endian);
    var entries = List.generate(
      count,
      (i) => ZstBlockEntryInfo.read(bytes, 4 + i * ZstBlockEntryInfo.structSize)
    );
    
    var dataStart = 4 + count * ZstBlockEntryInfo.structSize;
    for (var entry in entries) {
      yield bytes.buffer.asUint8List(dataStart + entry.offset, entry.size);
    }
  }

  static Future<Uint8List> readRow(RandomAccessFileWrapper file, int rowIndex) async {
    var blockSize = await file.readUint32();
    var compressedData = await file.readBytes(blockSize);
    var decompressedData = zstd.decode(compressedData);
    var bytes = ByteData(decompressedData.length);
    bytes.buffer.asUint8List().setAll(0, decompressedData);
    
    var count = bytes.getUint32(0, _endian);
    if (rowIndex >= count)
      throw Exception("Entry index out of range");
    var row = ZstBlockEntryInfo.read(bytes, 4 + rowIndex * ZstBlockEntryInfo.structSize);

    var dataStart = 4 + count * ZstBlockEntryInfo.structSize;
    return bytes.buffer.asUint8List(dataStart + row.offset, row.size);
  }

  Future<void> write(RandomAccessFileWrapper file) async {
    var uncompressedSize = 
      4 +
      rows.length * ZstBlockEntryInfo.structSize +
      rows.fold<int>(0, (sum, row) => sum + row.length);
    var uncompressedBytes = ByteData(uncompressedSize);
    uncompressedBytes.setUint32(0, rows.length, _endian);
    
    var dataOffset = 4 + rows.length * ZstBlockEntryInfo.structSize;
    var currentDataLocalOffset = 0;
    for (int i = 0; i < rows.length; i++) {
      var row = rows[i];
      var entryInfo = ZstBlockEntryInfo(currentDataLocalOffset, row.length);
      entryInfo.write(uncompressedBytes, 4 + i * ZstBlockEntryInfo.structSize);
      uncompressedBytes.buffer.asUint8List(dataOffset + currentDataLocalOffset, row.length).setAll(0, row);
      currentDataLocalOffset += row.length;
    }
    var uncompressedData = uncompressedBytes.buffer.asUint8List();
    var compressedData = zstd.encode(uncompressedData);
    var compressedSize = compressedData.length;
    var blockBytes = ByteData(4 + compressedSize);
    blockBytes.setUint32(0, compressedSize, _endian);
    blockBytes.buffer.asUint8List(4).setAll(0, compressedData);
    await file.write(blockBytes);
  }
}

class ZstBlockEntryInfo {
  static const int structSize = 8;
  final int offset;
  final int size;

  const ZstBlockEntryInfo(this.offset, this.size);

  static ZstBlockEntryInfo read(ByteData bytes, int position) {
    var offset = bytes.getUint32(position + 0, _endian);
    var size = bytes.getUint32(position + 4, _endian);
    return ZstBlockEntryInfo(offset, size);
  }

  void write(ByteData bytes, int position) {
    bytes.setUint32(position + 0, offset, _endian);
    bytes.setUint32(position + 4, size, _endian);
  }
}
