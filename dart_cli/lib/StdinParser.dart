
import "dart:convert";
import "dart:io";
import "dart:typed_data";

/// read in 4 bytes, convert to uint32
/// read that many bytes, yield as Uint8List
/// repeat
Stream<Uint8List> stdinByteStream([int Function()? readByteSync]) async* {
  readByteSync ??= stdin.readByteSync;
  while (true) {
    var t1 = DateTime.now();
    Uint8List sizeBuffer = Uint8List(4);
    for (int i = 0; i < 4; i++) {
      int byte = readByteSync();
      if (byte == -1)
        return;
      sizeBuffer[i] = byte;
    }
    int size = ByteData.view(sizeBuffer.buffer).getUint32(0, Endian.little);
    Uint8List data = Uint8List(size);
    for (int i = 0; i < size; i++) {
      int byte = readByteSync();
      if (byte == -1)
        return;
      data[i] = byte;
    }
    var t2 = DateTime.now();
    print("Read block of size $size in ${t2.difference(t1).inMilliseconds}ms");
    yield data;
  }
}

final testJsonRows = [
  { "test": 123, "test2": "abc" },
  { "hello": "world", "test": 456 },
  { "test": [ "arrays" ], "and": { "objects": "too" } },
  { "test": 789, "test2": "def" },
  { "test": 101112, "test2": "ghi" }
];
final testJsonStringRows = testJsonRows
  .map((row) => JsonEncoder.withIndent("\t").convert(row))
  .toList();
final testJsonByteRows = testJsonStringRows
  .map((row) {
    var data = utf8.encode(row);
    var rowBytes = ByteData(4 + data.length);
    rowBytes.setUint32(0, data.length, Endian.little);
    rowBytes.buffer.asUint8List(4).setAll(0, data);
    return rowBytes.buffer.asUint8List();
  })
  .toList();

Stream<Uint8List> testJsonByteStream() async* {
  int pos = 0;
  int row = 0;
  int readJsonByte() {
    if (row >= testJsonByteRows.length)
      return -1;
    int byte = testJsonByteRows[row][pos];
    pos++;
    if (pos >= testJsonByteRows[row].length) {
      pos = 0;
      row++;
    }
    return byte;
  }
  yield* stdinByteStream(readJsonByte);
}
