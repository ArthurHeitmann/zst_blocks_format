
import "dart:async";
import "dart:convert";
import "dart:io";
import "dart:typed_data";


int _getUint32FromList(List<int> bytes) {
  return ByteData.view(Uint8List.fromList(bytes).buffer).getUint32(0, Endian.little);
}

const _endFlag = 0xFFFFFFFF;

/// read in 4 bytes, convert to uint32
/// read that many bytes, yield as Uint8List
/// repeat
// Stream<Uint8List> stdinByteStream([int Function()? readByteSync]) async* {
Stream<Uint8List> stdinByteStream() {
  List<int> pendingBytes = [];
  StreamController<Uint8List> controller = StreamController<Uint8List>();
  bool isReadingSize = true;
  int? size;
  stdin.listen((event) {
    pendingBytes.addAll(event);
    while (pendingBytes.isNotEmpty) {
      if (isReadingSize) {
        if (pendingBytes.length < 4)
          return;
        size = _getUint32FromList(pendingBytes);
        if (size == _endFlag) {
          controller.close();
          return;
        }
        pendingBytes.removeRange(0, 4);
        isReadingSize = false;
      } else {
        if (pendingBytes.length < size!)
          return;
        Uint8List data = Uint8List.fromList(pendingBytes.sublist(0, size!));
        pendingBytes.removeRange(0, size!);
        isReadingSize = true;
        controller.add(data);
      }
    }
  }, onDone: () {
    controller.close();
  });

  return controller.stream;
}

/// read in uint32 for row count
/// for each row:
///   read in uint32 for row size
///   read that many bytes, yield as Uint8List
Stream<List<Uint8List>> stdinByteBlocksStream() {
  List<int> pendingBytes = [];
  StreamController<List<Uint8List>> controller = StreamController<List<Uint8List>>();
  bool isReadingRowCount = true;
  bool isReadingRowSize = true;
  int? rowCount;
  int? rowSize;
  List<Uint8List> rows = [];
  stdin.listen((event) {
    pendingBytes.addAll(event);
    while (pendingBytes.isNotEmpty) {
      if (isReadingRowCount) {
        if (pendingBytes.length < 4)
          return;
        rowCount = _getUint32FromList(pendingBytes);
        if (rowCount == _endFlag) {
          controller.close();
          return;
        }
        pendingBytes.removeRange(0, 4);
        isReadingRowCount = false;
      } else {
        if (isReadingRowSize) {
          if (pendingBytes.length < 4)
            return;
          rowSize = _getUint32FromList(pendingBytes);
          pendingBytes.removeRange(0, 4);
          isReadingRowSize = false;
        } else {
          if (pendingBytes.length < rowSize!)
            return;
          Uint8List data = Uint8List.fromList(pendingBytes.sublist(0, rowSize!));
          pendingBytes.removeRange(0, rowSize!);
          isReadingRowSize = true;
          rows.add(data);
          if (rows.length == rowCount) {
            controller.add(rows);
            rows = [];
            isReadingRowCount = true;
          }
        }
      }
    }
  }, onDone: () {
    controller.close();
  });

  return controller.stream;
}

Stream<Uint8List> textFileLineStream(String filename) async* {
  var lines = File(filename).openRead()
    .transform(utf8.decoder)
    .transform(LineSplitter());
  await for (var line in lines) {
    yield Uint8List.fromList(utf8.encode(line));
  }
}
