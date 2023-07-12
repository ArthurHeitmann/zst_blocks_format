
import "dart:io";
import "dart:typed_data";

class RandomAccessFileWrapper {
  final endian = Endian.little;
  final RandomAccessFile file;
  int _position = 0;

  RandomAccessFileWrapper(this.file);

  static Future<void> openWith(File file, FileMode mode, Future<void> Function(RandomAccessFileWrapper) callback) async {
    var raf = await file.open(mode: mode);
    try {
      var wrapper = RandomAccessFileWrapper(raf);
      await callback(wrapper);
    } finally {
      await raf.close();
    }
  }

  Future<void> setPosition(int position) async {
    await file.setPosition(position);
    _position = position;
  }

  int get position => _position;

  Future<int> get length => file.length();

  Future<ByteData> read(int size) async {
    var bytes = await file.read(size);
    _position += size;
    await file.setPosition(_position);
    return bytes.buffer.asByteData();
  }

  Future<Uint8List> readBytes(int size) async {
    var bytes = await file.read(size);
    _position += size;
    await file.setPosition(_position);
    return bytes;
  }

  Future<int> readUint16() async {
    var bytes = await read(2);
    return bytes.getUint16(0, endian);
  }

  Future<int> readUint32() async {
    var bytes = await read(4);
    return bytes.getUint32(0, endian);
  }

  Future<int> readUint64() async {
    var bytes = await read(8);
    return bytes.getUint64(0, endian);
  }

  Future<void> write(ByteData bytes) async {
    await file.writeFrom(bytes.buffer.asUint8List());
    _position += bytes.lengthInBytes;
    await file.setPosition(_position);
  }

  Future<void> writeUint16(int value) async {
    var bytes = ByteData(2);
    bytes.setUint16(0, value, endian);
    await write(bytes);
  }

  Future<void> writeUint32(int value) async {
    var bytes = ByteData(4);
    bytes.setUint32(0, value, endian);
    await write(bytes);
  }

  Future<void> writeUint64(int value) async {
    var bytes = ByteData(8);
    bytes.setUint64(0, value, endian);
    await write(bytes);
  }
}
