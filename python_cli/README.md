# The zst_blocks format with Python

- [CLI usage](#cli-usage)
- [Usage in Python](#usage-in-python)

For information about the file format itself, see [here](../)

## CLI usage

#### Decompressing a .zst_blocks file

```bash
python zst_blocks.py decode -i inFile.zst_blocks -o outFile.txt
```

By default each row is separated by a new line (LF) character. To disable that use the `--no-line-separator` option.

#### Printing/writing a .zst_blocks file to stdout, line by line

```bash
python zst_blocks.py decode -i inFile.zst_blocks --stdout
```

#### Compressing a text file, line by line

```bash
python zst_blocks.py encode -t -i inFile.txt -o outFile.zst_blocks
```

The `-t` option is a shorthand for `--input-as-text`.  
Use the `-b` or `--block-size` parameter to specify the block size. Default is 256. The higher the block size the better the compression ratio (with diminishing returns). However it also increases the time read out a single row.  
Use the `-a` or `--append` option to append to the end of an existing file.  
Text is encoded as utf-8.

#### Converting a .zst_blocks file to a .zst file

**Using the zstd binary (significantly faster):**

For this you need to have the `zstd`/`zstd.exe` binary downloaded.

```bash
python zst_blocks.py decode -i inFile.zst_blocks --stdout | zstd -o outFile.zst
```

**Using the `toZst.py` script (slower):**

This is slower, but you don't need to download the `zstd` binary. Also you get a percentage progress indicator.

```bash
python toZst.py inFile.zst_blocks [outFile.zst]
```

Rows are separated by a new line character (LF).

### (Advanced) Compressing a data stream from stdin

#### From a row stream

```bash
python zst_blocks.py encode --stdin --output outFile.zst_blocks
```

Shorthand for `--stdin` is `-s`.

The stdin binary stream uses the following format:

- `uint32 row_size` indicating the size in bytes of the row
- `byte row_bytes[row_size]`
- repeat...

If the `row_size` is 0xFFFFFFFF it indicates end of stream. It's optional but recommended, to ensure that all data is flushed.

#### From a block stream

```bash
python zst_blocks.py encode --stdin --stdin-as-blocks --output outFile.zst_blocks
```

Shorthand for `--stdin-as-blocks` is `-S`.

The stdin binary stream uses the following format:

- `uint32 row_count` indicating the number of rows in this block
	- `uint32 row_size` indicating the size in bytes of the row
	- `byte row_bytes[row_size]`
- repeat...

If the `row_count` is 0xFFFFFFFF it indicates end of stream. It's optional but recommended, to ensure that all data is flushed.

## Usage in Python

#### Iterating over all rows in a .zst_blocks file

```python
from ZstBlocksFile import ZstBlocksFile

with open(path, "rb") as f:
	for row in ZstBlocksFile.streamRows(f):
		# row is of type `bytes`
		...
```

#### Lookup a row from a known position

```python
from ZstBlocksFile import ZstBlocksFile, RowPosition

rowPosition = RowPosition(0x12263, 42)
with open(path, "rb") as f:
	row = ZstBlocksFile.readBlockRowAt(f, rowPosition)
```

#### Lookup multiple rows from known positions

```python
from ZstBlocksFile import ZstBlocksFile, RowPosition

rowPositions = [
	RowPosition(0x12263, 42),
	RowPosition(0x12263, 73),
	RowPosition(0x5E8F7, 5),
]
with open(path, "rb") as f:
	rows = ZstBlocksFile.readMultipleBlocks(f, rowPositions)
	...
```

#### Writing

For writing from a stream of rows use:

```python
ZstBlocksFile.writeStream(file: BinaryIO, rowStream: Iterable[bytes], blockSize: int, rowPositions: list[RowPosition]|None = None, compressionLevel = _defaultCompressionLevel) -> None
```

- `rowPositions` is optional and if supplied, will be filled with the `RowPosition` of each row. Useful if you want to generate some sort of index over the file or for usage in a separate database.
- `compressionLevel` is optional. It is the zst compression level. Allowed range: 1 - 22. Default is 3.

For writing a stream of blocks:

```python
ZstBlocksFile.writeBlocksStream(file: BinaryIO, blocksStream: Iterable[list[bytes]], rowPositions: list[RowPosition]|None = None, compressionLevel = _defaultCompressionLevel) -> None
```

`rowPositions` and `compressionLevel` are the same as above.

#### Get all row positions in a file

```python
from ZstBlocksFile import ZstBlocksFile

with open(path, "rb") as f:
	rowPositions = list(ZstBlocksFile.generateRowPositions(file))
```
