# Zst Blocks format

The zst blocks format is a simple format for storing rows of data in a compressed format. Compression is done using zstandard (zstd).

## If you're just interested in using the CLI or the python scripts, see [here](./python_cli/).

## Format

A file is made up of a series of blocks. Each block is independent and can be decoded without any information from other blocks.

```C
struct ZstCompressedBlock {
	uint32 compressed_size;
	byte compressed_data[compressed_size];
};
```

The decompressed data has the following format:

```C
struct ZstBlock {
	uint32 row_count;
	ZstRowInfo row_infos[row_count];
	byte rows_data[];
};

struct ZstRowInfo {
	uint32 offset;
	uint32 size;
};
```

The `offset` is relative to the start of the `rows_data` field.

All fields are little endian.

## Comparison to plain zstd

### Pros

- Can decode individual rows without decoding the entire file. For individual reads this can be much faster than decoding the entire file.
- New data can be appended to the end of the file without having to decode or re-encode the entire file.
- More memory efficient, since the entire file does not need to be decoded into memory.

### Cons

- Slightly larger file size than plain zstd. This is because each block has a header and the data is not compressed as well as a single zstd stream.
- Slower to decode the entire file. This is because each block has to be decoded individually.
