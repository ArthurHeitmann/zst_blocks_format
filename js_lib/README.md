This is an incomplete implementation. Currently only reading of specific rows is supported. All functions are async.

## Usage

### Importing

```TypeScript
import { promises as fsp } from "fs";
import { ZstBlocksFile, RowPosition } from "/zst_blocks_format/js_lib/src/ZstBlocksFile";	// or wherever you put it
```

### Reading a row

```TypeScript
const filePath = "XYZ.zst_blocks";
const rowPosition = new RowPosition(0x123456, 42);
const file = await fsp.open(filePath, "r");
try {
	const row = await ZstBlocksFile.readBlockRowAt(file, rowPosition);
	// row is of type `Uint8Array`
} finally {
	await file.close();
}
```

### Reading multiple rows

```TypeScript
const filePath = "XYZ.zst_blocks";
const rowPositions = [
	new RowPosition(0x123456, 42),
	new RowPosition(0x123456, 43),
	new RowPosition(0x123456, 44),
];
const file = await fsp.open(filePath, "r");
try {
	const rows = await ZstBlocksFile.readBlockRowsAt(file, rowPositions);
	// rows is of type `Uint8Array[]`
} finally {
	await file.close();
}
```

## Getting started

Install dependencies:

```bash
npm install
```

If you want to build the library:

```bash
npm run build
```

Alternatively, you can add this project as a submodule to your project and use the TypeScript files directly.
