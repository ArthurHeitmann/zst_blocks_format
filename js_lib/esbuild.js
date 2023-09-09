import esbuild from "esbuild"
import { typecheckPlugin } from "@jgoz/esbuild-plugin-typecheck";

const watch = process.argv.includes("--watch") || process.argv.includes("-w");

const context = await esbuild.context({
	entryPoints: [
		"src/ZstBLocksFile.ts"
	],
	bundle: true,
	sourcemap: true,
	platform: "neutral",
	packages: "external",
	outdir: "./src",
	plugins: [
		typecheckPlugin({
			watch: watch,
			omitStartLog: true,
		})
	],
});

if (watch) {
	await context.watch();
}
else {
	await context.rebuild();
	await context.dispose();
}
