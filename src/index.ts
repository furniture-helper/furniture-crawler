import Crawler from "./Crawler";
import {getStartUrl} from "./config";

async function main() {
	const crawler = new Crawler();
	const startUrl = getStartUrl();
	await crawler.run(startUrl);
}
main().catch((err) => {
	console.error(err);
	process.exit(1);
});
