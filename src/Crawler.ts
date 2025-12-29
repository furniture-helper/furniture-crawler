import {Dataset, PlaywrightCrawler} from "crawlee";
import {getMaxConcurrency, getMaxRequestsPerCrawl, getMaxRequestsPerMinute} from "./config";
import {getSpecialization} from "./specializations/Specialization";
import {savePageSnapshot} from "./utils/file_utils";

export default class Crawler {
	
	private readonly crawler: PlaywrightCrawler;
	
	constructor() {
		this.crawler = new PlaywrightCrawler({
			maxRequestsPerCrawl: getMaxRequestsPerCrawl(),
			maxConcurrency: getMaxConcurrency(),
			maxRequestsPerMinute: getMaxRequestsPerMinute(),
			
			async requestHandler({ request, page, enqueueLinks, log }) {
				await page.waitForLoadState('load');
				
				// wait for network to be idle (or timeout after 10 seconds)
				await page.waitForLoadState('networkidle', { timeout: 10000 }).catch(() => {
					log.warning(`Network idle timeout for ${request.loadedUrl}`);
				});
				
				const specialization = await getSpecialization(request.loadedUrl, page);
				if (specialization) {
					await specialization.apply();
				}
				
				const title = await page.title();
				log.info(`Parsing page: ${request.loadedUrl}`);
				
				await savePageSnapshot(request.loadedUrl, page);
				
				await Dataset.pushData({ title, url: request.loadedUrl });
				await enqueueLinks();
			}
		});
	}
	
	public async run(startUrl: string) {
		await this.crawler.run([startUrl]);
	}
}