import {Page} from "playwright";

export type PageStorageConstructor = new (url: string, page: Page) => PageStorage;


export abstract class PageStorage {
	protected readonly page: Page;
	protected readonly url: string;
	
	constructor(url: string, page: Page) {
		this.url = url;
		this.page = page;
	}
	
	abstract store(): Promise<void>;
}