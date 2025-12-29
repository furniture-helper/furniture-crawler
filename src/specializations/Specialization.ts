import {Page} from "playwright";

abstract class Specialization {
	
	protected readonly page: Page;
	
	protected constructor(page: Page) {
		this.page = page;
	}
	
	abstract apply(): Promise<void>;
}

async function getSpecialization(url: string, page: Page): Promise<Specialization | null> {
	if (url.includes('buyabans.com')) {
		const {default: AbansSpecialization} = await import('./AbansSpecialization');
		return new AbansSpecialization(page);
	}
	
	return null;
}

export { Specialization, getSpecialization };