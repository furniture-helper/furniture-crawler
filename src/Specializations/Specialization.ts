import { Page } from 'playwright';

abstract class Specialization {
    protected readonly page: Page;

    protected constructor(page: Page) {
        this.page = page;
    }

    abstract apply(): Promise<void>;
}

async function getSpecialization(url: string, page: Page): Promise<Specialization | null> {
    if (url.includes('buyabans.com')) {
        const { default: AbansSpecialization } = await import('./AbansSpecialization');
        return new AbansSpecialization(page);
    }
    // if (url.includes('celltronics.lk')) {
    //     const { default: CelltronicsSpecialization } = await import('./CelltronicsSpecialization');
    //     return new CelltronicsSpecialization(page);
    // }
    // if (url.includes('ugreen.lk')) {
    //     const { default: UgreenSpecialization } = await import('./UgreenSpecialization');
    //     return new UgreenSpecialization(page);
    // }

    return null;
}

export { Specialization, getSpecialization };
