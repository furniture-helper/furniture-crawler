import { Page } from 'playwright';
import { Specialization } from './Specialization';

export default class CelltronicsSpecialization extends Specialization {
    public constructor(page: Page) {
        super(page);
    }

    async apply(): Promise<void> {
        await this.page.evaluate(() => {
            const suggestedProductsNav = document.querySelector('.wd-products-nav');
            if (suggestedProductsNav) {
                suggestedProductsNav.remove();
            }
        });
    }
}
