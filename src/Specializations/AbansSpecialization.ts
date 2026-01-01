import { Page } from 'playwright';
import { Specialization } from './Specialization';

export default class AbansSpecialization extends Specialization {
    public constructor(page: Page) {
        super(page);
    }

    async apply(): Promise<void> {
        await this.page.evaluate(() => {
            const abansCartOverlay = document.querySelector('.mini-cart');
            if (abansCartOverlay) {
                (abansCartOverlay as HTMLElement).style.display = 'none';
            }
        });
    }
}
