import { PageStorage } from './PageStorage';

export default class LocalStorage extends PageStorage {
    async store(): Promise<void> {
        const pageHtml = await this.page.content();
        const pageImage = await this.page.screenshot({
            fullPage: true,
            type: 'jpeg',
            quality: 80,
        });

        const fs = require('fs');
        const safeFileName = this.url.replace(/[^a-z0-9]/gi, '_').toLowerCase();

        if (!fs.existsSync('pages/html')) {
            fs.mkdirSync('pages/html', { recursive: true });
        }

        if (!fs.existsSync('pages/images')) {
            fs.mkdirSync('pages/images', { recursive: true });
        }

        fs.writeFileSync(`pages/html/${safeFileName}.html`, pageHtml);
        fs.writeFileSync(`pages/images/${safeFileName}.jpg`, pageImage);
    }
}
