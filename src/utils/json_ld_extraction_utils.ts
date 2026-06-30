import { Page } from 'playwright';
import logger from '../Logger';
import DatabaseUpsertQueue from '../db/DBUpsertQueue';

type JsonLdNode = Record<string, unknown>;

async function getJsonLdNodes(page: Page): Promise<JsonLdNode[]> {
    const scripts = await page.$$eval('script[type="application/ld+json"]', (els: HTMLScriptElement[]) =>
        els.map((s) => s.textContent ?? ''),
    );

    const nodes: JsonLdNode[] = [];
    for (const raw of scripts) {
        try {
            // Literal newlines/carriage-returns inside JSON string values are
            // invalid JSON (they must be escaped as \n). Normalise them to a
            // space before parsing so malformed but otherwise readable scripts
            // are not silently dropped.
            const sanitised = raw.replace(/[\r\n]/g, ' ');
            const data = JSON.parse(sanitised);
            if (Array.isArray(data)) {
                nodes.push(...data);
            } else {
                nodes.push(data);
                if (Array.isArray(data['@graph'])) {
                    nodes.push(...data['@graph']);
                }
            }
        } catch {
            // Malformed JSON-LD, skip
        }
    }
    return nodes;
}

function findProductNode(nodes: JsonLdNode[]): JsonLdNode | undefined {
    return nodes.find(
        (n) => typeof n === 'object' && n !== null && (n['@type'] === 'Product' || n['@type'] === 'ProductGroup'),
    );
}

export async function has_json_ld_schema(page: Page): Promise<boolean> {
    const nodes = await getJsonLdNodes(page);
    return nodes.length > 0;
}

export async function is_product_page(page: Page): Promise<boolean> {
    const nodes = await getJsonLdNodes(page);
    return findProductNode(nodes) !== undefined;
}

export async function extract_product_title(page: Page): Promise<string | null> {
    const nodes = await getJsonLdNodes(page);
    const product = findProductNode(nodes);
    if (!product) return null;

    const name = product['name'];
    return typeof name === 'string' ? cleanProductTitle(name) : null;
}

function extractPriceFromOffers(node: JsonLdNode): number | null {
    const offers = Array.isArray(node['offers']) ? node['offers'] : [node['offers']];

    for (const offer of offers) {
        if (typeof offer !== 'object' || offer === null) continue;
        const o = offer as JsonLdNode;

        // Try direct price field first
        if (o['price'] !== undefined) {
            const price = parseFloat(String(o['price']));
            if (!isNaN(price)) return price;
        }

        // Try priceSpecification array (e.g. UnitPriceSpecification)
        const specs = Array.isArray(o['priceSpecification']) ? o['priceSpecification'] : [o['priceSpecification']];
        for (const spec of specs) {
            if (typeof spec !== 'object' || spec === null) continue;
            const s = spec as JsonLdNode;
            const price = parseFloat(String(s['price']));
            if (!isNaN(price)) return price;
        }
    }

    return null;
}

export async function extract_product_price(page: Page): Promise<number | null> {
    const nodes = await getJsonLdNodes(page);
    const product = findProductNode(nodes);
    if (!product) return null;

    // For ProductGroup, collect prices from all hasVariant entries
    if (product['@type'] === 'ProductGroup' && Array.isArray(product['hasVariant'])) {
        const prices: number[] = [];
        for (const variant of product['hasVariant'] as JsonLdNode[]) {
            const price = extractPriceFromOffers(variant);
            if (price !== null) prices.push(price);
        }
        return prices.length > 0 ? Math.min(...prices) : null;
    }

    return extractPriceFromOffers(product);
}

/**
 * Strips site-branding suffixes from a product title.
 * e.g. "Logitech K380s | Best Prices in Sri Lanka | Xclusive" → "Logitech K380s"
 * Splits on " | " which sites use exclusively for branding suffixes.
 */
export function cleanProductTitle(raw: string): string {
    const pipeParts = raw.split(' | ');
    if (pipeParts.length > 1) {
        return pipeParts[0].trim();
    }

    return raw.trim();
}

/**
 * Resolves the best URL for a variant node.
 * Prefers @id; falls back to offers[].url when @id is absent.
 */
function resolveVariantUrl(variant: JsonLdNode, baseUrl: URL): string | null {
    if (typeof variant['@id'] === 'string') {
        return new URL(variant['@id'], baseUrl).href.split('#')[0];
    }

    const offers = Array.isArray(variant['offers']) ? variant['offers'] : [variant['offers']];
    for (const offer of offers) {
        if (typeof offer === 'object' && offer !== null) {
            const offerUrl = (offer as JsonLdNode)['url'];
            if (typeof offerUrl === 'string' && offerUrl.length > 0) {
                return new URL(offerUrl, baseUrl).href.split('#')[0];
            }
        }
    }

    return null;
}

async function upsertProductVariant(
    variantUrl: string,
    title: string,
    price: number,
    image: string | null,
): Promise<void> {
    await DatabaseUpsertQueue.markAsProductPage(variantUrl, 'DIRECTLY_INFERRED').catch((err) => {
        logger.error(
            `Error marking variant as product page ${variantUrl}: ${err instanceof Error ? err.message : String(err)}`,
        );
    });

    if (title.endsWith('...')) {
        logger.debug(`Variant title truncated, skipping title/price upsert for ${variantUrl}`);
        return;
    }

    logger.debug(`Upserting variant from JSON-LD for ${variantUrl}: title=${title}, price=${price}`);
    await DatabaseUpsertQueue.addProductTitleAndPrice(variantUrl, title, String(price)).catch((err) => {
        logger.error(
            `Error adding title/price for variant ${variantUrl}: ${err instanceof Error ? err.message : String(err)}`,
        );
    });

    if (image) {
        logger.debug(`Upserting variant image from JSON-LD for ${variantUrl}: image=${image}`);
        await DatabaseUpsertQueue.addProductImage(variantUrl, image).catch((err) => {
            logger.error(
                `Error adding image for variant ${variantUrl}: ${err instanceof Error ? err.message : String(err)}`,
            );
        });
    }
}

export async function extract_details_from_jsonld_schema(url: string, page: Page): Promise<void> {
    if (!(await has_json_ld_schema(page))) {
        logger.debug(`No JSON-LD schema found for ${url}`);
        return;
    }

    const nodes = await getJsonLdNodes(page);
    const product = findProductNode(nodes);

    if (!product) {
        logger.debug(`Not a product page for ${url}`);
        // await DatabaseUpsertQueue.markAsNotProductPage(url, 'DIRECTLY_INFERRED').catch((err) => {
        //     logger.error(
        //         `Error marking as not product page URL ${url}: ${err instanceof Error ? err.message : String(err)}`,
        //     );
        // });
        return;
    }

    // Mark the crawled page URL itself as a product page
    await DatabaseUpsertQueue.markAsProductPage(url, 'DIRECTLY_INFERRED').catch((err) => {
        logger.error(`Error marking as product page URL ${url}: ${err instanceof Error ? err.message : String(err)}`);
    });

    // ProductGroup with variants — upsert each variant individually
    if (product['@type'] === 'ProductGroup' && Array.isArray(product['hasVariant'])) {
        const baseUrl = new URL(url);
        for (const variant of product['hasVariant'] as JsonLdNode[]) {
            const variantUrl = resolveVariantUrl(variant, baseUrl);
            const variantName = typeof variant['name'] === 'string' ? cleanProductTitle(variant['name']) : null;
            const variantPrice = extractPriceFromOffers(variant);
            const variantImage: string | null = typeof variant['image'] === 'string' ? variant['image'] : null;

            if (!variantUrl || !variantName || variantPrice === null) {
                logger.debug(`Skipping variant with missing url, name, or price`);
                continue;
            }

            await upsertProductVariant(variantUrl, variantName, variantPrice, variantImage);
        }
        return;
    }

    // Single Product node
    const title = typeof product['name'] === 'string' ? cleanProductTitle(product['name']) : null;
    const price = extractPriceFromOffers(product);
    const image = typeof product['image'] === 'string' ? product['image'] : null;

    if (title && price !== null) {
        await upsertProductVariant(url, title, price, image);
    }
}
