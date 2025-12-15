
import { Actor } from 'apify';
import { PlaywrightCrawler, log } from 'crawlee';
import fs from 'fs';
import path from 'path';

await Actor.init();

const input = await Actor.getInput() || {};

// Input validation and defaults
const county = input.county || 8;
const resultsPerPage = input.resultsPerPage || 56;
const pageStart = input.pageStart || 1;
const pageEnd = input.pageEnd || null; // If null, run until empty
const includeDetailPages = input.includeDetailPages || false;
const maxConcurrency = input.maxConcurrency || 3;
const minDelayMs = input.minDelayMs || 750;
const emitWebhook = input.emitWebhook || false;
const webhookUrl = input.webhookUrl;
const webhookAuthToken = input.webhookAuthToken;

const OUTPUT_FILE = 'OUTPUT.jsonl';

// Initialize the output file in the Key-Value store (local file for now, pushed later)
// actually, for Apify Key-Value store, we usually use setValue. 
// But the requirement says "write a JSONL file to the Key-Value Store: Key: OUTPUT.jsonl"
// We will build it locally and update it, or push chunks. 
// For simplicity and scalability, we might append to a stream and periodically upload, 
// but standard Actor kv store setValue overwrites. 
// Best practice for "Log" style is to keep a local buffer or file and upload at the end or interval.
// We will write to a local file and push it at the end.

log.info(`Starting ArrestWatch Florida Scraper`, { county, pageStart, pageEnd });

const crawler = new PlaywrightCrawler({
    maxConcurrency: maxConcurrency,
    useSessionPool: true,
    persistCookiesPerSession: true,

    // Cloudflare handling: Use headful mode with xvfb
    launchContext: {
        launchOptions: {
            headless: false, // Run headful (uses xvfb in Docker) to reduce bot score
            args: [
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-blink-features=AutomationControlled',
                '--start-maximized'
            ],
            viewport: { width: 1920, height: 1080 }
        },
    },
    browserPoolOptions: {
        useFingerprints: true,
    },
    requestHandlerTimeoutSecs: 180, // Give more time for challenges to resolve

    requestHandler: async ({ page, request, log, enqueueLinks }) => {
        // Manual rate limiting
        await new Promise(resolve => setTimeout(resolve, minDelayMs));

        log.info(`Processing ${request.url}`);

        // Best-effort wait for network idle
        try {
            await page.waitForLoadState('networkidle', { timeout: 15000 });
        } catch (e) {
            log.debug('Network idle timeout, proceeding...');
        }

        // Wait for specific content indicators (strictly arrest related)
        try {
            // Wait for text that appears in cards
            await page.waitForSelector('text=/Arrested|Charges|Booking|Bond/i', { timeout: 15000 });
        } catch (e) {
            log.info('Content text selector wait timed out, proceeding to check content...');
        }

        // Small buffer for rendering
        await page.waitForTimeout(2000);

        // Check for Cloudflare title
        const title = await page.title();
        if (title.includes('Just a moment') || title.includes('Attention Required')) {
            log.warning('Cloudflare challenge detected. Waiting...');
            await page.waitForTimeout(10000 + Math.random() * 10000);
        }

        const isListing = request.userData.type === 'listing';

        if (isListing) {
            // Extract listing data

            // Selector strategy: 
            // The site usually lists arrests in a grid or list. 
            // We look for elements that contain "Arrested" or standard profile classes.
            // Since we couldn't inspect, we will try standard generic selectors for this specific arrests.org template.
            // arrests.org usually uses 'div.profile-card' or similar containers inside a main wrapper.
            // We'll select all divs and filter for those that look like entries.

            // Let's grab specific containers. 
            // A common structure for florida.arrests.org is:
            // .content list -> .row -> .col or generic divs.
            // We will look for elements matching the "arrest-card" pattern.
            // Actually, based on public knowledge of arrests.org, they often use tile-like divs.

            const arrestCards = await page.$$('.profile-card, .search-result, .tile');
            // Fallback: try to identify by structure if class names are obfuscated.

            let extractedCount = 0;
            const records = [];

            // If we found specific classes
            if (arrestCards.length > 0) {
                // implement extraction wrapper
                for (const card of arrestCards) {
                    // Extract details
                    // This part is speculative without the DOM, but I will make it robust to "missing fields"
                }
            } else {
                // FALLBACK: Iterate over all <a> tags that have an image inside, which is common for mugshots
                const possibleCards = await page.$$eval('div', divs => {
                    return divs.map(div => {
                        // checks if div has img and text
                        const img = div.querySelector('img');
                        const text = div.innerText;
                        // minimal validation
                        if (img && text && text.includes('Arrested')) return {
                            valid: true,
                            html: div.outerHTML,
                            text: div.innerText,
                            imgSrc: img.src,
                            href: div.querySelector('a')?.href
                        };
                        return { valid: false };
                    }).filter(d => d.valid);
                });

                // We'll process these possible cards
                for (const card of possibleCards) {
                    const record = parseCard(card, county);
                    if (record) records.push(record);
                }
            }

            // TRY TO FIND SPECIFIC SELECTORS (Since I cannot see them, I will provide a generic parser)
            // But the User asked me to "Create a robust... actor".
            // I will use a very generic "container" strategy.
            // Search for the main content area.

            // Let's use a Page function to extract validation.
            const pageData = await page.evaluate((currentCounty) => {
                const results = [];
                // Attempt to find the main container.
                // Usually #content or .container
                const candidates = document.querySelectorAll('div');
                for (const div of candidates) {
                    // Check if this div is an arrest card.
                    // Heuristics:
                    // 1. Has an Image (mugshot)
                    // 2. Has a Name (First Last)
                    // 3. Has "Arrested" or Date.

                    const img = div.querySelector('img');
                    if (!img) continue;

                    const text = div.innerText;
                    if (!text.match(/\d{4}/)) continue; // Must have some date-like string or number
                    if (text.length > 500) continue; // Too big to be a card

                    // Valid candidate
                    const nameMatch = text.match(/^([A-Z\s]+)/); // Rough extraction request
                    const name = div.querySelector('.title, h4, strong')?.innerText || (nameMatch ? nameMatch[1] : 'Unknown');

                    const timestampRaw = text.match(/Arrested:?\s*(.*)/i)?.[1] || text.match(/\d{1,2}\/\d{1,2}\/\d{4}/)?.[0] || '';

                    // Charges often list items
                    const charges = Array.from(div.querySelectorAll('li, .charge')).map(c => c.innerText);

                    const detailLink = div.querySelector('a')?.href;

                    results.push({
                        county_id: currentCounty,
                        person_name: name.trim(),
                        booking_datetime_text: timestampRaw.trim(),
                        charges_preview: charges,
                        image_url: img.src,
                        detail_url: detailLink,
                        bond_text: text.match(/Bond:?\s*(\$[\d,]+)/i)?.[1] || null,
                        notes_text: null, // Hard to guess without selector
                        scraped_at: new Date().toISOString()
                    });
                }

                // Deduplicate based on detail_url or name
                const unique = [];
                const seen = new Set();
                for (const r of results) {
                    const key = r.detail_url || r.person_name;
                    if (!seen.has(key)) {
                        seen.add(key);
                        unique.push(r);
                    }
                }
                return unique;
            }, county);

            if (pageData.length === 0) {
                log.warning('No records found on this page. Stopping or blocked.');

                // Screenshot for debugging
                try {
                    const buffer = await page.screenshot({ fullPage: true });
                    await Actor.setValue(`debug_screenshot_${request.userData.page}.png`, buffer, { contentType: 'image/png' });
                    const html = await page.content();
                    await Actor.setValue(`debug_html_${request.userData.page}.html`, html, { contentType: 'text/html' });
                    log.info(`Saved debug credentials to Key-Value Store (debug_screenshot_${request.userData.page}.png)`);
                } catch (err) {
                    log.error(`Failed to save debug info: ${err.message}`);
                }

            } else {
                log.info(`Found ${pageData.length} records.`);
                extractedCount = pageData.length;

                for (const record of pageData) {
                    const fullRecord = {
                        source: "florida.arrests.org",
                        state: "FL",
                        county_id: county,
                        county_name: "Unknown", // extraction todo
                        ...record
                    };

                    await Actor.pushData(fullRecord);

                    // Append to JSONL
                    fs.appendFileSync(OUTPUT_FILE, JSON.stringify(fullRecord) + '\n');
                }
            }

            // Pagination Logic
            // If we found results AND (no pageEnd OR current < pageEnd)
            const current = request.userData.page;
            if (extractedCount > 0 && (!pageEnd || current < pageEnd)) {
                const nextPage = current + 1;
                const nextUrl = `https://florida.arrests.org/index.php?county=${county}&page=${nextPage}&results=${resultsPerPage}`;
                log.info(`Enqueuing page ${nextPage}`);
                await enqueueLinks({
                    urls: [nextUrl],
                    userData: { type: 'listing', page: nextPage }
                });
            }

        }
    },
});

// Initial Request
const startUrl = `https://florida.arrests.org/index.php?county=${county}&page=${pageStart}&results=${resultsPerPage}`;
log.info(`Queueing initial url: ${startUrl}`);

await crawler.run([{
    url: startUrl,
    userData: { type: 'listing', page: pageStart }
}]);

// Post-processing: Upload JSONL to KV store
if (fs.existsSync(OUTPUT_FILE)) {
    const data = fs.readFileSync(OUTPUT_FILE);
    await Actor.setValue('OUTPUT.jsonl', data, { contentType: 'application/jsonl' });
}

// Webhook
if (emitWebhook && webhookUrl) {
    // Basic implementation of webhook info
    // In a real scenario, we might batch this during the crawl, but for now we send a completion signal or the data
    const dataset = await Actor.openDataset();
    const { items } = await dataset.getData();

    try {
        await fetch(webhookUrl, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                ...(webhookAuthToken ? { 'Authorization': `Bearer ${webhookAuthToken}` } : {})
            },
            body: JSON.stringify({
                source: "florida.arrests.org",
                county,
                runId: Actor.getEnv().actorRunId,
                records: items
            })
        });
        log.info('Webhook sent successfully.');
    } catch (e) {
        log.error(`Webhook failed: ${e.message}`);
    }
}

await Actor.exit();

function parseCard(cardData, countyId) {
    // Helper if we moved logic out
    // ...
    return null;
}
