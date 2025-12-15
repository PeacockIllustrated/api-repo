import { Actor } from 'apify';
import { PlaywrightCrawler, log } from 'crawlee';
import fs from 'fs';

export async function runScraper(input) {
    const county = input.county || 8;
    const resultsPerPage = input.resultsPerPage || 56;
    const pageStart = input.pageStart || 1;
    const pageEnd = input.pageEnd || null; // If null, run until empty
    const maxConcurrency = input.maxConcurrency || 3;
    const minDelayMs = input.minDelayMs || 750;
    const emitWebhook = input.emitWebhook || false;
    const webhookUrl = input.webhookUrl;
    const webhookAuthToken = input.webhookAuthToken;

    const OUTPUT_FILE = 'OUTPUT.jsonl';

    log.info(`Starting ArrestWatch Florida Scraper`, { county, pageStart, pageEnd });

    const proxyConfiguration = await Actor.createProxyConfiguration();

    const crawler = new PlaywrightCrawler({
        proxyConfiguration,
        maxConcurrency: maxConcurrency,
        useSessionPool: true,
        persistCookiesPerSession: false, // DISABLE this to avoid carrying over "blocked" cookies

        // IMPORTANT: Cloudflare often returns 403 for challenges. 
        // We must tell Crawlee NOT to treat 403 as a failure so we can handle it in the requestHandler.
        sessionPoolOptions: {
            blockedStatusCodes: [],
        },

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

        requestHandler: async ({ page, request, log, enqueueLinks, requestQueue, session }) => {
            // Manual rate limiting
            await new Promise(resolve => setTimeout(resolve, minDelayMs));

            log.info(`Processing ${request.url}`);

            // Wait for Cloudflare challenge to potentially pass
            // Active interaction to trigger human checks
            try {
                await page.waitForLoadState('networkidle', { timeout: 30000 });

                const title = await page.title();
                if (title.includes('Just a moment') || title.includes('Attention Required')) {
                    log.warning('Cloudflare challenge detected. Initiating interaction...');

                    // Try up to 3 times to interact
                    for (let i = 0; i < 3; i++) {
                        // 1. Mouse movements
                        await page.mouse.move(100 + Math.random() * 200, 200 + Math.random() * 200);
                        await page.waitForTimeout(1000 + Math.random() * 2000);

                        // 2. Click if "bound" to a specific location (heuristics)
                        await page.mouse.down();
                        await page.waitForTimeout(50);
                        await page.mouse.up();

                        // 3. Search for Cloudflare iframe/checkbox
                        const frames = page.frames();
                        for (const frame of frames) {
                            try {
                                const checkbox = await frame.$('input[type="checkbox"], label.ctp-checkbox-label, #challenge-stage div');
                                if (checkbox) {
                                    const box = await checkbox.boundingBox();
                                    if (box) {
                                        log.info('Found potential challenge checkbox/div. Clicking...');
                                        await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
                                        await page.mouse.down();
                                        await page.waitForTimeout(50 + Math.random() * 100);
                                        await page.mouse.up();
                                    }
                                }
                            } catch (err) {
                                // Ignore frame access errors
                            }
                        }

                        // Check if passed
                        const newTitle = await page.title();
                        if (!newTitle.includes('Just a moment') && !newTitle.includes('Attention Required')) {
                            log.info('Challenge appeared to resolve.');
                            break;
                        }
                    }
                }
            } catch (e) {
                log.warning(`Wait load state warning: ${e.message}`);
            }

            const isListing = request.userData.type === 'listing';

            if (isListing) {
                // Check if we are still on a Cloudflare page
                const title = await page.title();
                const content = await page.content();

                if (title.includes('Just a moment') || title.includes('Attention Required') || content.includes('challenge-platform')) {
                    log.error('Request blocked by Cloudflare (Challenge persists). Retiring session.');
                    session.retire(); // Mark this IP/Session as bad
                    throw new Error('Blocked by Cloudflare - Retrying with new session'); // Force retry
                }

                // Extract listing data
                const arrestCards = await page.$$('.profile-card, .search-result, .tile');

                let extractedCount = 0;
                const records = [];

                if (arrestCards.length > 0) {
                    // implement extraction wrapper (Placeholder for explicit extraction)
                } else {
                    // FALLBACK: Iterate over all <a> tags that have an image inside, which is common for mugshots
                    const possibleCards = await page.$$eval('div', divs => {
                        return divs.map(div => {
                            const img = div.querySelector('img');
                            const text = div.innerText;
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

                    for (const card of possibleCards) {
                        const record = parseCard(card, county); // Logic below
                        if (record) records.push(record);
                    }
                }

                // Page-level extraction fallback
                const pageData = await page.evaluate((currentCounty) => {
                    const results = [];
                    const candidates = document.querySelectorAll('div');
                    for (const div of candidates) {
                        const img = div.querySelector('img');
                        if (!img) continue;

                        const text = div.innerText;
                        if (!text.match(/\d{4}/)) continue;
                        if (text.length > 500) continue;

                        const nameMatch = text.match(/^([A-Z\s]+)/);
                        const name = div.querySelector('.title, h4, strong')?.innerText || (nameMatch ? nameMatch[1] : 'Unknown');

                        const timestampRaw = text.match(/Arrested:?\s*(.*)/i)?.[1] || text.match(/\d{1,2}\/\d{1,2}\/\d{4}/)?.[0] || '';

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
                            scraped_at: new Date().toISOString()
                        });
                    }
                    // Deduplicate
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
                    try {
                        const buffer = await page.screenshot({ fullPage: true });
                        await Actor.setValue(`debug_screenshot_${request.userData.page}.png`, buffer, { contentType: 'image/png' });
                        const html = await page.content();
                        await Actor.setValue(`debug_html_${request.userData.page}.html`, html, { contentType: 'text/html' });
                        log.info(`Saved debug credentials to Key-Value Store (debug_screenshot_${request.userData.page}.png)`);
                    } catch (err) {
                        log.error(`Failed to save debug info: ${err.message}`);
                    }

                    if (extractedCount === 0) { // Should check pageData.length here
                        session.retire();
                        throw new Error('No records found - likely blocked or layout mismatch. Retrying.');
                    }
                } else {
                    log.info(`Found ${pageData.length} records.`);
                    extractedCount = pageData.length;

                    for (const record of pageData) {
                        const fullRecord = {
                            source: "florida.arrests.org",
                            state: "FL",
                            county_id: county,
                            county_name: "Unknown",
                            ...record
                        };

                        await Actor.pushData(fullRecord);
                        fs.appendFileSync(OUTPUT_FILE, JSON.stringify(fullRecord) + '\n');
                    }
                }

                // Pagination Logic
                const current = request.userData.page;
                if (extractedCount > 0 && (!pageEnd || current < pageEnd)) {
                    const nextPage = current + 1;
                    const nextUrl = `https://florida.arrests.org/index.php?county=${county}&page=${nextPage}&results=${resultsPerPage}`;
                    log.info(`Enqueuing page ${nextPage}`);
                    await requestQueue.addRequest({
                        url: nextUrl,
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

    // Post-processing
    if (fs.existsSync(OUTPUT_FILE)) {
        const data = fs.readFileSync(OUTPUT_FILE);
        await Actor.setValue('OUTPUT.jsonl', data, { contentType: 'application/jsonl' });
    }
}

function parseCard(cardData, countyId) {
    // Placeholder for non-page-evaluate parsing if needed
    return null;
}
