import { Actor } from 'apify';
import { log } from 'crawlee';
import * as floridaScraper from './scrapers/floridaArrests.js';
import * as miamiScraper from './scrapers/miamiDade.js';

await Actor.init();

const input = await Actor.getInput() || {};
const dataSource = input.dataSource || 'florida.arrests.org';

log.info(`Selected Data Source: ${dataSource}`);

try {
    if (dataSource === 'florida.arrests.org') {
        await floridaScraper.runScraper(input);
    } else if (dataSource === 'miami-dade-arcgis') {
        await miamiScraper.runScraper(input);
    } else {
        throw new Error(`Unknown data source: ${dataSource}`);
    }
} catch (e) {
    log.error(`Actor Run Failed: ${e.message}`);
    process.exit(1);
}

await Actor.exit();
