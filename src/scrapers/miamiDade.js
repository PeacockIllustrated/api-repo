import { Actor } from 'apify';
import { log } from 'crawlee';
import fs from 'fs';

// Helper for HTTP requests (using built-in fetch for Node 18+)
async function fetchJson(url) {
    const response = await fetch(url);
    if (!response.ok) {
        throw new Error(`HTTP Error ${response.status}: ${response.statusText}`);
    }
    return response.json();
}

export async function runScraper(input) {
    const arcgisUrl = input.arcgisUrl;
    if (!arcgisUrl) {
        throw new Error("ArcGIS FeatureServer URL is required for this source.");
    }

    log.info(`Starting Miami-Dade ArcGIS Scraper`, { arcgisUrl });

    // 1. Latency Check (Latest Booking)
    // Query: orderByFields=BookDate DESC, resultRecordCount=1
    const queryUrl = `${arcgisUrl}/query`;
    const latencyParams = new URLSearchParams({
        where: '1=1',
        orderByFields: 'BookDate DESC',
        resultRecordCount: '1',
        outFields: 'BookDate,ObjectId',
        f: 'json',
        returnGeometry: 'false'
    });

    try {
        log.info(`Checking latency...`);
        const latestData = await fetchJson(`${queryUrl}?${latencyParams.toString()}`);

        if (latestData.features && latestData.features.length > 0) {
            const latest = latestData.features[0].attributes;
            const bookDate = latest.BookDate; // Epoch MS
            const now = Date.now();
            const latencyMinutes = Math.round((now - bookDate) / 60000);

            log.info(`Latest Booking: ${new Date(bookDate).toISOString()} (Latenty: ${latencyMinutes}m)`);
            await Actor.setValue('LATENCY_METRIC', { latencyMinutes, lastBookDate: bookDate });
        } else {
            log.warning("Latency Check: No records found.");
        }
    } catch (e) {
        log.error(`Latency check failed: ${e.message}`);
    }

    // 2. Ingestion (Incremental)
    // Get last state
    const state = await Actor.getValue('STATE') || { lastObjectId: -1 };
    let lastObjectId = state.lastObjectId;
    log.info(`Resuming ingestion from ObjectId > ${lastObjectId}`);

    let hasMore = true;
    const batchSize = 100;

    while (hasMore) {
        const params = new URLSearchParams({
            where: `ObjectId > ${lastObjectId}`,
            orderByFields: 'ObjectId ASC',
            resultRecordCount: batchSize.toString(),
            outFields: '*', // Get all fields to map
            f: 'json',
            returnGeometry: 'false'
        });

        try {
            const batchUrl = `${queryUrl}?${params.toString()}`;
            log.info(`Fetching batch: ${batchUrl}`);
            const data = await fetchJson(batchUrl);

            if (data.error) {
                throw new Error(`ArcGIS Error: ${data.error.message}`);
            }

            const features = data.features || [];
            if (features.length === 0) {
                hasMore = false;
                log.info("No more records found.");
                break;
            }

            log.info(`Processing ${features.length} records...`);

            for (const feature of features) {
                const attrs = feature.attributes;

                // Normalization
                const record = {
                    source: "miami_dade_arcgis",
                    source_id: attrs.ObjectId,
                    booking_datetime: attrs.BookDate ? new Date(attrs.BookDate).toISOString() : null,
                    person_name: attrs.Defendant,
                    charges: [attrs.ChargeCode, attrs.ChargeDesc].filter(Boolean), // Simple aggregation
                    facility: "Miami-Dade Waiting/Jail", // Generic
                    details: {
                        dob: attrs.DOB ? new Date(attrs.DOB).toISOString() : null,
                        address: attrs.Address,
                        location: attrs.CityStateZip,
                        case_number: attrs.CaseNum,
                        raw: attrs // meaningful raw data
                    },
                    scraped_at: new Date().toISOString()
                };

                await Actor.pushData(record);

                // Update high-water mark
                if (attrs.ObjectId > lastObjectId) {
                    lastObjectId = attrs.ObjectId;
                }
            }

            // Save state after batch
            await Actor.setValue('STATE', { lastObjectId });

            // Rate limit (polite)
            await new Promise(r => setTimeout(r, 1000));

        } catch (e) {
            log.error(`Ingestion batch failed: ${e.message}`);
            hasMore = false; // Stop on error for now (or implement retry)
        }
    }

    log.info("Miami-Dade Ingestion Complete.");
}
