# ArrestWatch Florida Apify Actor

A robust Apify Actor to crawl public arrest data from [florida.arrests.org](https://florida.arrests.org/).

## Features
- **URL-based Pagination**: Iterates intelligently without relying on DOM buttons.
- **Cloudflare Handling**: Uses Playwright with stealth settings to navigate changes.
- **Resumable**: Designed to run on the Apify platform.
- **Configurable**: Supports custom counties, page ranges, and concurrency.
- **Dual Output**: Pushes to Apify Dataset and a `OUTPUT.jsonl` Key-Value store file.
- **Webhook Integration**: Optionally POSTs data to an endpoint upon completion.

## Input Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `county` | number | `8` | The numeric ID of the county (e.g. 8). |
| `resultsPerPage` | number | `56` | Results per page setting in URL. |
| `pageStart` | number | `1` | Starting page number. |
| `pageEnd` | number | `null` | Ending page number (null for auto-stop). |
| `maxConcurrency` | number | `3` | Parallel pages to process. |
| `minDelayMs` | number | `750` | Throttle delay. |
| `emitWebhook` | boolean | `false` | Enable webhook. |
| `webhookUrl` | string | - | URL for webhook. |
| `webhookAuthToken` | string | - | Bearer token. |

## Local Development

1. Install dependencies:
   ```bash
   npm install
   ```
2. Run the actor:
   ```bash
   npm start
   ```

## Output Structure
Records are saved with the following fields:
- `source`: "florida.arrests.org"
- `state`: "FL"
- `county_id`: number
- `person_name`: string
- `booking_datetime_text`: string
- `charges_preview`: array of strings
- `image_url`: string (mugshot)
- `detail_url`: string
- `scraped_at`: ISO timestamp

## Cloudflare Note
The target site uses aggressive Cloudflare protection. This actor uses `headless` Playwright which generally works, but if you encounter blocks, consider using Apify's residential proxies or `stealth` plugin configurations.
