#!/usr/bin/env node
/**
 * download_trademark_xml.js
 * Uses Puppeteer (headless Chrome) to download trademark assignment XML files
 * from the USPTO Open Data Portal (data.uspto.gov).
 *
 * The site is behind AWS WAF which requires browser JavaScript execution.
 * This script uses headless Chrome with anti-detection flags to:
 * 1. Navigate to the TRTDXFAG dataset page (passes WAF challenge)
 * 2. Capture the product API response (file listing with download URLs)
 * 3. Download the ZIP files using the authenticated browser session
 *
 * Usage: node download_trademark_xml.js --days-back 7 --output-dir output/business
 * Output: JSON to stdout with { success, files: [...], error? }
 */

const puppeteer = require('puppeteer-core');
const path = require('path');
const fs = require('fs');

// Parse CLI args
const args = process.argv.slice(2);
let daysBack = 7;
let outputDir = 'output/business';

for (let i = 0; i < args.length; i++) {
    if (args[i] === '--days-back' && args[i + 1]) daysBack = parseInt(args[i + 1]);
    if (args[i] === '--output-dir' && args[i + 1]) outputDir = args[i + 1];
}

const CHROME_PATH = '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';
const USPTO_PAGE = 'https://data.uspto.gov/bulkdata/datasets/trtdxfag';

async function main() {
    const log = (msg) => process.stderr.write(`PROGRESS: ${msg}\n`);
    const result = { success: false, files: [], error: null };

    fs.mkdirSync(outputDir, { recursive: true });

    let browser;
    try {
        log('Launching headless Chrome...');
        browser = await puppeteer.launch({
            executablePath: CHROME_PATH,
            headless: 'new',
            args: [
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-blink-features=AutomationControlled',
                '--window-size=1920,1080',
            ]
        });

        const page = await browser.newPage();
        await page.setViewport({ width: 1920, height: 1080 });
        await page.evaluateOnNewDocument(() => {
            Object.defineProperty(navigator, 'webdriver', { get: () => false });
        });
        await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36');

        // Capture the product API response during page load
        let productData = null;
        page.on('response', async (resp) => {
            const url = resp.url();
            if (url.includes('/ui/datasets/products/trtdxfag')) {
                try {
                    const ct = resp.headers()['content-type'] || '';
                    if (ct.includes('json')) {
                        productData = await resp.json();
                        log('Captured product API response');
                    }
                } catch (e) {}
            }
        });

        log('Navigating to USPTO TRTDXFAG dataset page...');
        await page.goto(USPTO_PAGE, { waitUntil: 'networkidle2', timeout: 60000 });

        // Wait for Angular to route and fetch data
        log('Waiting for file listing to load...');
        for (let i = 0; i < 15 && !productData; i++) {
            await new Promise(r => setTimeout(r, 2000));
        }

        if (!productData) {
            // Try fetching the API directly from the browser context
            // Use a wider range (60 days) to ensure we find files with data
            log('API not captured during page load, trying direct fetch...');
            const today = new Date();
            const fromDate = new Date(today);
            fromDate.setDate(fromDate.getDate() - Math.max(daysBack, 60));
            const fromStr = fromDate.toISOString().split('T')[0];
            const toStr = today.toISOString().split('T')[0];

            productData = await page.evaluate(async (from, to) => {
                try {
                    const url = `/ui/datasets/products/trtdxfag?includeFiles=true&fileDataFromDate=${from}&fileDataToDate=${to}`;
                    const resp = await fetch(url, { credentials: 'same-origin' });
                    const ct = resp.headers.get('content-type') || '';
                    if (!ct.includes('json')) return null;
                    return await resp.json();
                } catch (e) { return null; }
            }, fromStr, toStr);
        }

        if (!productData) {
            result.error = 'Could not fetch file listing from USPTO. The WAF may have blocked the request.';
            log(result.error);
            process.stdout.write(JSON.stringify(result));
            await browser.close();
            return;
        }

        // Extract file listing from API response
        const product = (productData.bulkDataProductBag || [])[0] || productData;
        const fileBag = product.productFileBag || {};
        const fileList = fileBag.fileDataBag || [];

        log(`Found ${fileList.length} files in dataset`);

        if (fileList.length === 0) {
            result.error = 'No files found in the TRTDXFAG dataset for the requested date range.';
            process.stdout.write(JSON.stringify(result));
            await browser.close();
            return;
        }

        // Filter to recent files matching our days-back criteria
        // Skip files <= 2000 bytes — these are "no data" skeletons with
        // <data-available-code>N</data-available-code> and no actual records
        const MIN_FILE_SIZE = 2000;
        const today = new Date();
        const cutoff = new Date(today);
        cutoff.setDate(cutoff.getDate() - daysBack);

        const allRecentFiles = fileList.filter(f => {
            const fileDate = new Date(f.fileDataFromDate || f.fileReleaseDate);
            return fileDate >= cutoff;
        });

        const emptyFiles = allRecentFiles.filter(f => (f.fileSize || 0) <= MIN_FILE_SIZE);
        const targetFiles = allRecentFiles.filter(f => (f.fileSize || 0) > MIN_FILE_SIZE);

        log(`Found ${allRecentFiles.length} files in last ${daysBack} days: ${targetFiles.length} with data, ${emptyFiles.length} empty (no data)`);

        // If all files in the requested range are empty, look further back
        if (targetFiles.length === 0) {
            log(`No files with data in last ${daysBack} days. Searching further back...`);
            const olderFiles = fileList
                .filter(f => {
                    const fileDate = new Date(f.fileDataFromDate || f.fileReleaseDate);
                    return fileDate < cutoff && (f.fileSize || 0) > MIN_FILE_SIZE;
                })
                .sort((a, b) => new Date(b.fileDataFromDate || b.fileReleaseDate) - new Date(a.fileDataFromDate || a.fileReleaseDate))
                .slice(0, 10);  // grab up to 10 most recent non-empty files

            if (olderFiles.length > 0) {
                log(`Found ${olderFiles.length} files with data from before the requested range`);
                targetFiles.push(...olderFiles);
            }
        }

        if (targetFiles.length === 0) {
            result.error = `No files with actual data found. All ${allRecentFiles.length} files in the last ${daysBack} days were empty (no trademark assignments published). Try increasing days back.`;
            log(result.error);
            process.stdout.write(JSON.stringify(result));
            await browser.close();
            return;
        }

        log(`Downloading ${targetFiles.length} files with data...`);

        // Download each file using the browser session (has WAF cookies)
        // Add delay between downloads to avoid HTTP 429 rate limiting
        for (let idx = 0; idx < targetFiles.length; idx++) {
            const fileInfo = targetFiles[idx];
            const filename = fileInfo.fileName;
            const downloadUrl = fileInfo.fileDownloadURI;
            const destPath = path.resolve(outputDir, filename);

            // Rate limit: wait 1s between downloads (skip for first file)
            if (idx > 0) {
                await new Promise(r => setTimeout(r, 1000));
            }

            log(`Downloading ${filename} (${fileInfo.fileSize} bytes)... [${idx + 1}/${targetFiles.length}]`);

            try {
                const dlResult = await page.evaluate(async (url) => {
                    try {
                        const resp = await fetch(url, { credentials: 'same-origin' });
                        if (!resp.ok) return { error: `HTTP ${resp.status}` };
                        const ct = resp.headers.get('content-type') || '';
                        const buf = await resp.arrayBuffer();
                        const bytes = new Uint8Array(buf);

                        // Verify it's a ZIP file (magic bytes PK)
                        if (bytes.length < 4) return { error: `Too small (${bytes.length}b)` };
                        const first4 = String.fromCharCode(...bytes.slice(0, 4));
                        if (!first4.startsWith('PK')) {
                            const preview = String.fromCharCode(...bytes.slice(0, 100));
                            if (preview.includes('<!doctype') || preview.includes('<html')) {
                                return { error: 'Got HTML page instead of file' };
                            }
                            return { error: `Not a ZIP file (starts with: ${first4})` };
                        }

                        // Convert to base64 for transfer to Node
                        let binary = '';
                        for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
                        return { data: btoa(binary), size: buf.byteLength };
                    } catch (e) {
                        return { error: e.message };
                    }
                }, downloadUrl);

                if (dlResult.error) {
                    log(`  ${filename}: ${dlResult.error}`);
                    // On rate limit, wait longer and retry once
                    if (dlResult.error.includes('429')) {
                        log(`  Rate limited, waiting 5s and retrying...`);
                        await new Promise(r => setTimeout(r, 5000));
                        const retry = await page.evaluate(async (url) => {
                            try {
                                const resp = await fetch(url, { credentials: 'same-origin' });
                                if (!resp.ok) return { error: `HTTP ${resp.status}` };
                                const buf = await resp.arrayBuffer();
                                const bytes = new Uint8Array(buf);
                                if (bytes.length < 4 || String.fromCharCode(...bytes.slice(0, 4)).indexOf('PK') !== 0) {
                                    return { error: 'Not a ZIP file on retry' };
                                }
                                let binary = '';
                                for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
                                return { data: btoa(binary), size: buf.byteLength };
                            } catch (e) { return { error: e.message }; }
                        }, downloadUrl);
                        if (retry.data) {
                            const buffer = Buffer.from(retry.data, 'base64');
                            fs.writeFileSync(destPath, buffer);
                            log(`  Retry saved ${filename} (${buffer.length} bytes)`);
                            result.files.push({ filename, path: destPath, size: buffer.length, url: downloadUrl, date: fileInfo.fileDataFromDate });
                        } else {
                            log(`  Retry also failed: ${retry.error}`);
                        }
                    }
                    continue;
                }

                if (dlResult.data) {
                    const buffer = Buffer.from(dlResult.data, 'base64');
                    fs.writeFileSync(destPath, buffer);
                    log(`  Saved ${filename} (${buffer.length} bytes)`);
                    result.files.push({
                        filename,
                        path: destPath,
                        size: buffer.length,
                        url: downloadUrl,
                        date: fileInfo.fileDataFromDate
                    });
                }
            } catch (e) {
                log(`  ${filename}: error - ${e.message}`);
            }
        }

        result.success = result.files.length > 0;
        if (!result.success) {
            result.error = 'Files were found but could not be downloaded. The USPTO site may have changed its download mechanism.';
        } else {
            log(`Successfully downloaded ${result.files.length} files`);
        }

    } catch (e) {
        result.error = e.message;
        log(`Fatal error: ${e.message}`);
    } finally {
        if (browser) await browser.close().catch(() => {});
    }

    process.stdout.write(JSON.stringify(result));
}

main().catch(e => {
    process.stdout.write(JSON.stringify({ success: false, files: [], error: e.message }));
    process.exit(1);
});
