# =============================================================================
# runners/download_trademarks.py
# Download trademark XML from USPTO bulk data (trtdxfag dataset)
# Uses a Puppeteer-based Node.js helper to bypass AWS WAF on data.uspto.gov
# =============================================================================
import subprocess
import logging
import json
import os
import zipfile
import re
from typing import Dict, List

logger = logging.getLogger(__name__)

# Path to the Node.js download helper (uses Puppeteer + headless Chrome)
DOWNLOAD_HELPER = os.path.join(os.path.dirname(__file__), '..', 'front-end', 'download_trademark_xml.js')


def run_trademark_download(config: Dict) -> Dict:
    """Download trademark XML from USPTO bulk data.

    Uses a headless Chrome browser (via Puppeteer) to navigate the USPTO
    Open Data Portal (data.uspto.gov), which is behind AWS WAF and requires
    JavaScript execution to access.

    Steps:
    1. Run Node.js helper to download ZIP files via headless Chrome
    2. Extract the XML from the ZIP files
    3. Combine into a single XML file

    Returns: { success, trademarks_downloaded, output_files }
    """
    output_dir = config.get('OUTPUT_DIR', 'output/business')
    os.makedirs(output_dir, exist_ok=True)

    days_back = config.get('days_back', 7)

    print(f"PROGRESS: Starting USPTO trademark download (headless Chrome)")
    logger.info(f"Starting trademark download from USPTO (days_back={days_back})")

    try:
        # Run the Node.js Puppeteer helper to handle WAF and download ZIPs
        helper_path = os.path.abspath(DOWNLOAD_HELPER)
        if not os.path.exists(helper_path):
            return {
                'success': False,
                'error': f'Download helper not found: {helper_path}'
            }

        abs_output_dir = os.path.abspath(output_dir)
        cmd = [
            'node', helper_path,
            '--days-back', str(days_back),
            '--output-dir', abs_output_dir
        ]

        print(f"PROGRESS: Launching headless Chrome to access USPTO...")
        logger.info(f"Running download helper: {' '.join(cmd)}")

        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,  # 5 minute timeout
            cwd=os.path.join(os.path.dirname(__file__), '..', 'front-end')
        )

        # Print progress from stderr (helper writes progress there)
        if proc.stderr:
            for line in proc.stderr.strip().split('\n'):
                if line.strip():
                    print(line.strip())
                    logger.info(line.strip())

        # Parse JSON result from stdout
        if not proc.stdout.strip():
            return {
                'success': False,
                'error': f'Download helper returned no output. Exit code: {proc.returncode}',
                'stderr': proc.stderr
            }

        try:
            result = json.loads(proc.stdout.strip())
        except json.JSONDecodeError:
            return {
                'success': False,
                'error': f'Download helper returned invalid JSON: {proc.stdout[:500]}',
                'stderr': proc.stderr
            }

        if not result.get('success') or not result.get('files'):
            return {
                'success': False,
                'error': result.get('error', 'No files were downloaded'),
                'details': result
            }

        # Process downloaded ZIP files
        downloaded_files = result['files']
        print(f"PROGRESS: Downloaded {len(downloaded_files)} ZIP files, extracting XML...")

        combined_xml_path = os.path.join(output_dir, 'downloaded_trademarks.xml')
        xml_files = []

        for file_info in downloaded_files:
            zip_path = file_info.get('path', '')
            if zip_path and os.path.exists(zip_path) and zip_path.endswith('.zip'):
                extracted = _extract_zip(zip_path, output_dir)
                xml_files.extend(extracted)

        if not xml_files:
            return {
                'success': False,
                'error': 'Downloaded ZIP files did not contain any XML files'
            }

        # Combine all XML files into one (or use the first one)
        if len(xml_files) == 1:
            os.rename(xml_files[0], combined_xml_path)
        else:
            _combine_xml_files(xml_files, combined_xml_path)

        print(f"PROGRESS: Download complete - {len(downloaded_files)} files processed")

        return {
            'success': True,
            'mode': 'auto',
            'trademarks_downloaded': len(downloaded_files),
            'files_downloaded': downloaded_files,
            'xml_files_extracted': len(xml_files),
            'output_files': {
                'xml': combined_xml_path
            }
        }

    except subprocess.TimeoutExpired:
        logger.error("Download helper timed out after 5 minutes")
        return {
            'success': False,
            'error': 'Download timed out after 5 minutes. The USPTO site may be slow or unavailable.'
        }
    except Exception as e:
        logger.error(f"Trademark download failed: {e}")
        return {
            'success': False,
            'error': str(e)
        }



def _extract_zip(zip_path: str, output_dir: str) -> List[str]:
    """Extract XML files from a ZIP archive."""
    extracted_files = []

    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            for name in zf.namelist():
                if name.lower().endswith('.xml'):
                    extracted_path = os.path.join(output_dir, name)
                    zf.extract(name, output_dir)
                    extracted_files.append(extracted_path)
                    logger.info(f"Extracted: {name}")
    except zipfile.BadZipFile:
        logger.warning(f"Bad ZIP file: {zip_path}")
    except Exception as e:
        logger.warning(f"Error extracting {zip_path}: {e}")

    return extracted_files


def _combine_xml_files(xml_files: List[str], output_path: str):
    """Combine multiple XML files into one."""
    with open(output_path, 'w', encoding='utf-8') as out:
        out.write('<?xml version="1.0" encoding="UTF-8"?>\n<trademark-files>\n')
        for xml_file in xml_files:
            try:
                with open(xml_file, 'r', encoding='utf-8', errors='replace') as f:
                    content = f.read()
                    # Strip XML declarations
                    content = re.sub(r'<\?xml[^?]*\?>', '', content)
                    # Strip inline DTD declarations (can span many lines)
                    content = re.sub(r'<!DOCTYPE[^[>]*\[.*?\]>', '', content, flags=re.DOTALL)
                    content = re.sub(r'<!DOCTYPE[^>]*>', '', content)
                    out.write(content)
                    out.write('\n')
            except Exception as e:
                logger.warning(f"Error reading {xml_file}: {e}")
        out.write('</trademark-files>\n')
