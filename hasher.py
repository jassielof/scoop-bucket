import os
import json
import hashlib
import tempfile
import aiohttp
import asyncio
import ssl

BUCKET_DIR = os.path.join(os.path.dirname(__file__), "bucket")

def compute_hash(filepath):
    sha256 = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256.update(chunk)
    return sha256.hexdigest().upper()

async def download_file(url):
    tmp = tempfile.NamedTemporaryFile(delete=False)
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    async with aiohttp.ClientSession() as session:
        async with session.get(url, ssl=ssl_context) as resp:
            resp.raise_for_status()
            while True:
                chunk = await resp.content.read(8192)
                if not chunk:
                    break
                tmp.write(chunk)
    tmp.close()
    return tmp.name

async def process_url(url, manifest_path):
    try:
        tmp_file = await download_file(url)
        actual_hash = compute_hash(tmp_file)
        os.remove(tmp_file)
        return actual_hash
    except Exception as e:
        print(f"Error downloading {url} in {manifest_path}: {e}")
        return None

async def process_manifest(manifest_path):
    try:
        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest = json.load(f)
        urls = manifest.get("url")
        hashes = manifest.get("hash")
        if not urls:
            print(f"Skipping {manifest_path}: No 'url' field.")
            return

        # Normalize to list
        if isinstance(urls, str):
            urls = [urls]
        if isinstance(hashes, str) or hashes is None:
            hashes = [hashes] * len(urls)
        print(f"Checking {manifest_path} ...")

        tasks = [process_url(url, manifest_path) for url in urls]
        results = await asyncio.gather(*tasks)

        updated = False
        for i, actual_hash in enumerate(results):
            if actual_hash is None:
                continue
            manifest_hash = hashes[i] if i < len(hashes) else ""
            if not manifest_hash or manifest_hash.upper() != actual_hash:
                print(f"Updating hash for {manifest_path} [{i}]: {actual_hash}")
                hashes[i] = actual_hash
                updated = True
            else:
                print(f"Hash OK for {manifest_path} [{i}]")

        # Write back as string if only one url, else as array
        if len(urls) == 1:
            manifest["hash"] = hashes[0]
        else:
            manifest["hash"] = hashes

        if updated:
            with open(manifest_path, "w", encoding="utf-8") as f:
                json.dump(manifest, f, indent=4, ensure_ascii=False)
    except Exception as e:
        print(f"Error processing {manifest_path}: {e}")

async def main():
    tasks = []
    for filename in os.listdir(BUCKET_DIR):
        if filename.endswith(".json"):
            manifest_path = os.path.join(BUCKET_DIR, filename)
            tasks.append(process_manifest(manifest_path))
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
