#!/usr/bin/env python3
"""
hormozi_pipeline.py — All-in-one pipeline for ingesting Alex Hormozi's
YouTube channel and distilling it into a dense, Bully-AI-injectable corpus.

What this does:
  1. Uses yt-dlp to enumerate every video on @AlexHormozi
  2. Downloads YouTube auto-generated captions (no Whisper compute needed)
  3. Chunks transcripts by video, with metadata (title, URL, duration)
  4. Sends batches through Claude to extract dense frameworks:
       - Offer architecture (value stack, grand slam offers)
       - Pricing psychology (anchor, decoy, premium positioning)
       - Sales scripts (objection handling, closing, urgency, scarcity)
       - Mindset / business ops (delegation, hiring, leverage)
  5. Outputs a single ~15K-token "frameworks_bible.md" file
  6. Uploads that file to the Render persistent disk at /var/data/hormozi/
     (or saves locally if no Render disk available)

How to run:
  # Required env vars:
  export ANTHROPIC_API_KEY=sk-ant-...

  # Optional: change the channel
  export YT_CHANNEL=https://www.youtube.com/@AlexHormozi/videos

  # Optional: cap videos for testing
  export MAX_VIDEOS=50

  # Optional: where to save output (default = /var/data/hormozi if exists, else ./hormozi)
  export HORMOZI_OUT_DIR=/var/data/hormozi

  pip install yt-dlp anthropic
  python3 hormozi_pipeline.py

Time + cost:
  - Hormozi has ~600 videos, ~150 hrs of content
  - Captions download: ~30 min (rate-limited by YouTube)
  - Distillation through Claude: ~10 min, ~$5-7 one-time
  - Output: 1 markdown file, ~15K tokens, ~60KB on disk

Re-running:
  - Idempotent — caches transcripts to disk so re-runs only fetch new videos
  - The distilled bible is overwritten each run; keep a backup if you've manually edited
"""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("hormozi_pipeline")


# ─── Config ─────────────────────────────────────────────────────────────────

CHANNEL_URL = os.getenv("YT_CHANNEL", "https://www.youtube.com/@AlexHormozi/videos")
MAX_VIDEOS = int(os.getenv("MAX_VIDEOS", "0"))  # 0 = no cap
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-6")

# Where we save raw transcripts and the final distilled bible
def _default_out_dir() -> Path:
    if Path("/var/data").exists() and os.access("/var/data", os.W_OK):
        return Path("/var/data/hormozi")
    return Path(__file__).resolve().parent.parent / "hormozi_data"


OUT_DIR = Path(os.getenv("HORMOZI_OUT_DIR", str(_default_out_dir())))
RAW_DIR = OUT_DIR / "raw_transcripts"
BIBLE_PATH = OUT_DIR / "frameworks_bible.md"
INDEX_PATH = OUT_DIR / "video_index.json"

# Distillation batching — how many videos per Claude call
BATCH_VIDEOS = int(os.getenv("BATCH_VIDEOS", "30"))


# ─── Step 1: Enumerate videos via yt-dlp ─────────────────────────────────────

def enumerate_videos(channel_url: str, max_videos: int = 0) -> list[dict]:
    """Use yt-dlp to list every video on the channel (metadata only, fast)."""
    logger.info("Enumerating videos from %s ...", channel_url)
    cmd = [
        "yt-dlp",
        "--flat-playlist",
        "--print", "%(id)s\t%(title)s\t%(duration)s",
        "--skip-download",
        channel_url,
    ]
    if max_videos > 0:
        cmd.insert(-1, "--playlist-end")
        cmd.insert(-1, str(max_videos))

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    if result.returncode != 0:
        logger.error("yt-dlp enumeration failed: %s", result.stderr[:500])
        return []

    videos = []
    for line in result.stdout.strip().split("\n"):
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        vid_id, title = parts[0], parts[1]
        duration = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
        videos.append({
            "id": vid_id,
            "title": title,
            "duration_sec": duration,
            "url": f"https://www.youtube.com/watch?v={vid_id}",
        })
    logger.info("Found %d videos.", len(videos))
    return videos


# ─── Step 2: Download captions ───────────────────────────────────────────────

def download_captions(video: dict, out_dir: Path) -> str:
    """Download auto-captions for one video and return the cleaned text."""
    vid_id = video["id"]
    target = out_dir / f"{vid_id}.txt"
    if target.exists():
        return target.read_text()

    # Use yt-dlp to grab auto-captions in vtt format
    cmd = [
        "yt-dlp",
        "--write-auto-subs",
        "--sub-langs", "en",
        "--skip-download",
        "--sub-format", "vtt",
        "-o", str(out_dir / "%(id)s.%(ext)s"),
        video["url"],
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        logger.warning("Caption download failed for %s: %s", vid_id, result.stderr[:200])
        return ""

    # yt-dlp writes <vid_id>.en.vtt — parse it to plain text
    vtt_path = out_dir / f"{vid_id}.en.vtt"
    if not vtt_path.exists():
        return ""
    text = _vtt_to_plain(vtt_path.read_text())
    target.write_text(text)
    vtt_path.unlink()  # remove the raw vtt to keep disk usage down
    return text


def _vtt_to_plain(vtt: str) -> str:
    """Strip VTT timestamps + tags, dedupe consecutive duplicate lines."""
    lines = []
    last_line = ""
    for line in vtt.split("\n"):
        # Skip headers, timing lines, blank lines
        if (not line.strip()
                or line.startswith(("WEBVTT", "Kind:", "Language:", "NOTE"))
                or "-->" in line
                or re.match(r"^\d{2}:", line)):
            continue
        # Strip inline tags like <00:00:01.500><c>...</c>
        clean = re.sub(r"<[^>]+>", "", line).strip()
        if clean and clean != last_line:
            lines.append(clean)
            last_line = clean
    return " ".join(lines)


# ─── Step 3: Distill batches through Claude ─────────────────────────────────

DISTILL_PROMPT = """You are extracting Alex Hormozi's most actionable frameworks from raw video transcripts. Your output will be injected into another AI assistant's system prompt to teach it how to sell, close, and handle objections like Hormozi.

Below is a batch of transcripts from Alex Hormozi's YouTube channel. Read them and extract:

1. **OFFER ARCHITECTURE**: How he thinks about value stacks, grand slam offers, dream outcomes, perceived likelihood of achievement, time delay, effort & sacrifice. Pull verbatim quotes when they're punchy.

2. **PRICING PSYCHOLOGY**: Anchor pricing, premium positioning, the more-you-pay-the-more-you-value principle, charm pricing, payment plans vs. pay-in-full positioning.

3. **SALES SCRIPTS**: Objection handling for "too expensive", "let me think about it", "I need to talk to my spouse", "send me more info", "now's not a good time". Verbatim closes when found.

4. **URGENCY & SCARCITY**: How he creates real urgency without lying. Bonus stacking. Decay of value. Cohort closes.

5. **MINDSET / BUSINESS OPS**: Volume negates luck, the most valuable skill, how to think about time, leverage principles.

For each framework, write 2-4 dense sentences PLUS a verbatim Hormozi quote when one is in the source. NO fluff. NO "Hormozi talks about..." narration — write it as DIRECT INSTRUCTIONS that a sales AI can use.

Format the output as Markdown with H2 sections matching the 5 categories above. Inside each H2, use H3 for sub-frameworks. Aim for ~3000 tokens of output per batch — dense, copyable, immediately usable.

=== TRANSCRIPTS BATCH ===
{batch}
=== END BATCH ===

Now extract the frameworks. Be ruthless about cutting padding."""


def distill_batch(batch_text: str, api_key: str) -> str:
    """Send one batch of transcripts to Claude and return the distilled markdown."""
    from anthropic import Anthropic
    client = Anthropic(api_key=api_key)
    resp = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=4000,
        messages=[{
            "role": "user",
            "content": DISTILL_PROMPT.format(batch=batch_text[:160000]),  # ~40K tokens cap
        }],
    )
    return resp.content[0].text


# ─── Step 4: Final synthesis pass ───────────────────────────────────────────

SYNTHESIS_PROMPT = """You are creating the FINAL "Hormozi Frameworks Bible" — a single dense Markdown document that an AI sales agent will load into its system prompt for EVERY customer message.

Below are partial extractions from many batches of Hormozi videos. Some content is duplicated, some contradicts itself slightly, some is gold and some is filler.

Your job:
1. Merge duplicates into the cleanest single statement
2. Drop filler / generic advice
3. Keep ONLY frameworks that an AI agent would actually USE in a sales conversation (offer building, pricing, closing, objections, urgency)
4. Preserve verbatim Hormozi quotes — they carry weight
5. Organize into 5 H2 sections: OFFER ARCHITECTURE, PRICING PSYCHOLOGY, SALES SCRIPTS & OBJECTION HANDLING, URGENCY & SCARCITY, MINDSET. Each H2 should have 4-8 H3 sub-frameworks.
6. Target: ~10-15K tokens of dense, immediately-usable instruction.

=== RAW EXTRACTIONS ===
{extractions}
=== END EXTRACTIONS ===

Output ONLY the final Bible markdown, no preamble. Start with `# Hormozi Frameworks Bible` and go straight into H2 sections."""


def synthesize_bible(batch_outputs: list[str], api_key: str) -> str:
    """Merge all batch outputs into one final dense bible."""
    from anthropic import Anthropic
    client = Anthropic(api_key=api_key)
    joined = "\n\n---\n\n".join(batch_outputs)
    resp = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=16000,
        messages=[{
            "role": "user",
            "content": SYNTHESIS_PROMPT.format(extractions=joined[:200000]),
        }],
    )
    return resp.content[0].text


# ─── Main pipeline ──────────────────────────────────────────────────────────

def run_pipeline():
    if not ANTHROPIC_API_KEY:
        logger.error("ANTHROPIC_API_KEY not set — aborting")
        sys.exit(2)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Step 1: enumerate
    videos = enumerate_videos(CHANNEL_URL, MAX_VIDEOS)
    if not videos:
        logger.error("No videos found — aborting")
        sys.exit(3)

    INDEX_PATH.write_text(json.dumps(videos, indent=2))
    logger.info("Saved video index to %s", INDEX_PATH)

    # Step 2: download captions
    logger.info("Downloading captions for %d videos...", len(videos))
    transcripts = []  # [{video_meta, text}]
    for i, vid in enumerate(videos, 1):
        try:
            text = download_captions(vid, RAW_DIR)
            if text:
                transcripts.append({"video": vid, "text": text})
            if i % 25 == 0:
                logger.info("  fetched %d/%d", i, len(videos))
            time.sleep(0.3)  # be polite to YouTube
        except Exception as e:
            logger.warning("Skipping %s: %s", vid["id"], e)
    logger.info("Successfully fetched %d transcripts", len(transcripts))

    if not transcripts:
        logger.error("No transcripts retrieved — aborting before distill step")
        sys.exit(4)

    # Step 3: distill in batches
    logger.info("Distilling %d transcripts in batches of %d...", len(transcripts), BATCH_VIDEOS)
    batch_outputs = []
    for batch_idx, start in enumerate(range(0, len(transcripts), BATCH_VIDEOS), 1):
        batch = transcripts[start:start + BATCH_VIDEOS]
        batch_text = "\n\n".join(
            f"### VIDEO: {t['video']['title']} ({t['video']['url']})\n{t['text']}"
            for t in batch
        )
        try:
            out = distill_batch(batch_text, ANTHROPIC_API_KEY)
            batch_outputs.append(out)
            logger.info("  batch %d done (%d chars distilled)", batch_idx, len(out))
            # save intermediate so we can recover if something blows up
            (OUT_DIR / f"batch_{batch_idx:03d}.md").write_text(out)
        except Exception as e:
            logger.exception("Batch %d failed: %s", batch_idx, e)

    if not batch_outputs:
        logger.error("All distill batches failed")
        sys.exit(5)

    # Step 4: synthesize final bible
    logger.info("Synthesizing final Hormozi Frameworks Bible...")
    bible = synthesize_bible(batch_outputs, ANTHROPIC_API_KEY)
    BIBLE_PATH.write_text(bible)
    logger.info("✅ Bible written to %s (%d chars)", BIBLE_PATH, len(bible))

    # Cleanup intermediates
    for p in OUT_DIR.glob("batch_*.md"):
        p.unlink()

    print(f"\nDone. Frameworks Bible: {BIBLE_PATH}")
    print(f"Raw transcripts: {RAW_DIR}")
    print(f"Video index: {INDEX_PATH}")


if __name__ == "__main__":
    run_pipeline()
