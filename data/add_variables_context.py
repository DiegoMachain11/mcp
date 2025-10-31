#!/usr/bin/env python3
# augment_context_batched.py
import os, json, argparse, asyncio, time
from typing import List, Dict, Any
from openai import AsyncOpenAI, OpenAIError  # pip install openai
# --- helpers.py (or inline above gen_batch) ---
import json, re

SYSTEM_PROMPT = (
    "You are a dairy-analytics assistant. Given short Spanish metric names, "
    "produce a concise one-sentence context for dashboards. Be accurate, neutral, "
    "and domain-specific. Return ONLY JSON with a 'contexts' array of strings."
)

USER_TEMPLATE = (
    "Idioma de salida: {lang}\n"
    'Devuelve estrictamente: {{"contexts": ["..."]}}\n'
    "Reglas:\n"
    "• 1 oración por elemento (10–20 palabras).\n"
    "• No repitas la variable; explica el indicador en contexto de tablero KPI.\n"
    "• Mantén términos de lechería (lactación, DEL, etc.).\n"
    "• El orden y la cantidad de salidas deben coincidir con la entrada.\n\n"
    "Descripciones:\n{descriptions_json}\n"
)



def extract_json_object(text: str) -> dict:
    """
    Be forgiving: strip code fences and extract the first top-level JSON object.
    Raises ValueError if none found or parse fails.
    """
    if not text or not text.strip():
        raise ValueError("Empty completion content.")

    # remove common code fences
    t = text.strip()
    if t.startswith("```"):
        # remove triple backticks blocks
        t = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", t.strip())
        t = re.sub(r"\s*```$", "", t.strip())

    # sometimes models prepend "json"
    if t.lower().startswith("json"):
        t = t[4:].lstrip()

    # fast path: direct JSON
    try:
        return json.loads(t)
    except Exception:
        pass

    # fallback: extract first {...} blob (handles extra chatter)
    start = t.find("{")
    end = t.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError(f"No JSON object found. Preview: {t[:240].replace('\n', ' ')}")
    candidate = t[start:end+1]

    # try parse, if fails, try to fix common trailing commas
    try:
        return json.loads(candidate)
    except json.JSONDecodeError as e:
        # remove trailing commas (very common)
        candidate2 = re.sub(r",\s*([}\]])", r"\1", candidate)
        return json.loads(candidate2)  # may still raise; that's ok


def chunked(seq: List[Any], size: int) -> List[List[Any]]:
    return [seq[i : i + size] for i in range(0, len(seq), size)]


async def gen_batch(client: AsyncOpenAI, model: str, lang: str, batch: list, max_retries=4):

    user = USER_TEMPLATE.format(lang=lang, descriptions_json=json.dumps(batch, ensure_ascii=False))
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            print(f"Generating batch of {len(batch)} (attempt {attempt})...")
            resp = await client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user},
                ],
                # Some models don't support response_format; remove it to avoid silent failures
                # response_format={"type": "json_object"},
                max_completion_tokens=800,  # use this for newer models
            )

            print(resp)

            raw = (resp.choices[0].message.content or "").strip()

            data = extract_json_object(raw)  # <<< robust parsing
            contexts = data.get("contexts", [])
            if not isinstance(contexts, list) or len(contexts) != len(batch):
                raise ValueError(
                    f"Wrong shape. Expected {len(batch)}, got {len(contexts)}. Raw preview: {raw[:240].replace('\n',' ')}"
                )
            # Normalize each context to a single sentence line
            return [str(x).splitlines()[0].strip(' "')
                    for x in contexts]

        except Exception as e:
            last_err = e
            print(f"Batch of {len(batch)} failed: {e}")
            await asyncio.sleep(min(2**attempt, 8))  # backoff
    # preserve alignment
    return [f"[Context generation failed: {last_err}]" for _ in batch]


async def process_all(
    descriptions: List[str], *, model: str, lang: str, chunk_size: int, concurrency: int
) -> List[str]:
    client = AsyncOpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    batches = chunked(descriptions, chunk_size)
    print(f"Processing {len(descriptions)} descriptions in {len(batches)} batches.")

    semaphore = asyncio.Semaphore(concurrency)
    results: List[List[str]] = [None] * len(batches)

    async def worker(idx: int, batch: List[str]):
        async with semaphore:
            results[idx] = await gen_batch(client, model, lang, batch)

    await asyncio.gather(*(worker(i, b) for i, b in enumerate(batches)))
    # Flatten back
    flat: List[str] = [item for sub in results for item in sub]
    return flat


def main():
    ap = argparse.ArgumentParser(
        description="Batch-generate Context for each Description using OpenAI."
    )
    ap.add_argument("input", help="Path to input JSON with Description/Code arrays")
    ap.add_argument("-o", "--out", help="Path to write JSON with added Context")
    ap.add_argument(
        "--lang", default="English", help="Output language (e.g., English, Spanish)"
    )
    ap.add_argument(
        "--model",
        default=os.environ.get("OPENAI_MODEL", "gpt-4.1-mini"),
        help="Model name (default: gpt-4.1-mini or from OPENAI_MODEL)",
    )
    ap.add_argument(
        "--chunk-size",
        type=int,
        default=25,
        help="Descriptions per request (default: 25)",
    )
    ap.add_argument(
        "--concurrency", type=int, default=4, help="Concurrent requests (default: 4)"
    )
    args = ap.parse_args()

    if not os.environ.get("OPENAI_API_KEY"):
        raise SystemExit("Please set OPENAI_API_KEY.")

    with open(args.input, "r", encoding="utf-8") as f:
        data = json.load(f)

    desc = data.get("Description") or []
    codes = data.get("Code") or []
    if not isinstance(desc, list) or not isinstance(codes, list):
        raise ValueError("Input JSON must contain 'Description' and 'Code' arrays.")
    if len(desc) != len(codes):
        raise ValueError("Description and Code arrays must be same length.")

    contexts = asyncio.run(
        process_all(
            desc,
            model=args.model,
            lang=args.lang,
            chunk_size=args.chunk_size,
            concurrency=args.concurrency,
        )
    )

    out = {"Description": desc, "Code": codes, "Context": contexts}
    js = json.dumps(out, ensure_ascii=False, indent=4)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(js)
    else:
        print(js)


if __name__ == "__main__":
    main()
