import type { IncomingMessage, ServerResponse } from 'node:http';
import { validateChatRequest } from '../src/lib/chatValidation';
import { createStreamAccumulator } from '../src/lib/streamParser';
import {
  extractGroundingSourcesFromChunk,
  generateChatStream,
  generateFollowUpsFallback,
  mapGeminiError,
} from '../server/gemini';
import { resolveUserLocationContext } from '../server/location';
import type { GroundingSource } from '../src/types/chat';

type VercelIncomingMessage = IncomingMessage & {
  body?: unknown;
};

function sendJson(res: ServerResponse, status: number, payload: Record<string, unknown>): void {
  if (res.writableEnded) return;
  res.statusCode = status;
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.end(JSON.stringify(payload));
}

function writeSseEvent(res: ServerResponse, payload: Record<string, unknown>): void {
  if (!res.writableEnded) {
    res.write(`data: ${JSON.stringify(payload)}\n\n`);
  }
}

async function readRequestJson(
  req: VercelIncomingMessage
): Promise<{ ok: true; data: unknown } | { ok: false; status: number; code: string; message: string }> {
  if (req.body !== undefined && req.body !== null) {
    if (typeof req.body === 'object' && !Buffer.isBuffer(req.body)) {
      return { ok: true, data: req.body };
    }
    const rawStr = Buffer.isBuffer(req.body) ? req.body.toString('utf8') : String(req.body);
    if (rawStr.length > 100_000) {
      return {
        ok: false,
        status: 413,
        code: 'PAYLOAD_TOO_LARGE',
        message: 'Request payload is too large. Please shorten your message or start a new chat.',
      };
    }
    try {
      return { ok: true, data: JSON.parse(rawStr) };
    } catch {
      return {
        ok: false,
        status: 400,
        code: 'INVALID_JSON',
        message: 'Request body must be valid JSON.',
      };
    }
  }

  let totalBytes = 0;
  const chunks: Buffer[] = [];
  try {
    for await (const chunk of req) {
      const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk));
      totalBytes += buf.length;
      if (totalBytes > 100_000) {
        return {
          ok: false,
          status: 413,
          code: 'PAYLOAD_TOO_LARGE',
          message: 'Request payload is too large. Please shorten your message or start a new chat.',
        };
      }
      chunks.push(buf);
    }
  } catch {
    return {
      ok: false,
      status: 400,
      code: 'BAD_REQUEST',
      message: 'Unable to read request body.',
    };
  }

  const rawText = Buffer.concat(chunks).toString('utf8');
  if (!rawText.trim()) {
    return {
      ok: false,
      status: 400,
      code: 'BAD_REQUEST',
      message: 'Request body is required.',
    };
  }

  try {
    return { ok: true, data: JSON.parse(rawText) };
  } catch {
    return {
      ok: false,
      status: 400,
      code: 'INVALID_JSON',
      message: 'Request body must be valid JSON.',
    };
  }
}

export default async function handler(
  req: VercelIncomingMessage,
  res: ServerResponse
): Promise<void> {
  try {
    if (req.method === 'OPTIONS') {
      res.statusCode = 204;
      res.end();
      return;
    }

    if (req.method !== 'POST') {
      sendJson(res, 405, {
        code: 'METHOD_NOT_ALLOWED',
        message: 'Method not allowed. Use POST /api/chat.',
        retryable: false,
      });
      return;
    }

    const parsedBody = await readRequestJson(req);
    if (!parsedBody.ok) {
      sendJson(res, parsedBody.status, {
        code: parsedBody.code,
        message: parsedBody.message,
        retryable: false,
      });
      return;
    }

    const validation = validateChatRequest(parsedBody.data);
    if (!validation.ok) {
      sendJson(res, 400, {
        code: 'VALIDATION_ERROR',
        message: validation.error,
        retryable: false,
      });
      return;
    }

    const abortController = new AbortController();
    res.on('close', () => {
      if (!res.writableEnded) {
        abortController.abort();
      }
    });

    res.statusCode = 200;
    res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
    res.setHeader('Cache-Control', 'no-cache, no-transform');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');

    const accumulator = createStreamAccumulator();
    const seenUris = new Set<string>();
    const sources: GroundingSource[] = [];

    try {
      const locationContext = await resolveUserLocationContext({
        headers: req.headers,
        remoteAddress: req.socket?.remoteAddress,
        clientContext: validation.data.clientContext,
      });

      const locationParts = [locationContext.city, locationContext.country].filter(Boolean);
      writeSseEvent(res, {
        type: 'context',
        timezone: locationContext.timezone,
        locationLabel: locationParts.length > 0 ? locationParts.join(', ') : undefined,
      });

      const { stream, model, fallbackSources } = await generateChatStream({
        message: validation.data.message,
        history: validation.data.history,
        responseLength: validation.data.responseLength,
        requestedModel: validation.data.model,
        locationContext,
        signal: abortController.signal,
      });

      for (const src of fallbackSources) {
        if (!seenUris.has(src.uri) && sources.length < 6) {
          seenUris.add(src.uri);
          sources.push(src);
        }
      }

      writeSseEvent(res, { type: 'meta', model });

      for await (const chunk of stream) {
        if (abortController.signal.aborted) {
          break;
        }
        extractGroundingSourcesFromChunk(chunk, seenUris, sources);

        const chunkText = chunk.text ?? '';
        if (!chunkText) continue;

        const delta = accumulator.push(chunkText);
        if (delta) {
          writeSseEvent(res, { type: 'delta', text: delta });
        }
      }

      if (!abortController.signal.aborted) {
        const finalized = accumulator.finalize();
        if (finalized.remainingDelta) {
          writeSseEvent(res, { type: 'delta', text: finalized.remainingDelta });
        }
        if (sources.length > 0) {
          writeSseEvent(res, { type: 'sources', sources });
        }

        let followUps = finalized.followUps;
        if (followUps.length === 0 && finalized.fullAnswer.length > 0) {
          followUps = await generateFollowUpsFallback({
            userMessage: validation.data.message,
            assistantAnswer: finalized.fullAnswer,
            requestedModel: model,
          });
        }

        writeSseEvent(res, { type: 'followups', questions: followUps });
        writeSseEvent(res, { type: 'done' });
      }
    } catch (err) {
      if (!abortController.signal.aborted) {
        const mapped = mapGeminiError(err);
        writeSseEvent(res, {
          type: 'error',
          code: mapped.code,
          message: mapped.message,
          retryable: mapped.retryable,
        });
      }
    } finally {
      if (!res.writableEnded) {
        res.end();
      }
    }
  } catch (outerErr) {
    const mapped = mapGeminiError(outerErr);
    if (!res.headersSent) {
      sendJson(res, mapped.status, {
        code: mapped.code,
        message: mapped.message,
        retryable: mapped.retryable,
      });
    } else if (!res.writableEnded) {
      writeSseEvent(res, {
        type: 'error',
        code: mapped.code,
        message: mapped.message,
        retryable: mapped.retryable,
      });
      res.end();
    }
  }
}
