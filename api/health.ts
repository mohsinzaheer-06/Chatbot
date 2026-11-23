import type { IncomingMessage, ServerResponse } from 'node:http';
import { resolveEligibleModel } from '../src/lib/chatValidation';
import { isApiKeyConfigured } from '../server/gemini';

export default async function handler(
  _req: IncomingMessage,
  res: ServerResponse
): Promise<void> {
  res.statusCode = 200;
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.end(
    JSON.stringify({
      ok: true,
      configured: isApiKeyConfigured(),
      defaultModel: resolveEligibleModel(undefined, process.env.GEMINI_MODEL),
    })
  );
}
