import express from 'express';
import path from 'path';
import cors from 'cors';
import { createLogger } from '@portside/shared';

const log = createLogger('dashboard');
const app = express();
const PORT = parseInt(process.env.DASHBOARD_PORT || '3000', 10);
const API_BASE = process.env.CONTROL_PLANE_API || 'http://localhost:7000';

app.use(cors());
app.use(express.json());

// Proxy API calls directly to control plane API to avoid CORS issues if opened in browser
app.use('/api', async (req, res) => {
  try {
    const targetUrl = `${API_BASE}${req.url}`;
    const response = await fetch(targetUrl, {
      method: req.method,
      headers: {
        'Content-Type': req.headers['content-type'] || 'application/json',
      },
      body: ['POST', 'PUT', 'PATCH'].includes(req.method) ? JSON.stringify(req.body) : undefined,
    });

    const data = await response.text();
    res.status(response.status);
    const contentType = response.headers.get('content-type');
    if (contentType) res.setHeader('content-type', contentType);
    res.send(data);
  } catch (err) {
    log.error('API proxy error', { error: (err as Error).message, url: req.url });
    res.status(502).json({ error: 'Control plane unreachable', message: (err as Error).message });
  }
});

// Serve static frontend assets
const publicDir = path.join(__dirname, '../public');
app.use(express.static(publicDir));

app.get('*', (_req, res) => {
  res.sendFile(path.join(publicDir, 'index.html'));
});

app.listen(PORT, () => {
  log.info(`Dashboard server running on http://localhost:${PORT}`);
});
