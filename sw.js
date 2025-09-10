/**
 * IPFS Transparent Proxy Service Worker
 * Implements transparent request interception and routing to IPFS gateways
 */

// ============================================================================
// LOGGER MODULE
// ============================================================================
class Logger {
  constructor(name = 'SW-CGI', level = 'INFO') {
    this.name = name;
    this.level = level;
    this.levels = { ERROR: 0, WARN: 1, INFO: 2, DEBUG: 3 };
    this.colors = {
      ERROR: '#ff4757',
      WARN: '#ffa502',
      INFO: '#3742fa',
      DEBUG: '#747d8c'
    };
  }

  _log(level, ...args) {
    if (this.levels[level] > this.levels[this.level]) return;
    
    const timestamp = new Date().toISOString().slice(11, 23);
    const prefix = `%c[${this.name}] %c${timestamp} %c[${level}]`;
    const styles = [
      `color: #2f3542; font-weight: bold;`,
      `color: #57606f; font-size: 0.9em;`,
      `color: ${this.colors[level]}; font-weight: bold;`
    ];
    
    console.log(prefix, ...styles, ...args);
  }

  error(...args) { this._log('ERROR', ...args); }
  warn(...args) { this._log('WARN', ...args); }
  info(...args) { this._log('INFO', ...args); }
  debug(...args) { this._log('DEBUG', ...args); }
}

const logger = new Logger('SW-CGI', 'INFO');

// ============================================================================
// CONFIGURATION MODULE
// ============================================================================
class ConfigManager {
  constructor() {
    this.defaultConfig = {
      localGateway: 'http://localhost:8080',
      publicGateways: [
        'https://ipfs.imwolf.ink',
        'https://ipfs.io',
        'https://dweb.link',
      ],
      timeouts: {
        local: 1000,      // 1 second for local gateway
        public: 10000     // 10 seconds for public gateways
      },
      cache: {
        version: 'sw-cache-v1',
        ttl: {
          html: 15 * 60 * 1000,        // 15 minutes
          css: 72 * 60 * 60 * 1000,    // 72 hours
          js: 72 * 60 * 60 * 1000,     // 72 hours
          image: 72 * 60 * 60 * 1000,  // 72 hours
          font: 72 * 60 * 60 * 1000,   // 72 hours
          default: 6 * 60 * 60 * 1000  // 6 hour
        }
      },
      maxConcurrentRequests: 3,
      blacklistTimeout: 5 * 60 * 1000, // 5 minutes
      probeInterval: 60 * 1000         // 60 seconds
    };
    this.config = { ...this.defaultConfig };
    this.blacklist = new Map();
  }

  async load() {
    try {
      const db = await this._openDB();
      const tx = db.transaction(['config'], 'readonly');
      const store = tx.objectStore('config');
      const result = await this._promisifyRequest(store.get('main'));
      
      if (result) {
        this.config = { ...this.defaultConfig, ...result };
        logger.info('Configuration loaded from IndexedDB');
      } else {
        logger.info('Using default configuration');
      }
    } catch (error) {
      logger.warn('Failed to load config from IndexedDB, using defaults:', error);
      this.config = { ...this.defaultConfig };
    }
  }

  async save(newConfig) {
    try {
      this.config = { ...this.config, ...newConfig };
      const db = await this._openDB();
      const tx = db.transaction(['config'], 'readwrite');
      const store = tx.objectStore('config');
      await this._promisifyRequest(store.put(this.config, 'main'));
      logger.info('Configuration saved to IndexedDB');
    } catch (error) {
      logger.error('Failed to save config to IndexedDB:', error);
      throw error;
    }
  }

  get() {
    return { ...this.config };
  }

  _openDB() {
    return new Promise((resolve, reject) => {
      const request = indexedDB.open('sw-cgi-config', 1);
      request.onerror = () => reject(request.error);
      request.onsuccess = () => resolve(request.result);
      request.onupgradeneeded = (event) => {
        const db = event.target.result;
        if (!db.objectStoreNames.contains('config')) {
          db.createObjectStore('config');
        }
      };
    });
  }

  _promisifyRequest(request) {
    return new Promise((resolve, reject) => {
      request.onerror = () => reject(request.error);
      request.onsuccess = () => resolve(request.result);
    });
  }

  addToBlacklist(gateway) {
    this.blacklist.set(gateway, Date.now());
    logger.debug(`Gateway ${gateway} added to blacklist`);
  }

  isBlacklisted(gateway) {
    const blacklistedAt = this.blacklist.get(gateway);
    if (!blacklistedAt) return false;
    
    if (Date.now() - blacklistedAt > this.config.blacklistTimeout) {
      this.blacklist.delete(gateway);
      return false;
    }
    return true;
  }
}

// ============================================================================
// CACHE MODULE
// ============================================================================
class CacheManager {
  constructor(configManager) {
    this.config = configManager;
    this.stats = {
      hits: 0,
      misses: 0,
      total: 0
    };
  }

  async get(request) {
    try {
      const cache = await caches.open(this.config.get().cache.version);
      const cached = await cache.match(request);
      
      if (cached) {
        this.stats.hits++;
        this.stats.total++;
        
        // Check if cache is stale and needs revalidation
        const cacheTime = cached.headers.get('sw-cached-at');
        if (cacheTime && this._isStale(parseInt(cacheTime), this._getTTL(request.url))) {
          logger.debug('Serving stale cache, revalidating in background:', request.url);
          this._revalidateInBackground(request);
        }
        
        logger.debug('[Cache] HIT:', request.url);
        return cached;
      }
      
      this.stats.misses++;
      this.stats.total++;
      logger.debug('[Cache] MISS:', request.url);
      return null;
    } catch (error) {
      logger.error('[Cache] get error:', error);
      return null;
    }
  }

  async put(request, response) {
    try {
      const cache = await caches.open(this.config.get().cache.version);
      
      // Clone response and add cache timestamp
      const responseClone = response.clone();
      const headers = new Headers(responseClone.headers);
      headers.set('sw-cached-at', Date.now().toString());
      
      const cachedResponse = new Response(responseClone.body, {
        status: responseClone.status,
        statusText: responseClone.statusText,
        headers: headers
      });
      
      await cache.put(request, cachedResponse);
      logger.debug('Cached response for:', request.url);
    } catch (error) {
      logger.error('Cache put error:', error);
    }
  }

  async clear(pattern = null) {
    try {
      const cache = await caches.open(this.config.get().cache.version);
      
      if (!pattern) {
        // Clear all cache
        const keys = await cache.keys();
        await Promise.all(keys.map(key => cache.delete(key)));
        logger.info('Cleared all cache entries');
        return keys.length;
      } else {
        // Clear by pattern
        const keys = await cache.keys();
        const matching = keys.filter(key => key.url.includes(pattern));
        await Promise.all(matching.map(key => cache.delete(key)));
        logger.info(`Cleared ${matching.length} cache entries matching: ${pattern}`);
        return matching.length;
      }
    } catch (error) {
      logger.error('Cache clear error:', error);
      throw error;
    }
  }

  async getStats() {
    try {
      const cache = await caches.open(this.config.get().cache.version);
      const keys = await cache.keys();
      
      return {
        entries: keys.length,
        hitRate: this.stats.total > 0 ? (this.stats.hits / this.stats.total * 100).toFixed(2) : '0.00',
        totalRequests: this.stats.total,
        hits: this.stats.hits,
        misses: this.stats.misses
      };
    } catch (error) {
      logger.error('Cache stats error:', error);
      return { entries: 0, hitRate: '0.00', totalRequests: 0, hits: 0, misses: 0 };
    }
  }

  _getTTL(url) {
    const config = this.config.get().cache.ttl;
    const extension = url.split('.').pop()?.toLowerCase();
    
    if (['html', 'htm'].includes(extension)) return config.html;
    if (['css'].includes(extension)) return config.css;
    if (['js', 'mjs', 'ts'].includes(extension)) return config.js;
    if (['png', 'jpg', 'jpeg', 'gif', 'svg', 'webp', 'ico'].includes(extension)) return config.image;
    if (['woff', 'woff2', 'ttf', 'eot'].includes(extension)) return config.font;
    
    return config.default;
  }

  _isStale(cacheTime, ttl) {
    return Date.now() - cacheTime > ttl;
  }

  async _revalidateInBackground(request) {
    // This would trigger a background revalidation
    // Implementation depends on the specific use case
    logger.debug('Background revalidation triggered for:', request.url);
  }
}

// ============================================================================
// IPFS PROXY MODULE
// ============================================================================
class IPFSProxy {
  constructor(configManager, cacheManager) {
    this.config = configManager;
    this.cache = cacheManager;
    this.activeRequests = new Map();
  }

  async fetch(cidPath, originalRequest) {
    const cacheKey = `ipfs:${cidPath}`;
    
    // Check for refresh parameter
    const url = new URL(originalRequest.url);
    const forceRefresh = url.searchParams.get('refresh') === '1';
    
    // Try cache first (unless refresh is forced)
    if (!forceRefresh) {
      const cached = await this.cache.get(new Request(cacheKey));
      if (cached) return cached;
    }

    // Prevent duplicate requests
    if (this.activeRequests.has(cacheKey)) {
      logger.debug('Waiting for existing request:', cacheKey);
      return await this.activeRequests.get(cacheKey);
    }

    const requestPromise = this._fetchFromGateways(cidPath, originalRequest);
    this.activeRequests.set(cacheKey, requestPromise);

    try {
      const response = await requestPromise;
      
      // Cache successful responses
      if (response.ok) {
        await this.cache.put(new Request(cacheKey), response.clone());
      }
      
      return response;
    } finally {
      this.activeRequests.delete(cacheKey);
    }
  }

  async _fetchFromGateways(cidPath, originalRequest) {
    const config = this.config.get();
    const [cid, ...pathParts] = cidPath.split('/');
    const subPath = pathParts.length > 0 ? '/' + pathParts.join('/') : '';
    
    // Build gateway URLs
    const localUrl = `${config.localGateway}/ipfs/${cid}${subPath}`;
    const publicUrls = config.publicGateways
      .filter(gateway => !this.config.isBlacklisted(gateway))
      .map(gateway => `${gateway}/ipfs/${cid}${subPath}`);

    logger.debug(`Fetching IPFS content: ${cid}${subPath}`);

    // Try local gateway first with short timeout
    try {
      const localResponse = await this._fetchWithTimeout(localUrl, config.timeouts.local, originalRequest);
      if (localResponse.ok) {
        logger.debug('Local gateway success:', localUrl);
        return localResponse;
      }
    } catch (error) {
      logger.debug('Local gateway failed:', error.message);
      this.config.addToBlacklist(config.localGateway);
    }

    // Fallback to public gateways with Promise.race
    if (publicUrls.length === 0) {
      throw new Error('No available gateways');
    }

    const publicPromises = publicUrls.slice(0, config.maxConcurrentRequests).map(async (url) => {
      try {
        const response = await this._fetchWithTimeout(url, config.timeouts.public, originalRequest);
        if (response.ok) {
          logger.debug('Public gateway success:', url);
          return response;
        }
        throw new Error(`Gateway returned ${response.status}`);
      } catch (error) {
        const gateway = new URL(url).origin;
        this.config.addToBlacklist(gateway);
        throw error;
      }
    });

    try {
      return await Promise.any(publicPromises);
    } catch (error) {
      logger.error('All gateways failed for:', cidPath);
      return new Response('IPFS content not available', { status: 502 });
    }
  }

  async _fetchWithTimeout(url, timeout, originalRequest) {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeout);

    try {
      const response = await fetch(url, {
        signal: controller.signal,
        headers: {
          'User-Agent': originalRequest.headers.get('User-Agent') || 'SW-CGI/1.0'
        }
      });
      clearTimeout(timeoutId);
      return response;
    } catch (error) {
      clearTimeout(timeoutId);
      if (error.name === 'AbortError') {
        throw new Error(`Request timeout after ${timeout}ms`);
      }
      throw error;
    }
  }
}

// ============================================================================
// VIRTUAL ROUTER MODULE
// ============================================================================
class VirtualRouter {
  constructor(configManager, cacheManager, ipfsProxy) {
    this.config = configManager;
    this.cache = cacheManager;
    this.ipfs = ipfsProxy;
    this.routes = new Map();
    this._setupRoutes();
  }

  _setupRoutes() {
    // IPFS proxy route
    this.routes.set(/^\/sw-cgi\/ipfs\/(.+)$/, async (match, request) => {
      const cidPath = match[1];
      return await this.ipfs.fetch(cidPath, request);
    });

    // Trace endpoint
    this.routes.set(/^\/sw-cgi\/trace$/, async () => {
      const info = [
        `fl=sw-cgi`,
        `h=${self.location.hostname}`,
        `ip=unknown`,
        `ts=${Math.floor(Date.now() / 1000)}`,
        `visit_scheme=https`,
        `uag=${navigator.userAgent || 'unknown'}`,
        `colo=WEB`,
        `http=http/1.1`
      ].join('\n');

      return new Response(info, {
        headers: { 'Content-Type': 'text/plain' }
      });
    });

    // Configuration API
    this.routes.set(/^\/sw-cgi\/api\/config$/, async (match, request) => {
      return await this._handleConfigAPI(request);
    });

    // Cache API
    this.routes.set(/^\/sw-cgi\/api\/cache$/, async (match, request) => {
      return await this._handleCacheAPI(request);
    });
  }

  async route(request) {
    const url = new URL(request.url);
    const path = url.pathname;

    for (const [pattern, handler] of this.routes) {
      const match = path.match(pattern);
      if (match) {
        try {
          logger.debug(`Routing ${request.method} ${path}`);
          return await handler(match, request);
        } catch (error) {
          logger.error(`Route handler error for ${path}:`, error);
          return new Response(JSON.stringify({ error: error.message }), {
            status: 500,
            headers: { 'Content-Type': 'application/json' }
          });
        }
      }
    }

    return new Response('Not Found', { status: 404 });
  }

  async _handleConfigAPI(request) {
    // Security check: only allow same-origin requests for write operations
    if (['POST', 'PUT'].includes(request.method)) {
      const origin = request.headers.get('Origin');
      if (origin && origin !== self.location.origin) {
        return new Response('Forbidden', { status: 403 });
      }
    }

    switch (request.method) {
      case 'GET': {
        const url = new URL(request.url);
        const type = url.searchParams.get('type');
        const config = this.config.get();
        
        const response = type === 'public' 
          ? { publicGateways: config.publicGateways, timeouts: config.timeouts }
          : config;
          
        return new Response(JSON.stringify(response, null, 2), {
          headers: { 'Content-Type': 'application/json' }
        });
      }

      case 'PUT': {
        // Full configuration update
        const newConfig = await request.json();
        await this.config.save(newConfig);
        return new Response(JSON.stringify({ success: true }), {
          headers: { 'Content-Type': 'application/json' }
        });
      }

      case 'POST': {
        // Partial configuration update
        const updates = await request.json();
        const currentConfig = this.config.get();
        const mergedConfig = this._deepMerge(currentConfig, updates);
        await this.config.save(mergedConfig);
        return new Response(JSON.stringify({ success: true }), {
          headers: { 'Content-Type': 'application/json' }
        });
      }

      default:
        return new Response('Method Not Allowed', { status: 405 });
    }
  }

  async _handleCacheAPI(request) {
    const url = new URL(request.url);
    const action = url.searchParams.get('action');

    // Security check for write operations
    if (['POST', 'PUT'].includes(request.method)) {
      const origin = request.headers.get('Origin');
      if (origin && origin !== self.location.origin) {
        return new Response('Forbidden', { status: 403 });
      }
    }

    switch (action) {
      case 'status': {
        const stats = await this.cache.getStats();
        return new Response(JSON.stringify(stats, null, 2), {
          headers: { 'Content-Type': 'application/json' }
        });
      }

      case 'clear': {
        if (!['POST', 'PUT'].includes(request.method)) {
          return new Response('Method Not Allowed', { status: 405 });
        }

        let cleared = 0;
        if (request.method === 'PUT') {
          // Clear all cache
          cleared = await this.cache.clear();
        } else {
          // Clear specific patterns
          const body = await request.json();
          const patterns = Array.isArray(body) ? body : [body.pattern || body];
          for (const pattern of patterns) {
            cleared += await this.cache.clear(pattern);
          }
        }

        return new Response(JSON.stringify({ 
          success: true, 
          cleared: cleared 
        }), {
          headers: { 'Content-Type': 'application/json' }
        });
      }

      default:
        return new Response('Invalid action', { status: 400 });
    }
  }

  _deepMerge(target, source) {
    const result = { ...target };
    for (const key in source) {
      if (source[key] && typeof source[key] === 'object' && !Array.isArray(source[key])) {
        result[key] = this._deepMerge(target[key] || {}, source[key]);
      } else {
        result[key] = source[key];
      }
    }
    return result;
  }
}

// ============================================================================
// MAIN SERVICE WORKER LOGIC
// ============================================================================
let configManager;
let cacheManager;
let ipfsProxy;
let virtualRouter;

// Initialize modules
async function initialize() {
  try {
    configManager = new ConfigManager();
    await configManager.load();
    
    cacheManager = new CacheManager(configManager);
    ipfsProxy = new IPFSProxy(configManager, cacheManager);
    virtualRouter = new VirtualRouter(configManager, cacheManager, ipfsProxy);
    
    logger.info('SW-CGI initialized successfully');
  } catch (error) {
    logger.error('Initialization failed:', error);
  }
}

// Service Worker Events
self.addEventListener('install', (event) => {
  logger.info('SW-CGI installing...');
  event.waitUntil(initialize());
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  logger.info('SW-CGI activated');
  event.waitUntil(self.clients.claim());
});

// Main fetch event handler
self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);
  
  // Only intercept requests to our virtual paths
  if (!url.pathname.startsWith('/sw-cgi/')) {
    return; // Let browser handle normally
  }

  event.respondWith(handleVirtualRequest(event.request));
});

async function handleVirtualRequest(request) {
  try {
    // Ensure modules are initialized
    if (!virtualRouter) {
      await initialize();
    }

    return await virtualRouter.route(request);
  } catch (error) {
    logger.error('Request handling error:', error);
    return new Response('Internal Server Error', { status: 500 });
  }
}

// Periodic tasks
self.addEventListener('message', (event) => {
  if (event.data && event.data.type === 'PROBE_GATEWAYS') {
    probeGateways();
  }
});

async function probeGateways() {
  if (!configManager) return;
  
  const config = configManager.get();
  const testCid = 'QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG'; // "hello world"
  
  // Test local gateway
  try {
    const response = await fetch(`${config.localGateway}/ipfs/${testCid}`, {
      method: 'HEAD',
      signal: AbortSignal.timeout(config.timeouts.local)
    });
    if (response.ok) {
      configManager.blacklist.delete(config.localGateway);
    }
  } catch (error) {
    logger.debug('Local gateway probe failed:', error.message);
  }
}

// Start periodic probing
setInterval(probeGateways, 30000); // Every 30 seconds

logger.info('SW-CGI Service Worker loaded and ready');
