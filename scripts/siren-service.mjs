import { promises as fs, createReadStream, createWriteStream } from 'node:fs'
import path from 'node:path'
import os from 'node:os'
import { pipeline } from 'node:stream/promises'
import { spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import crypto from 'node:crypto'

const PROJECT_DIR = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const API_ORIGIN = 'https://monster-siren.hypergryph.com'
const API = `${API_ORIGIN}/api`
const DEFAULT_DOWNLOAD_DIR = path.join(os.homedir(), 'Music', 'Siren-MSR')
const AUDIO_EXTENSIONS = new Set(['.mp3', '.wav', '.flac', '.ogg', '.m4a', '.aac'])
const safeText = (value, fallback = '') => typeof value === 'string' ? value.trim() : fallback
const safeName = (value) => safeText(value, '未命名').replace(/[\\/:*?"<>|\u0000-\u001f]/g, '_').replace(/\s+/g, ' ').trim().slice(0, 160) || '未命名'
const sha = (value) => crypto.createHash('sha1').update(String(value)).digest('hex').slice(0, 16)
const extFromUrl = (value, fallback = '.mp3') => {
  try {
    const ext = path.extname(new URL(value).pathname).toLowerCase()
    return AUDIO_EXTENSIONS.has(ext) ? ext : fallback
  } catch { return fallback }
}
const extFromMime = (value, fallback = '.png') => {
  const mime = String(value || '').split(';')[0].toLowerCase()
  return mime === 'image/jpeg' ? '.jpg' : mime === 'image/webp' ? '.webp' : mime === 'image/svg+xml' ? '.svg' : fallback
}

async function exists(file) { try { await fs.access(file); return true } catch { return false } }
async function readJson(file, fallback) { try { return JSON.parse(await fs.readFile(file, 'utf8')) } catch { return fallback } }
async function writeJson(file, value) {
  await fs.mkdir(path.dirname(file), { recursive: true })
  const tmp = `${file}.${process.pid}.tmp`
  await fs.writeFile(tmp, JSON.stringify(value, null, 2) + '\n', { mode: 0o600 })
  await fs.rename(tmp, file)
}
async function fetchJson(url) {
  const response = await fetch(url, { headers: { accept: 'application/json' }, signal: AbortSignal.timeout(20000) })
  if (!response.ok) throw new Error(`塞壬唱片接口 ${response.status}: ${url}`)
  const data = await response.json()
  if (data?.code !== undefined && data.code !== 0) throw new Error(data.msg || `塞壬唱片接口返回 code=${data.code}`)
  return data?.data ?? data
}
async function mapLimit(values, limit, worker) {
  const results = new Array(values.length)
  let cursor = 0
  async function run() {
    while (true) {
      const index = cursor++
      if (index >= values.length) return
      results[index] = await worker(values[index], index)
    }
  }
  await Promise.all(Array.from({ length: Math.min(limit, values.length || 1) }, run))
  return results
}

export class SirenService {
  constructor({ dataDir = process.env.MUSIC_DATA_DIR ?? path.join(PROJECT_DIR, '..', 'music-data-v3'), downloadDir = process.env.SIREN_DOWNLOAD_DIR ?? DEFAULT_DOWNLOAD_DIR } = {}) {
    this.dataDir = dataDir
    this.cacheDir = path.join(dataDir, 'siren')
    this.catalogFile = path.join(this.cacheDir, 'catalog.json')
    this.configFile = path.join(this.cacheDir, 'config.json')
    this.config = {
      downloadDir,
      autoLyrics: true,
      convertWavToFlac: false,
      concurrency: 3,
    }
    this.configPromise = readJson(this.configFile, {}).then((saved) => {
      if (saved && typeof saved === 'object') this.config = { ...this.config, ...saved }
      return this.config
    })
    this.catalogCache = null
    this.albumCache = new Map()
    this.songCache = new Map()
    this.jobs = new Map()
    this.jobSequence = 0
  }

  async init() { await this.configPromise; await fs.mkdir(this.cacheDir, { recursive: true }); return this }

  async updateConfig(patch = {}) {
    await this.configPromise
    if (typeof patch.downloadDir === 'string' && patch.downloadDir.trim()) this.config.downloadDir = path.resolve(patch.downloadDir.trim())
    if (typeof patch.autoLyrics === 'boolean') this.config.autoLyrics = patch.autoLyrics
    if (typeof patch.convertWavToFlac === 'boolean') this.config.convertWavToFlac = patch.convertWavToFlac
    if (Number.isInteger(patch.concurrency) && patch.concurrency >= 1 && patch.concurrency <= 8) this.config.concurrency = patch.concurrency
    await writeJson(this.configFile, this.config)
    return this.snapshotConfig()
  }

  snapshotConfig() { return { ...this.config, source: API_ORIGIN, officialCovers: true } }

  async getSongs(refresh = false) {
    if (!refresh && this.catalogCache?.songs) return this.catalogCache.songs
    if (!refresh) {
      const cached = await readJson(this.catalogFile, null)
      if (cached?.songs?.length) { this.catalogCache = cached; return cached.songs }
    }
    const value = await fetchJson(`${API}/songs`)
    const songs = Array.isArray(value?.list) ? value.list : Array.isArray(value) ? value : []
    if (!songs.length) throw new Error('塞壬唱片没有返回歌曲列表')
    this.catalogCache = { fetchedAt: new Date().toISOString(), songs }
    await writeJson(this.catalogFile, this.catalogCache)
    return songs
  }

  async getSong(cid, refresh = false) {
    const key = String(cid)
    if (!refresh && this.songCache.has(key)) return this.songCache.get(key)
    const value = await fetchJson(`${API}/song/${encodeURIComponent(key)}`)
    const song = value?.song ?? value
    this.songCache.set(key, song)
    return song
  }

  async getAlbum(albumCid, refresh = false) {
    const key = String(albumCid)
    if (!refresh && this.albumCache.has(key)) return this.albumCache.get(key)
    const value = await fetchJson(`${API}/album/${encodeURIComponent(key)}/detail`)
    const album = value?.album ?? value
    this.albumCache.set(key, album)
    return album
  }

  async getCatalog({ refresh = false } = {}) {
    const songs = await this.getSongs(refresh)
    const albumIds = [...new Set(songs.map((s) => String(s.albumCid || 'unknown')).filter(Boolean))]
    const albums = await mapLimit(albumIds, 8, async (albumCid) => {
      try { return await this.getAlbum(albumCid, refresh) } catch (error) { return { cid: albumCid, name: `专辑 ${albumCid}`, intro: '', belong: 'arknights', songs: [], error: error.message } }
    })
    const albumMap = new Map(albums.map((a) => [String(a.cid), a]))
    return {
      source: API_ORIGIN,
      fetchedAt: this.catalogCache?.fetchedAt,
      albums: albums.map((album) => ({
        cid: String(album.cid), name: safeText(album.name, `专辑 ${album.cid}`), intro: safeText(album.intro), coverUrl: album.coverDeUrl || album.coverUrl || null,
        coverOriginalUrl: album.coverUrl || null,
        songs: songs.filter((s) => String(s.albumCid) === String(album.cid)).map((s) => ({ cid: String(s.cid), name: safeText(s.name, `歌曲 ${s.cid}`), artists: Array.isArray(s.artists) ? s.artists : [] })),
      })),
    }
  }

  async snapshot({ refresh = false } = {}) {
    const catalog = await this.getCatalog({ refresh })
    const albums = catalog.albums.map((album) => {
      const tracks = album.songs.map((song, index) => ({
        id: `siren-${song.cid}`,
        sirenCid: song.cid,
        albumId: `siren-album-${album.cid}`,
        title: song.name,
        artist: song.artists.join(' / ') || '塞壬唱片-MSR',
        trackNumber: index + 1,
        discNumber: 1,
        duration: 0,
        format: 'MP3/WAV',
        codec: undefined,
        browserPlayable: true,
        audioUrl: `/api/siren/stream/${encodeURIComponent(song.cid)}`,
        relativePath: `Siren-MSR/${safeName(album.name)}/${safeName(song.name)}`,
        source: 'siren',
      }))
      return {
        id: `siren-album-${album.cid}`,
        sirenAlbumCid: album.cid,
        title: album.name,
        artist: '塞壬唱片-MSR',
        year: undefined,
        description: album.intro || undefined,
        genreId: 'siren-msr',
        rawGenres: ['塞壬唱片'],
        folder: `Siren-MSR/${safeName(album.name)}`,
        coverUrl: `/api/siren/cover/${encodeURIComponent(album.cid)}`,
        tracks,
        producers: [],
        offline: false,
        online: { status: 'matched', sourceUrl: `${API_ORIGIN}/music` },
      }
    })
    return {
      version: 1, albums, genres: [{ id: 'siren-msr', name: '塞壬唱片', albumCount: albums.length }], roots: [], scan: { running: false, finishedAt: catalog.fetchedAt }, onlineEnabled: true,
      siren: { source: API_ORIGIN, config: this.snapshotConfig(), albums: albums.length, tracks: albums.reduce((n, a) => n + a.tracks.length, 0) },
    }
  }

  async catalogResponse({ refresh = false } = {}) { return { ...(await this.getCatalog({ refresh })), config: this.snapshotConfig() } }

  async streamInfo(cid) {
    const song = await this.getSong(cid)
    const local = await this.findDownloadedAudio(cid)
    return { song, local }
  }

  async findDownloadedAudio(cid) {
    await this.configPromise
    const dir = path.join(this.config.downloadDir, 'audio')
    const prefix = `${String(cid)}.`
    try {
      const names = await fs.readdir(dir)
      const name = names.find((n) => n.startsWith(prefix) && AUDIO_EXTENSIONS.has(path.extname(n).toLowerCase()))
      return name ? path.join(dir, name) : null
    } catch { return null }
  }

  async findDownloadedCover(albumCid) {
    await this.configPromise
    const dir = path.join(this.config.downloadDir, 'covers')
    try {
      const names = await fs.readdir(dir)
      const name = names.find((n) => n.startsWith(`${String(albumCid)}.`))
      return name ? path.join(dir, name) : null
    } catch { return null }
  }

  async download(cid) {
    await this.configPromise
    const song = await this.getSong(cid)
    if (!song?.sourceUrl) throw new Error(`歌曲 ${cid} 没有官方音频地址`)
    const album = await this.getAlbum(song.albumCid)
    const root = this.config.downloadDir
    const audioDir = path.join(root, 'audio'), lyricDir = path.join(root, 'lyrics'), coverDir = path.join(root, 'covers'), metaDir = path.join(root, 'metadata')
    await Promise.all([audioDir, lyricDir, coverDir, metaDir].map((dir) => fs.mkdir(dir, { recursive: true })))
    const sourceExt = extFromUrl(song.sourceUrl, '.mp3')
    const sourceFile = path.join(audioDir, `${safeName(cid)}${sourceExt}`)
    if (!(await exists(sourceFile))) await this.downloadFile(song.sourceUrl, sourceFile)
    let finalAudio = sourceFile
    if (this.config.convertWavToFlac && sourceExt === '.wav') finalAudio = await this.convertToFlac(sourceFile, audioDir)
    let coverPath = await this.findDownloadedCover(song.albumCid)
    const coverUrl = album.coverDeUrl || album.coverUrl
    if (!coverPath && coverUrl) {
      const response = await fetch(coverUrl, { signal: AbortSignal.timeout(20000) })
      if (response.ok) {
        const ext = extFromMime(response.headers.get('content-type'))
        coverPath = path.join(coverDir, `${safeName(song.albumCid)}${ext}`)
        await fs.writeFile(coverPath, Buffer.from(await response.arrayBuffer()), { mode: 0o600 })
      }
    }
    let lyricPath = null
    if (this.config.autoLyrics && song.lyricUrl) {
      lyricPath = path.join(lyricDir, `${safeName(cid)}.lrc`)
      if (!(await exists(lyricPath))) await this.downloadFile(song.lyricUrl, lyricPath)
    }
    const metadata = { cid: String(song.cid), title: song.name, artists: song.artists || [], albumCid: String(song.albumCid), album: album.name, coverUrl: coverUrl || null, sourceUrl: song.sourceUrl, lyricUrl: song.lyricUrl || null, downloadedAt: new Date().toISOString(), audioPath: finalAudio, lyricPath, coverPath }
    await writeJson(path.join(metaDir, `${safeName(cid)}.json`), metadata)
    return metadata
  }

  async downloadFile(url, target) {
    const response = await fetch(url, { signal: AbortSignal.timeout(120000) })
    if (!response.ok || !response.body) throw new Error(`下载失败 ${response.status}: ${url}`)
    const temp = `${target}.${process.pid}.part`
    await pipeline(response.body, createWriteStream(temp, { mode: 0o600 }))
    await fs.rename(temp, target)
  }

  async convertToFlac(source, directory) {
    const target = path.join(directory, `${path.basename(source, path.extname(source))}.flac`)
    if (await exists(target)) return target
    await new Promise((resolve, reject) => {
      const child = spawn('ffmpeg', ['-y', '-i', source, '-vn', '-c:a', 'flac', target], { stdio: 'ignore' })
      child.once('error', reject); child.once('exit', (code) => code === 0 ? resolve() : reject(new Error(`ffmpeg 转码失败 (${code})`)))
    })
    return target
  }

  async startDownload(cids) {
    const ids = [...new Set((Array.isArray(cids) ? cids : [cids]).map(String).filter(Boolean))]
    if (!ids.length) throw new Error('至少选择一首歌曲')
    const id = `siren-job-${++this.jobSequence}-${Date.now()}`
    const job = { id, state: 'running', total: ids.length, completed: 0, failed: 0, errors: [], items: [], startedAt: new Date().toISOString() }
    this.jobs.set(id, job)
    void mapLimit(ids, Math.max(1, Math.min(8, Number(this.config.concurrency) || 3)), async (cid) => {
      try { job.items.push(await this.download(cid)); job.completed++ } catch (error) { job.failed++; job.errors.push({ cid, error: error.message }) }
    }).then(() => { job.state = job.failed ? (job.completed ? 'completed_with_errors' : 'failed') : 'completed'; job.finishedAt = new Date().toISOString() })
    return job
  }

  status(jobId) { return this.jobs.get(jobId) || null }
}

export const sirenMime = (file) => {
  const ext = path.extname(file).toLowerCase()
  return ext === '.mp3' ? 'audio/mpeg' : ext === '.wav' ? 'audio/wav' : ext === '.flac' ? 'audio/flac' : ext === '.ogg' || ext === '.opus' ? 'audio/ogg' : ext === '.m4a' || ext === '.aac' ? 'audio/mp4' : ext === '.lrc' ? 'text/plain; charset=utf-8' : ext === '.jpg' || ext === '.jpeg' ? 'image/jpeg' : ext === '.webp' ? 'image/webp' : 'image/png'
}
export { API_ORIGIN }
