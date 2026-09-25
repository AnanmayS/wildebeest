// Deterministic "sample dataset" for the mock: image i always has the same content,
// and a placeholder camera-trap frame (SVG) with a silhouette at its bounding box.

const SPECIES = [
  ['zebra', 'plains zebra', 18], ['wildebeest', 'blue wildebeest', 16], ['gazelle', "thomson's gazelle", 12],
  ['buffalo', 'african buffalo', 8], ['impala', 'impala', 7], ['giraffe', 'giraffe', 6],
  ['elephant', 'african elephant', 6], ['warthog', 'common warthog', 5], ['hyena', 'spotted hyena', 4],
  ['lion', 'lion', 3], ['baboon', 'olive baboon', 3], ['ostrich', 'common ostrich', 2],
];
const SPECIES_TOTAL = SPECIES.reduce((s, x) => s + x[2], 0);

export const round = (v, d) => Math.round(v * 10 ** d) / 10 ** d;

export function rand01(seed) {
  // mulberry32, one draw
  let t = (seed + 0x6d2b79f5) | 0;
  t = Math.imul(t ^ (t >>> 15), t | 1);
  t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
  return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
}

export function imageInfo(idx) {
  const r = rand01(idx * 7 + 1);
  const category = r < 0.7 ? 'empty' : r < 0.95 ? 'animal' : r < 0.98 ? 'human' : 'vehicle';
  let pick = rand01(idx * 13 + 5) * SPECIES_TOTAL;
  let species = SPECIES[0];
  for (const s of SPECIES) { if ((pick -= s[2]) <= 0) { species = s; break; } }
  const w = 0.18 + rand01(idx * 3 + 2) * 0.3;
  const h = w * (0.7 + rand01(idx * 5 + 3) * 0.4);
  const x = rand01(idx * 11 + 4) * (0.95 - w);
  const y = 0.3 + rand01(idx * 17 + 6) * (0.6 - h * 0.8);
  const conf = 0.55 + rand01(idx * 19 + 7) * 0.44;
  const detections = [];
  if (category !== 'empty') {
    detections.push({ label: category, conf: round(conf, 3), bbox: [x, y, w, Math.min(h, 0.92 - y)].map((v) => round(v, 4)) });
    if (category === 'animal' && rand01(idx * 23) < 0.3) {
      const x2 = Math.min(0.9 - w * 0.7, x + w + 0.03);
      detections.push({ label: 'animal', conf: round(conf * 0.8, 3), bbox: [x2, y + 0.04, w * 0.7, h * 0.7].map((v) => round(v, 4)) });
    }
  } else if (rand01(idx * 29) < 0.2) {
    detections.push({ label: 'animal', conf: 0.08, bbox: [0.6, 0.55, 0.1, 0.1] }); // below threshold (grass)
  }
  return {
    idx, sha256: `sha${idx.toString(16).padStart(8, '0')}`, category, detections,
    label: `mammalia;…;${species[0]}`, commonName: species[1], confidence: round(0.6 + rand01(idx * 31) * 0.39, 3),
  };
}

export function renderSvg(idx) {
  const info = imageInfo(idx % 100_000);
  const night = rand01(idx * 37) < 0.35;
  const W = 640, H = 480;
  const sky = night ? ['#2a2d2b', '#4a4f4b'] : ['#9fb7c4', '#e3d9bf'];
  const ground = night ? ['#555a55', '#3a3e3a'] : ['#b59a5e', '#7d6a3c'];
  const shade = night ? '#1c1f1c' : '#3b2f1d';
  const horizon = 0.38 + rand01(idx * 41) * 0.08;
  let shapes = '';
  // acacia
  const tx = rand01(idx * 43) * W;
  shapes += `<rect x="${tx - 4}" y="${H * horizon - 40}" width="8" height="46" fill="${shade}" opacity=".7"/>`;
  shapes += `<ellipse cx="${tx}" cy="${H * horizon - 44}" rx="70" ry="14" fill="${shade}" opacity=".7"/>`;
  // grass
  for (let i = 0; i < 70; i++) {
    const gx = rand01(idx * 101 + i) * W;
    const gy = H * horizon + 20 + rand01(idx * 103 + i) * (H * (1 - horizon) - 40);
    shapes += `<line x1="${gx}" y1="${gy}" x2="${gx + (rand01(i + idx) - 0.5) * 12}" y2="${gy - 10 - rand01(i * 3 + idx) * 22}" stroke="${shade}" stroke-width="1.5" opacity=".35"/>`;
  }
  for (const d of info.detections) {
    if (d.conf < 0.2) continue;
    const [x, y, w, h] = [d.bbox[0] * W, d.bbox[1] * H, d.bbox[2] * W, d.bbox[3] * H];
    if (d.label === 'animal') {
      shapes += `<g fill="${shade}">` +
        `<ellipse cx="${x + w * 0.45}" cy="${y + h * 0.45}" rx="${w * 0.33}" ry="${h * 0.22}"/>` +
        `<ellipse cx="${x + w * 0.85}" cy="${y + h * 0.25}" rx="${w * 0.11}" ry="${h * 0.12}"/>` +
        `<rect x="${x + w * 0.72}" y="${y + h * 0.22}" width="${w * 0.08}" height="${h * 0.3}" transform="rotate(25 ${x + w * 0.76} ${y + h * 0.37})"/>` +
        [0.2, 0.32, 0.58, 0.68].map((f) => `<rect x="${x + w * f}" y="${y + h * 0.55}" width="${w * 0.05}" height="${h * 0.42}"/>`).join('') +
        `</g>`;
    } else if (d.label === 'human') {
      shapes += `<g fill="${shade}"><circle cx="${x + w / 2}" cy="${y + h * 0.12}" r="${Math.min(w, h) * 0.12}"/><rect x="${x + w * 0.3}" y="${y + h * 0.25}" width="${w * 0.4}" height="${h * 0.72}" rx="6"/></g>`;
    } else {
      shapes += `<g fill="${shade}"><rect x="${x}" y="${y + h * 0.3}" width="${w}" height="${h * 0.5}" rx="8"/><rect x="${x + w * 0.2}" y="${y}" width="${w * 0.55}" height="${h * 0.4}" rx="6"/><circle cx="${x + w * 0.2}" cy="${y + h * 0.85}" r="${h * 0.15}" fill="#111"/><circle cx="${x + w * 0.8}" cy="${y + h * 0.85}" r="${h * 0.15}" fill="#111"/></g>`;
    }
  }
  const hh = String(Math.floor(rand01(idx * 47) * 24)).padStart(2, '0');
  const mm = String(Math.floor(rand01(idx * 53) * 60)).padStart(2, '0');
  return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${W} ${H}" width="${W}" height="${H}">
<defs><linearGradient id="s" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="${sky[0]}"/><stop offset="1" stop-color="${sky[1]}"/></linearGradient>
<linearGradient id="g" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="${ground[0]}"/><stop offset="1" stop-color="${ground[1]}"/></linearGradient></defs>
<rect width="${W}" height="${H}" fill="url(#s)"/><rect y="${H * horizon}" width="${W}" height="${H * (1 - horizon)}" fill="url(#g)"/>
${shapes}
<rect y="${H - 26}" width="${W}" height="26" fill="#000" opacity=".8"/>
<text x="10" y="${H - 8}" font-family="monospace" font-size="14" fill="#ddd">CAM${String((idx % 12) + 1).padStart(2, '0')}  2026-09-${String((idx % 28) + 1).padStart(2, '0')} ${hh}:${mm}  ${night ? 'IR' : '24°C'}  #${idx}</text>
</svg>`;
}
