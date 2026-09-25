import { useState } from 'react';
import type { GalleryImage } from '../lib/types';

const BOX_COLOR: Record<string, string> = {
  animal: 'border-leaf-400 bg-leaf-400',
  human: 'border-sky-400 bg-sky-400',
  vehicle: 'border-sky-400 bg-sky-400',
};

/**
 * The wrapper takes the image's natural aspect ratio, so percentage-positioned boxes
 * line up exactly with the normalised [x, y, w, h] bboxes (top-left origin).
 */
export function DetectionImage({ image, minBoxConf }: { image: GalleryImage; minBoxConf: number }) {
  const [ratio, setRatio] = useState(4 / 3);
  const boxes = image.detections.filter((d) => d.conf >= minBoxConf);

  return (
    <div className="relative w-full overflow-hidden bg-ink-800" style={{ aspectRatio: ratio }}>
      <img
        src={image.url}
        alt={image.commonName ?? image.category ?? 'camera trap photo'}
        loading="lazy"
        className="absolute inset-0 h-full w-full"
        onLoad={(e) => {
          const { naturalWidth: w, naturalHeight: h } = e.currentTarget;
          if (w && h) setRatio(w / h);
        }}
      />
      {boxes.map((d, i) => {
        const [x, y, w, h] = d.bbox;
        const color = BOX_COLOR[d.label] ?? BOX_COLOR.animal;
        // The first animal box carries the species name; the caption below shows its confidence.
        const tag = d.label === 'animal' && i === 0 && image.commonName ? image.commonName : `${d.label} ${Math.round(d.conf * 100)}%`;
        const side = x + w > 0.75 ? 'right-0' : 'left-0';
        return (
          <div
            key={i}
            className={`absolute rounded-[3px] border-2 !bg-transparent ${color}`}
            style={{ left: `${x * 100}%`, top: `${y * 100}%`, width: `${w * 100}%`, height: `${h * 100}%` }}
          >
            <span className={`absolute ${side} ${y < 0.07 ? 'top-0' : '-top-px -translate-y-full'} whitespace-nowrap rounded-t-[3px] px-1 text-[10px] font-semibold leading-4 text-ink-950 ${color}`}>
              {tag}
            </span>
          </div>
        );
      })}
    </div>
  );
}
